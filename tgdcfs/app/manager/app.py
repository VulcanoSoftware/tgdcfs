import asyncio
import logging
import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from tgdcfs.app.fs_cache import gfc
from tgdcfs.app.utils import split_global_path
from tgdcfs.config import Config
from tgdcfs.core import Clients
from tgdcfs.core.backfill import (
    backfill_mirrors,
    backfill_plaintext_sizes,
    count_files,
    create_backfill_task,
)
from tgdcfs.core.local_cache import LocalCache
from tgdcfs.core.ops import Ops
from tgdcfs.core.replication import ReplicationQueue
from tgdcfs.reqres import MessageRespWithDocument
from tgdcfs.tasks import task_store

logger = logging.getLogger(__name__)


def create_manager_app(
    clients: Clients,
    config: Config,
    replication: Optional[ReplicationQueue] = None,
    cache: Optional[LocalCache] = None,
) -> FastAPI:
    ops = {name: Ops(client) for name, client in clients.items()}

    def get_name_by_channel_id(channel_id: int) -> Optional[str]:
        """File system whose primary is the Telegram channel ``channel_id``.

        The mini app imports messages by Telegram channel id, so this
        lookup stays keyed by channel rather than by store name.
        """
        fs = config.filesystem_for_channel("telegram", str(channel_id))
        return fs.name if fs else None

    app = FastAPI()

    @app.get("/stores")
    async def get_stores():
        """Configured stores with their backend and capabilities."""
        res = {}
        for name, store_cfg in config.stores.items():
            entry: dict = {
                "backend": store_cfg.backend,
                "channel": store_cfg.channel,
                "key": store_cfg.key,
                "primary_of": [
                    fs.name for fs in config.filesystems.values() if fs.primary == name
                ],
                "mirror_of": [
                    fs.name for fs in config.filesystems.values() if name in fs.mirrors
                ],
            }
            # Capabilities come from the live store; a store only referenced
            # by a file system that failed to start has none.
            for client in clients.values():
                live = (
                    client.store
                    if client.store.key == store_cfg.key
                    else (
                        client.mirror_group.store_for(store_cfg.key)
                        if client.mirror_group
                        else None
                    )
                )
                if live is not None:
                    caps = live.caps
                    entry["capabilities"] = {
                        "max_part_bytes": caps.max_part_bytes,
                        "max_text_chars": caps.max_text_chars,
                        "supports_server_copy": caps.supports_server_copy,
                        "supports_edit_media": caps.supports_edit_media,
                    }
                    break
            res[name] = entry
        return res

    @app.get("/filesystems")
    async def get_filesystems():
        """File systems with their primary, mirrors and mirroring settings."""
        res = {}
        for name, fs in config.filesystems.items():
            client = clients.get(name)
            res[name] = {
                "primary": fs.primary,
                "mirrors": list(fs.mirrors),
                "mode": fs.mode,
                "sync": fs.sync,
                "strict": fs.strict,
                "read_preference": list(fs.read_preference),
                "read_parallel": fs.read_parallel,
                "read_sources": list(fs.read_sources),
                "write_ack": fs.write_ack,
                "metadata": fs.metadata.type.value,
                "primary_dead": bool(
                    client
                    and client.mirror_group
                    and client.mirror_group.primary_dead()
                ),
            }
        return res

    @app.get("/tasks", response_model=List[dict])
    async def get_tasks(
        path: Optional[str] = Query(
            None, description="Filter tasks under specific path"
        )
    ):
        if path is not None:
            tasks = await task_store.get_tasks_under_path(path)
        else:
            tasks = await task_store.get_all_tasks()

        return [task.to_dict() for task in tasks]

    @app.get("/tasks/{task_id}", response_model=dict)
    async def get_task(task_id: str):
        if not (task := await task_store.get_task(task_id)):
            raise HTTPException(status_code=404, detail="Task not found")
        return task.to_dict()

    @app.delete("/tasks/{task_id}")
    async def delete_task(task_id: str):
        """Delete a task."""
        if not await task_store.remove_task(task_id):
            raise HTTPException(status_code=404, detail="Task not found")
        return {"message": "Task deleted successfully"}

    @app.get("/replication/queue")
    async def get_replication_queue():
        """Files waiting for background replication, per file system."""
        if replication is None:
            return {}
        return replication.to_dict()

    @app.post("/replication/retry")
    async def retry_replication(
        filesystem: Optional[str] = Query(
            None, description="Retry only this file system's queue"
        )
    ):
        """Clear the backoff of failed items so the workers retry them now."""
        if replication is None:
            raise HTTPException(status_code=400, detail="No replication queue")
        if filesystem is not None and filesystem not in clients:
            raise HTTPException(status_code=404, detail="Unknown file system")
        replication.retry_now(filesystem)
        return {"message": "Retry scheduled"}

    @app.get("/cache")
    async def get_cache():
        """The local cache: budget, entries, pinned bytes, hit ratio."""
        if cache is None:
            return {"enabled": False}
        return cache.stats()

    @app.post("/cache/evict")
    async def evict_cache():
        """Drop every cache entry no mirror is waiting for."""
        if cache is None:
            raise HTTPException(status_code=400, detail="The local cache is off")
        return {"evicted": cache.evict_unpinned(), **cache.stats()}

    @app.get("/redundancy")
    async def get_redundancy():
        """Redundancy overview per file system, keyed by mirror store key.

        Kept for the tgfs manager UI; ``/filesystems`` is the fuller view.
        """
        return {
            name: {
                "mirrors": (
                    client.mirror_group.store_keys if client.mirror_group else []
                ),
                "mode": client.mirror_group.mode if client.mirror_group else None,
                "strict": (
                    client.mirror_group.strict if client.mirror_group else False
                ),
            }
            for name, client in clients.items()
        }

    @app.post("/redundancy/backfill/{client_name}")
    async def start_backfill(client_name: str, verify: bool = Query(False)):
        """Mirror all pre-existing, unmirrored data of one client.

        Runs in the background; progress is reported through the regular
        /tasks endpoints (type ``mirror_backfill``). ``verify=true``
        additionally checks that recorded mirror copies still exist and
        re-mirrors any that were deleted.
        """
        if (client := clients.get(client_name)) is None:
            raise HTTPException(status_code=404, detail="Unknown client")
        if client.mirror_group is None:
            raise HTTPException(
                status_code=400,
                detail=f"No mirror stores configured for '{client_name}'",
            )

        task_id = await create_backfill_task(client, count_files(client))

        async def run() -> None:
            try:
                await backfill_mirrors(client, verify=verify, task_id=task_id)
            except Exception:
                logger.exception(f"Mirror backfill for '{client_name}' crashed")

        asyncio.get_running_loop().create_task(run())
        return {"task_id": task_id}

    @app.post("/metadata/backfill/{client_name}")
    async def start_metadata_backfill(client_name: str):
        """Probe and record plaintext sizes for all files missing it."""
        if (client := clients.get(client_name)) is None:
            raise HTTPException(status_code=404, detail="Unknown client")

        task_id = await create_backfill_task(client, count_files(client))

        async def run() -> None:
            try:
                await backfill_plaintext_sizes(client, task_id=task_id)
            except Exception:
                logger.exception(f"Metadata backfill for '{client_name}' crashed")

        asyncio.get_running_loop().create_task(run())
        return {"task_id": task_id}

    async def get_message(channel_id: int, message_id: int) -> MessageRespWithDocument:
        if (client_name := get_name_by_channel_id(channel_id)) is None:
            raise HTTPException(
                status_code=400,
                detail="The message is not in one of the configured file channels. "
                "Please forward the message to the file channel of your importing location first.",
            )

        client = clients[client_name]

        message = (await client.store.get_messages([message_id]))[0]

        if not message:
            raise HTTPException(
                status_code=404,
                detail=f"Message {message_id} not found in the file channel.",
            )

        if not message.document:
            raise HTTPException(
                status_code=400, detail="The message does not contain a document."
            )

        return MessageRespWithDocument(
            message_id=message.message_id,
            document=message.document,
            text=message.text,
        )

    @app.get("/message/{channel_id}/{message_id}")
    async def get_telegram_message(channel_id: int, message_id: int):
        message = await get_message(channel_id, message_id)

        # Return message info regardless of whether it has a document
        return {
            "id": message.message_id,
            "file_size": message.document.size,
            "caption": message.text or "",
            "mime_type": message.document.mime_type,
        }

    class ImportTelegramMessageData(BaseModel):
        directory: str
        name: str
        channel_id: int
        message_id: int

    @app.post("/import")
    async def import_telegram_message(body: ImportTelegramMessageData):
        message = await get_message(body.channel_id, body.message_id)
        if not body.directory.endswith("/"):
            directory = body.directory + "/"
        else:
            directory = body.directory
        client_name, sub_path = split_global_path(directory)

        if client_name != get_name_by_channel_id(body.channel_id):
            raise HTTPException(
                status_code=400,
                detail=f"The file is not in the channel managing the current folder {client_name}",
            )

        gfc[client_name].reset(f"/{sub_path}")

        await ops[client_name].import_from_existing_file_message(
            message, os.path.join(f"/{sub_path}", body.name)
        )

        return {"message": "Document imported successfully"}

    return app
