import asyncio
import logging
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    import uvloop  # type: ignore[import]

    uvloop.install()
except ImportError:
    logging.warning("uvloop is not installed, using default event loop")

from uvicorn.config import Config as UvicornConfig
from uvicorn.server import Server

from tgdcfs.app import create_app
from tgdcfs.app.sftp import create_pending_uploads, start_sftp_server
from tgdcfs.backends.base import IStore
from tgdcfs.backends.discord import factory as discord
from tgdcfs.backends.telegram import factory as telegram
from tgdcfs.config import Config, StoreConfig, get_config
from tgdcfs.core import Client, Clients
from tgdcfs.core.client import StoreFactory
from tgdcfs.core.local_cache import CacheSweeper, LocalCache
from tgdcfs.core.replication import ReplicationQueue, ReplicationWorker
from tgdcfs.utils.message_cache import set_message_cache_size


async def create_store_factory(config: Config) -> StoreFactory:
    """Log in to every backend the config uses and return the factory
    that turns a store config into a live store."""
    tdlib_api = (
        await telegram.login(config) if config.uses_backend["telegram"] else None
    )
    discord_bots = (
        await discord.login(config) if config.uses_backend["discord"] else None
    )

    async def create_store(store_cfg: StoreConfig) -> IStore:
        if store_cfg.backend == "telegram" and tdlib_api is not None:
            return await telegram.create_store(store_cfg, config, tdlib_api)
        if store_cfg.backend == "discord" and discord_bots is not None:
            return await discord.create_store(store_cfg, config, discord_bots)
        raise ValueError(f"Unsupported backend: {store_cfg.backend}")

    return create_store


def create_cache(config: Config) -> Optional[LocalCache]:
    """The local cache when the config turns it on; loaded from disk."""
    if not config.tgdcfs.cache.enabled:
        return None
    cache = LocalCache(config.tgdcfs.cache)
    cache.load()
    return cache


async def create_clients(
    config: Config,
    replication: Optional[ReplicationQueue] = None,
    cache: Optional[LocalCache] = None,
) -> Clients:
    store_factory = await create_store_factory(config)

    clients: Clients = {}
    for filesystem in config.filesystems.values():
        clients[filesystem.name] = await Client.create(
            filesystem,
            config,
            store_factory,
            encryption_cfg=config.tgdcfs.encryption,
            replication=replication,
            cache=cache,
        )
    return clients


def start_cache_sweeper(
    cache: Optional[LocalCache], replication: ReplicationQueue
) -> Optional[CacheSweeper]:
    """The periodic cache housekeeping; a pin counts as stale only when
    nothing is queued for its file system."""
    if cache is None:
        return None
    sweeper = CacheSweeper(cache, lambda fs: not replication.pending(fs))
    sweeper.start()
    return sweeper


def start_replication_workers(
    clients: Clients, config: Config, replication: ReplicationQueue
) -> List[ReplicationWorker]:
    """One background worker per file system with ``sync: background``."""
    workers: List[ReplicationWorker] = []
    for filesystem in config.filesystems.values():
        client = clients.get(filesystem.name)
        if client is None:
            continue
        background = filesystem.sync == "background" and filesystem.mirrors
        if not background and filesystem.write_ack != "cache":
            continue
        worker = ReplicationWorker(client, replication)
        worker.start()
        workers.append(worker)
    return workers


async def run_server(app, host: str, port: int, name: str):
    """Run a server with proper configuration"""
    logger = logging.getLogger(__name__)
    logger.info(f"Starting {name} server on {host}:{port}")

    server_config = UvicornConfig(
        app,
        host=host,
        port=port,
        loop="none",
        log_level="info",
    )
    server = Server(config=server_config)
    await server.serve()


async def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    # Before the first store exists, so every channel starts at this size.
    set_message_cache_size(config.tgdcfs.listing.message_cache_size)

    replication = ReplicationQueue(config.replication_queue_file)
    cache = create_cache(config)
    clients = await create_clients(config, replication, cache)
    workers = start_replication_workers(clients, config, replication)

    app = create_app(clients, config, replication=replication, cache=cache)
    sweeper = start_cache_sweeper(cache, replication)

    pending_uploads = create_pending_uploads(config)
    try:
        sftp_acceptor = await start_sftp_server(clients, config, pending_uploads)
    except Exception as ex:
        # A broken SFTP setup (port taken, unwritable host key) must not keep
        # the HTTP interface from coming up.
        logger.error("Failed to start the SFTP server: %s", ex)
        sftp_acceptor = None

    try:
        await run_server(
            app, config.tgdcfs.server.host, config.tgdcfs.server.port, "TGDCFS"
        )
    finally:
        for worker in workers:
            await worker.stop()
        # Size and dates read by listings since the last write
        # (``listing.stat_in_metadata``).
        for client in clients.values():
            await client.file_api.stats.close()
        if sweeper is not None:
            await sweeper.stop()
        if sftp_acceptor:
            sftp_acceptor.close()
            await sftp_acceptor.wait_closed()
        if pending_uploads is not None:
            # Uploads the SFTP clients were already told are done.
            await pending_uploads.drain()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
