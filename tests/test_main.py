from typing import Any, Dict

import pytest

from main import create_clients, create_store_factory, main, run_server
from tgdcfs.config import Config

CONFIG: Dict[str, Any] = {
    "backends": {
        "telegram": {
            "api_id": 12345,
            "api_hash": "hash123",
            "bot": {"token": "bot_token", "session_file": "bot.session"},
        }
    },
    "stores": {
        "tg-main": {"backend": "telegram", "channel": "-1001"},
        "tg-spare": {"backend": "telegram", "channel": "-1002"},
    },
    "filesystems": {
        "media": {"primary": "tg-main", "mirrors": ["tg-spare"]},
        "notes": {"primary": "tg-spare", "allow_shared_store": True},
    },
    "tgdcfs": {
        "users": {},
        "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
        "server": {"host": "0.0.0.0", "port": 8080},
    },
}


class TestMain:
    @pytest.mark.asyncio
    async def test_create_clients_logs_in_once_and_builds_every_filesystem(
        self, mocker
    ):
        config = Config.from_dict(CONFIG)
        tdlib = mocker.Mock()
        login = mocker.patch(
            "main.telegram.login", mocker.AsyncMock(return_value=tdlib)
        )
        stores = {}

        async def fake_create_store(store_cfg, cfg, api):
            assert cfg is config and api is tdlib
            stores[store_cfg.name] = mocker.Mock(key=store_cfg.key)
            return stores[store_cfg.name]

        mocker.patch("main.telegram.create_store", side_effect=fake_create_store)
        created = {}

        async def fake_client_create(
            filesystem, cfg, factory, encryption_cfg=None, replication=None, cache=None
        ):
            assert cfg is config
            assert encryption_cfg is config.tgdcfs.encryption
            created[filesystem.name] = await factory(cfg.stores[filesystem.primary])
            return mocker.Mock(name=filesystem.name)

        mocker.patch("main.Client.create", side_effect=fake_client_create)

        result = await create_clients(config)

        login.assert_awaited_once_with(config)
        assert set(result) == {"media", "notes"}
        assert created["media"].key == "tg:-1001"
        assert created["notes"].key == "tg:-1002"

    @pytest.mark.asyncio
    async def test_store_factory_logs_in_to_discord_when_used(self, mocker):
        config = Config.from_dict(
            {
                **CONFIG,
                "backends": {
                    **CONFIG["backends"],
                    "discord": {"bot_token": "t"},
                },
                "stores": {
                    **CONFIG["stores"],
                    "dc-mirror": {"backend": "discord", "channel": "42"},
                },
            }
        )
        mocker.patch("main.telegram.login", mocker.AsyncMock())
        bots = [mocker.Mock()]
        login = mocker.patch("main.discord.login", mocker.AsyncMock(return_value=bots))
        create = mocker.patch(
            "main.discord.create_store", mocker.AsyncMock(return_value="store")
        )

        factory = await create_store_factory(config)
        result = await factory(config.stores["dc-mirror"])

        login.assert_awaited_once_with(config)
        create.assert_awaited_once_with(config.stores["dc-mirror"], config, bots)
        assert result == "store"

    @pytest.mark.asyncio
    async def test_store_factory_rejects_unknown_backend(self, mocker):
        config = Config.from_dict(CONFIG)
        mocker.patch("main.telegram.login", mocker.AsyncMock())
        factory = await create_store_factory(config)

        store_cfg = mocker.Mock(backend="carrier-pigeon")
        with pytest.raises(ValueError, match="Unsupported backend"):
            await factory(store_cfg)

    @pytest.mark.asyncio
    async def test_store_factory_skips_the_login_of_unused_backends(self, mocker):
        config = Config.from_dict(CONFIG)
        config.stores.clear()
        login = mocker.patch("main.telegram.login", mocker.AsyncMock())

        await create_store_factory(config)

        login.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_run_server(self, mocker):
        # Setup mocks
        mock_get_logger = mocker.patch("main.logging.getLogger")
        mock_uvicorn_config = mocker.patch("main.UvicornConfig")
        mock_server_class = mocker.patch("main.Server")
        mock_app = mocker.Mock()
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_server = mocker.Mock()
        mock_server.serve = mocker.AsyncMock()

        mock_get_logger.return_value = mock_logger
        mock_uvicorn_config.return_value = mock_config
        mock_server_class.return_value = mock_server

        # Call function
        await run_server(mock_app, "localhost", 8080, "Test Server")

        # Assertions
        mock_logger.info.assert_called_once_with(
            "Starting Test Server server on localhost:8080"
        )
        mock_uvicorn_config.assert_called_once_with(
            mock_app,
            host="localhost",
            port=8080,
            loop="none",
            log_level="info",
        )
        mock_server_class.assert_called_once_with(config=mock_config)
        mock_server.serve.assert_called_once()

    @pytest.mark.asyncio
    async def test_main(self, mocker):
        # Setup mocks
        mock_get_config = mocker.patch("main.get_config")
        mock_create_clients = mocker.patch("main.create_clients")
        mock_create_app = mocker.patch("main.create_app")
        mock_run_server = mocker.patch("main.run_server")
        mocker.patch("main.start_replication_workers", return_value=[])
        mock_config = mocker.Mock()
        mock_config.tgdcfs.server.host = "0.0.0.0"
        mock_config.tgdcfs.server.port = 9000
        mock_config.tgdcfs.cache.enabled = False
        mock_config.tgdcfs.listing.message_cache_size = 1024
        mock_config.replication_queue_file = None

        mock_clients = mocker.Mock()
        stats = mocker.Mock()
        stats.close = mocker.AsyncMock()
        mock_clients.values.return_value = [
            mocker.Mock(file_api=mocker.Mock(stats=stats))
        ]
        mock_app = mocker.Mock()

        mock_get_config.return_value = mock_config
        mock_create_clients.return_value = mock_clients
        mock_create_app.return_value = mock_app
        mock_run_server.return_value = None

        # Call function
        await main()

        # Assertions
        mock_get_config.assert_called_once()
        mock_create_clients.assert_called_once()
        assert mock_create_clients.call_args.args[0] is mock_config
        mock_create_app.assert_called_once()
        assert mock_create_app.call_args.args[:2] == (mock_clients, mock_config)
        mock_run_server.assert_called_once_with(mock_app, "0.0.0.0", 9000, "TGDCFS")
        stats.close.assert_awaited_once()
