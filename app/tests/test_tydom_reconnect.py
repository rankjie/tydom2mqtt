import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from configuration.Configuration import Configuration
from mqtt.MqttClient import MqttClient
from tydom.TydomClient import TydomClient
from websockets.exceptions import ConnectionClosedOK
from websockets.frames import Close


@pytest.fixture
def application(monkeypatch):
    # Import the entry point without loading Hass.io options or live credentials.
    configuration = SimpleNamespace(
        log_level="INFO",
        tydom_polling_interval=300,
        health_enabled=False,
        tydom_mac="001A25000000",
        tydom_ip="localhost",
        tydom_password="test-password",
        thermostat_cool_mode_temp_default=26,
        thermostat_heat_mode_temp_default=16,
        tydom_alarm_pin=None,
        thermostat_custom_presets=None,
        mqtt_host="localhost",
        mqtt_port=1883,
        mqtt_user=None,
        mqtt_password=None,
        mqtt_ssl=False,
        tydom_alarm_home_zone=1,
        tydom_alarm_night_zone=2,
    )
    monkeypatch.setattr(Configuration, "load", lambda: configuration)
    monkeypatch.setattr(TydomClient, "__init__", lambda self, **kwargs: None)
    monkeypatch.setattr(MqttClient, "__init__", lambda self, **kwargs: None)
    spec = importlib.util.spec_from_file_location(
        "test_application", Path(__file__).parents[1] / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.tydom_client = SimpleNamespace(
        connect=AsyncMock(),
        setup=AsyncMock(),
        disconnect=AsyncMock(),
        post_refresh=AsyncMock(),
        polling_interval=300,
        connection=SimpleNamespace(recv=AsyncMock()),
    )
    # Avoid real retry/polling delays; the mocked I/O controls termination.
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    return module


def closed_connection():
    return ConnectionClosedOK(Close(1000, ""), Close(1000, ""), True)


@pytest.mark.parametrize("failure", [closed_connection, lambda: ConnectionResetError()])
def test_listener_recovers_when_reconnect_setup_fails(application, failure):
    client = application.tydom_client
    client.connection.recv.side_effect = [closed_connection(), asyncio.CancelledError()]
    client.setup.side_effect = [None, failure(), None]

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(application.listen_tydom(), timeout=1)

    asyncio.run(scenario())
    assert client.connect.await_count == 3
    assert client.setup.await_count == 3
    assert client.connection.recv.await_count == 2


@pytest.mark.parametrize("failure", [closed_connection, lambda: ConnectionResetError()])
def test_polling_resumes_after_connection_failure(application, failure):
    client = application.tydom_client
    client.post_refresh.side_effect = [failure(), None, asyncio.CancelledError()]

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(application.poll_device_tydom(), timeout=1)

    asyncio.run(scenario())
    assert client.post_refresh.await_count == 3


def test_listener_retries_an_initial_connection_failure(application):
    client = application.tydom_client
    client.connect.side_effect = [ConnectionRefusedError(), None]
    client.connection.recv.side_effect = asyncio.CancelledError()

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(application.listen_tydom(), timeout=1)

    asyncio.run(scenario())
    assert client.connect.await_count == 2
    client.setup.assert_awaited_once()
    application.asyncio.sleep.assert_awaited_once_with(application.RECONNECT_DELAY)


def test_unexpected_message_failure_is_not_swallowed(application):
    application.tydom_client.connection.recv.side_effect = [
        b"message",
        asyncio.CancelledError(),
    ]
    handler = SimpleNamespace(
        incoming_triage=AsyncMock(side_effect=ValueError("bad data"))
    )
    application.MessageHandler = Mock(return_value=handler)

    async def scenario():
        with pytest.raises(ValueError, match="bad data"):
            await asyncio.wait_for(application.listen_tydom(), timeout=0.1)

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [None, ValueError("unexpected failure")])
def test_critical_task_stopping_exits_with_failure(application, failure):
    async def background():
        if failure is not None:
            raise failure

    async def scenario():
        task = asyncio.create_task(background(), name="listener")
        await asyncio.gather(task, return_exceptions=True)
        with pytest.raises(SystemExit) as error:
            application.exit_on_task_failure(task)
        assert error.value.code == 1

    asyncio.run(scenario())


def test_shutdown_cancellation_does_not_exit_with_failure(application):
    async def scenario():
        task = asyncio.create_task(application.listen_tydom())
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        application.exit_on_task_failure(task)

    asyncio.run(scenario())


def test_websocket_connect_failure_reaches_the_retry_boundary(monkeypatch):
    client = TydomClient(
        mac="001A25000000",
        password="test-password",
        polling_interval=300,
        thermostat_cool_mode_temp_default=26,
        thermostat_heat_mode_temp_default=16,
        host="localhost",
    )
    response = Mock()
    response.headers = {"WWW-Authenticate": "test challenge"}
    response.read.return_value = b""
    handshake = Mock()
    handshake.return_value.getresponse.return_value = response
    monkeypatch.setattr("http.client.HTTPSConnection", handshake)
    monkeypatch.setattr(client, "build_digest_headers", Mock(return_value="test auth"))
    monkeypatch.setattr(
        "websockets.connect", AsyncMock(side_effect=ConnectionRefusedError)
    )

    with pytest.raises(ConnectionRefusedError):
        asyncio.run(client.connect())
    assert handshake.call_args.kwargs["timeout"] == client.reply_timeout
