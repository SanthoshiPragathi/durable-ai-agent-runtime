"""A TCP proxy that delays every chunk it forwards, to model a slow route to PostgreSQL.

Docker Desktop on Windows reaches the container through a port proxy, so each new connection and each
round trip costs far more than on a Linux host. This reproduces that cost anywhere, without Docker.
It runs its own event loop in a thread, so synchronous and asynchronous tests can both use it.
"""

import asyncio
import threading
from collections.abc import Iterator
from contextlib import contextmanager


class LatencyProxy:
    def __init__(self, target_host: str, target_port: int, delay: float) -> None:
        self.target = (target_host, target_port)
        self.delay = delay
        self.port = 0
        self.connections = 0
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._server: asyncio.AbstractServer | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._server = self._loop.run_until_complete(asyncio.start_server(self._handle, "127.0.0.1", 0))
        self.port = self._server.sockets[0].getsockname()[1]
        self._ready.set()
        self._loop.run_forever()

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                await asyncio.sleep(self.delay)
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def _handle(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        self.connections += 1
        try:
            server_r, server_w = await asyncio.open_connection(*self.target)
        except OSError:
            client_w.close()
            return
        await asyncio.gather(self._pipe(client_r, server_w), self._pipe(server_r, client_w))

    def start(self) -> "LatencyProxy":
        self._thread.start()
        self._ready.wait(timeout=5)
        return self

    def stop(self) -> None:
        async def shutdown() -> None:
            if self._server:
                self._server.close()

        asyncio.run_coroutine_threadsafe(shutdown(), self._loop).result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


@contextmanager
def latency_proxy(target_host: str, target_port: int, delay: float) -> Iterator[LatencyProxy]:
    proxy = LatencyProxy(target_host, target_port, delay).start()
    try:
        yield proxy
    finally:
        proxy.stop()
