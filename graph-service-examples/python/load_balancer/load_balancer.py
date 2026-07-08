# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from aiohttp import ClientConnectorError, ServerDisconnectedError
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import gc
import threading
import logging

from gremlin_python.driver.resultset import ResultSet
from gremlin_python.process.traversal import Bytecode


class RoundRobinClientRemoteConnection:
    def __init__(self, endpoints, traversal_source="g", health_check_interval=10,
                 logger: logging.Logger = None, log_level: int = logging.INFO):

        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(log_level)

        self._clients = [
            DriverRemoteConnection(f"ws://{host}/gremlin", traversal_source)
            for host in endpoints
        ]
        self._traversal_source = traversal_source
        self._available = [True] * len(self._clients)
        self._pos = 0
        self._lock = threading.Lock()

        # health checker
        self._health_interval = health_check_interval
        self._stop_event = threading.Event()
        self._health_thread   = threading.Thread(
            target=self.health_check_loop, daemon=True
        )
        self._health_thread.start()

        self._logger.debug("Initialized load-balancer with endpoints: %s", endpoints)

    def add_host(self, endpoint):
        self._clients.append(DriverRemoteConnection(f"ws://{endpoint}/gremlin", self._traversal_source))
        self._available.append(True)
        self._logger.info("Added host %s", endpoint)

    def remove_host(self, endpoint):
        try:
            with self._lock:
                index = -1
                for host in self._clients:
                    if endpoint in host.url:
                        index = self._clients.index(host)
                if not index == -1:
                    del self._clients[index]
                    del self._available[index]
                    self._logger.info("Removed host %s", endpoint)
                else:
                    self._logger.warning("Tried to remove non-existent host %s", endpoint)
        except ValueError:
            self._logger.warning("Tried to remove non-existent host %s", endpoint)

    def submit(self, bytecode: Bytecode, aliases=None) -> ResultSet:
        for _ in range(len(self._clients)):
            with self._lock:
                healthy = [i for i, ok in enumerate(self._available) if ok]
                if not healthy:
                    raise RuntimeError("No healthy Gremlin hosts available")
                pick = healthy[self._pos % len(healthy)]
                self._pos += 1
            try:
                result = self._clients[pick].submit(bytecode)
                with self._lock:
                    self._available[pick] = True
                self._logger.debug("Traversal submitted via connection #%d", pick)
                return result
            except (ClientConnectorError, ServerDisconnectedError) as e:
                with self._lock:
                    self._available[pick] = False
                self._logger.warning("Connection #%d failed: %s – marking host down", pick, e)
                last_exc = e
            except Exception:
                raise
        self._logger.error("All endpoints failed – raising")
        raise RuntimeError("All Gremlin endpoints failed") from last_exc

    def health_check_loop(self):
        while not self._stop_event.wait(self._health_interval):
            self._logger.debug("Running health check")
            for i, ok in enumerate(self._available):
                if not ok:
                    try:
                        self._clients[i]._client.submit("g.V().limit(1).toList()")
                        with self._lock:
                            self._available[i] = True
                            self._logger.info("Host #%d is healthy again", i)
                    except Exception:
                        with self._lock:
                            self._logger.debug("Host #%d still down", i)
                            self._available[i] = False
                        pass

    def get_clients(self):
        return list(self._clients)

    def get_available(self):
        return list(self._available)

    def close(self):
        self._stop_event.set()
        self._health_thread.join()
        # Force collection of any lingering aiohttp ClientResponse objects before
        # each connection's private event loop closes, avoiding "Event loop is
        # closed" errors from __del__ finalizers running after the fact.
        gc.collect()
        for c in self._clients:
            try:
                c.close()
            except Exception:
                pass
        gc.collect()
        self._logger.debug("Load-balancer shut down")
