#
# MIT License
#
# Copyright (c) 2018-2019 Groupe Allo-Media
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""Share temporary data between service instances."""


from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

import cbor2 as cbor
import redis
import redis.sentinel
from redis.exceptions import ConnectionError, RedisError

DEFAULT_TTL = 270_000
HCI = 30  # Redis connection health check interval (redis-py internal)


def to_host(host_str: str) -> Tuple[str, int]:
    host, port = host_str.split(":")
    return (host, int(port))


class STDataStore:
    """Temporary tmp_store shared among instances.

    To avoid race conditions between workers, all operations are atomic and we provide destructive reads,
    so that only one worker can take ownership a tmp_stored value.

    All stored value have a common, limited, Time To Live to avoid memory leaks.

    Current implementation relies on Redis.
    """

    def __init__(
        self, redis_client: redis.Redis, namespace: str, ttl: int = DEFAULT_TTL
    ) -> None:
        """Instantiate a new tmp_store. Not to be called directly!

        Namespaces prevent key collisions: you can safely use
        the same keys in two different tmp_stores as long as they use
        different namespaces.
        Time to live (ttl) argument is there to avoid memory leaks in
        case some data slots are "forgotten" (in seconds).
        """
        self.redis = redis_client
        self.namespace = namespace
        self.ttl = ttl
        self.health_key = self._absolute(f"health {uuid4()}")

    @classmethod
    def simple_redis_backend(
        cls,
        redis_server: Tuple[str, int],
        database: int,
        password: Optional[str],
        namespace: str,
        ttl: int = DEFAULT_TTL,
    ) -> "STDataStore":
        """Use this to connect to a single Redis database."""
        return cls(
            redis.Redis(  # type: ignore
                host=redis_server[0],
                port=redis_server[1],
                db=database,
                health_check_interval=HCI,
            ),
            namespace,
            ttl,
        )

    @classmethod
    def sentinel_backend(
        cls,
        sentinels: List[str],
        redis_service_name: str,
        database: int,
        password: Optional[str],
        namespace: str,
        ttl: int = DEFAULT_TTL,
    ) -> "STDataStore":
        """Use this to connect to Redis Sentinel or Single Redis database.

        If more than one server are provided as `sentinels`, use Sentinel mode, otherwise fallback to single mode.
        """
        hosts = [to_host(hs) for hs in sentinels]
        if len(hosts) == 1:
            return cls.simple_redis_backend(
                hosts[0], database, password, namespace, ttl
            )
        sentinel = redis.sentinel.Sentinel(
            hosts, db=database, password=password, health_check_interval=HCI
        )
        return cls(sentinel.master_for(redis_service_name), namespace, ttl)  # type: ignore

    def _absolute(self, name):
        """Convert local name to absolute"""
        return self.namespace + name

    def is_down(self):
        """If STDataStore is down return error message else empty string."""

        try:
            self.set(self.health_key, "health")
            val = self.pop(self.health_key)
        except (RedisError, ConnectionError) as e:
            return str(e)
        else:
            return "" if val == "health" else "inconsistent data read!"

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Store `value`  under key `key`.

        `value` can be any serializable python data.
        """
        self.redis.setex(self._absolute(key), ttl or self.ttl, cbor.dumps(value))

    def mset(self, data: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """Atomically store multiple `key: value` pairs given in dictionary."""
        ttl = ttl or self.ttl
        pipeline = self.redis.pipeline()
        pipeline.mset(
            {self._absolute(key): cbor.dumps(value) for key, value in data.items()}
        )
        for key in data.keys():
            pipeline.expire(self._absolute(key), ttl)
        pipeline.execute()

    def pop(self, key: str) -> Any:
        """Retrieve the data associated with `key` and delete the key atomically.

        Return None if operation key does not exist.
        """
        rkey = self._absolute(key)
        res = self.redis.pipeline().get(rkey).delete(rkey).execute()  # type: ignore
        return cbor.loads(res[0]) if res[0] is not None else None

    def peek(self, key: str) -> Any:
        """Peek at the value associated to `key`.

        The record won't be erased.
        """
        rkey = self._absolute(key)
        val = self.redis.get(rkey)
        return cbor.loads(val) if val else None

    def mpop(self, *keys: str) -> Optional[List[Any]]:
        """Retrieve values for multiple keys and delete them, atomically, if they are all set.

        If one of the key is not set, nothing is deleted.

        Return None if operation fails or if one of the key does not exist."""

        rkeys = [self._absolute(key) for key in keys]

        def get_all_or_nothing(pipe):
            values = pipe.mget(rkeys)
            pipe.multi()
            if all(values):
                pipe.delete(*rkeys)
                return values
            else:
                return None

        res = self.redis.transaction(
            get_all_or_nothing, *rkeys, value_from_callable=True, watch_delay=0.1
        )
        return [cbor.loads(r) for r in res] if res else None

    def peek_or_create(
        self,
        key: str,
        factory: Callable[[], Any],
        max_op_time: int,
        ttl: Optional[int] = None,
    ) -> Any:
        """Get value associated with ``key`` if it exists, otherwise create it and store it.

        The ``factory`` is a callable that creates new values on demand.
        The check and, if needed, the creation of the value are concurrency safe : if the data is missing
        (or its TTL expired), only one worker will be able to create the value, all others will wait, thanks
        to a lock.

        To avoid dead locks,``max_op_time`` is the maximum time in seconds the lock is hold. The factory
        must produce a value within that time span.
        """
        with self.redis.lock(key + "_lock", timeout=max_op_time):
            val = self.peek(key)
            if val is None:
                val = factory()
                self.set(key, val, ttl)
        return val
