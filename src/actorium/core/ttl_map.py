import time
from typing import Iterable

__all__ = ["TtlMap"]


class TtlMap[K, V]:
    def __init__(self) -> None:
        self._data: dict[K, V] = {}
        self._valid_until: dict[K, float] = {}

    def __repr__(self) -> str:
        return f"TtlMap({self._data!r})"

    def set(self, k: K, v: V, ttl_seconds: float) -> None:
        self._data[k] = v
        self._valid_until[k] = time.time() + ttl_seconds

    def items(self) -> Iterable[tuple[K, V]]:
        now = time.time()

        for k in list(self._data):  # `self.get` can remove expired entries.
            v = self.get(k, _now=now)
            if v is not None:
                yield k, v

    def keys(self) -> Iterable[K]:
        for k, _ in self.items():
            yield k

    def items_with_remaining_ttl(self) -> Iterable[tuple[K, V, float]]:
        now = time.time()

        for k in list(self._data):  # `self.get` can remove expired entries.
            v = self.get(k, _now=now)
            if v is not None:
                yield k, v, self._valid_until[k] - now

    def get(self, k: K, _now: float | None = None) -> V | None:
        now = _now or time.time()
        try:
            v = self._data[k]
            valid_until = self._valid_until[k]
        except KeyError:
            return None
        else:
            if valid_until >= now:
                return v

            # Key expired.
            del self._data[k]
            del self._valid_until[k]
            return None

    def pop(self, k: K) -> None:
        try:
            del self._data[k]
            del self._valid_until[k]
        except KeyError:
            pass
