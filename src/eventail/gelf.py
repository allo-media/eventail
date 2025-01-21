"""GELF log format support."""

import json
import time
from typing import Dict, Protocol

from eventail.log_criticity import CRITICITY_LABELS


class Endpoint(Protocol):
    HOSTNAME: str
    ID: int
    logical_service: str


class GELF:
    def __init__(
        self,
        endpoint: Endpoint,
        criticity: int,
        short: str,
        full: str = "",
        conversation_id: str = "",
        additional_fields: Dict = {},
    ) -> None:
        level_name = CRITICITY_LABELS[criticity % 8]
        log = {
            "version": "1.1",
            "short_message": short,
            "full_message": full,
            "level": criticity,
            "_levelname": level_name,
            "host": f"{endpoint.logical_service}@{endpoint.HOSTNAME}",
            "timestamp": time.time(),
            "_conversation_id": conversation_id,
            "_logical_service": endpoint.logical_service,
            "_worker_pid": endpoint.ID,
        }
        for key, value in additional_fields.items():
            assert key != "id" and key != "_id", "GELF: 'id' can't be used as key name!"
            if key.startswith("_"):  # already escaped
                log[key] = value
            else:
                log[f"_{key}"] = value
        self._log = log
        self._routing_key = "{}.{}".format(endpoint.logical_service, level_name)

    @property
    def payload(self) -> bytes:
        return json.dumps(self._log).encode("utf-8")

    @property
    def routing_key(self) -> str:
        return self._routing_key
