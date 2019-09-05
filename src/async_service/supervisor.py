import logging
import time
from typing import Callable

from .base import Service

LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.DEBUG)


class ReconnectingSupervisor(object):
    """This is an example supervisor that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.

    """

    def __init__(
        self, service_factory: Callable[..., Service], *args, **kwargs
    ) -> None:
        self._reconnect_delay = 0
        self.service: Service = service_factory(*args, **kwargs)

    def run(self) -> None:
        reconnect = True
        while reconnect:
            try:
                self.service.run()
            except KeyboardInterrupt:
                self.service.stop()
                break
            reconnect = self._maybe_reconnect()

    def _maybe_reconnect(self) -> bool:
        if self.service.should_reconnect:
            self.service.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            return True
        return False

    def _get_reconnect_delay(self) -> int:
        if self.service.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
