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
import logging
import time
from typing import Callable

from .base import Service

LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.DEBUG)


class ReconnectingSupervisor(object):
    """This is an example supervisor that will reconnect if the nested
    Service indicates that a reconnect is necessary.

    """

    def __init__(
        self, service_factory: Callable[..., Service], *args, **kwargs
    ) -> None:
        """Supervises a service and manages automatic reconnection.

        The ``*args`` and ``**kwargs**`` are passed unchanged to the
        ``service_factory``.
        """
        self._reconnect_delay = 0
        self.service: Service = service_factory(*args, **kwargs)

    def run(self) -> None:
        """Run the service until the service chooses to exit without reconnecting."""
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
