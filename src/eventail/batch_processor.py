#
# MIT License
#
# Copyright (c) 2021 Groupe Allo-Media
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

"""Process incoming events in batches."""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from eventail.async_service.pika import JSON_MODEL, Service


@dataclass
class RawEvent:
    payload: JSON_MODEL
    conversation_id: str
    meta: JSON_MODEL


class Batch:
    """A class for batch processing of events.

    You can instantiate as many Batch as you need, but only one thread can update it.
    You can't use Batch for Commands or Results.
    """

    def __init__(
        self,
        endpoint: Service,
        max_entries: int,
        timeout: float,
        callback: Callable[[List[RawEvent]], None],
    ) -> None:
        """Create a new Batch Processor.

          - `max_entries` is the maximum number of events to accumulate before triggering the batch;
          - `timeout` is the maximum time in seconds to wait for a complete batch after the first event
            is pushed;
          - `callback` is a callable that will be called with a list of `RawEvent`
            as arguments when the batch is triggered.

        Once you have a batch processor instance, all you have to do is to `.push()` events to it and it
        takes care of the rest.

        You can manually trigger the batch by calling the `.expire()` method.
        """
        self.endpoint = endpoint
        self.max_entries = max_entries
        self.timeout = timeout
        self.callback = callback
        self._timer: Optional[Any] = None
        self._batch: List[RawEvent] = []

    def push(self, payload: JSON_MODEL, conversation_id: str, meta: JSON_MODEL):
        """Push a new event to the batch."""
        if not self._batch:
            self._timer = self.endpoint.call_later(self.timeout, self._expired)
        self._batch.append(RawEvent(payload, conversation_id, meta))
        if len(self._batch) == self.max_entries:
            self.expire()

    def expire(self):
        if self._timer is not None:
            self.endpoint.cancel_timer(self._timer)
        self._expired()

    def _expired(self):
        self._timer = None
        try:
            self.callback(self._batch)
        finally:
            self._batch.clear()
