#!/usr/bin/env python
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
import datetime
import sys

from eventail.async_service.pika import Service, ReconnectingSupervisor
from eventail.batch_processor import Batch
from eventail.log_criticity import NOTICE


class MinuteClock(Service):

    last_time = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch = Batch(self, 60, 61, self.minute)

    def on_ready(self):
        self.healthcheck()

    def on_SecondTicked(self, payload, conversation_id, meta):
        self.batch.push(payload, conversation_id, meta)

    def minute(self, seconds):
        if seconds:
            out_event = (
                "MinuteTicked" if len(seconds) == 60 else "IncompleteMinuteTicked"
            )
            last_event = seconds[-1]
            dtime = datetime.datetime.fromtimestamp(last_event.payload["unix_time"])
            self.publish_event(
                out_event, {"iso_time": dtime.isoformat()}, last_event.conversation_id
            )

    def healthcheck(self):
        self.log(NOTICE, "I'm fine!")
        self.call_later(60, self.healthcheck)


if __name__ == "__main__":

    urls = sys.argv[1:] if len(sys.argv) > 2 else ["amqp://localhost"]
    minute_clock = ReconnectingSupervisor(
        MinuteClock, urls, ["SecondTicked"], [], "minute"
    )
    print("To exit press CTRL+C")
    minute_clock.run()
    print("Bye!")
