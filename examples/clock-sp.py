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
import sys
import time

from eventail.log_criticity import INFO
from eventail.sync_publisher import Endpoint


if __name__ == "__main__":
    assert len(sys.argv) > 1, "Usage: clock-sp amqp-url [amqp-url…]"
    amqp_urls = sys.argv[1:]
    print("Starting…")
    api = Endpoint(amqp_urls, "clock")
    i = 1
    try:
        while True:
            api.publish_event("SecondTicked", {"unix_time": int(time.time())}, str(i))
            api.log(INFO, "ticked!", conversation_id=str(i))
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("bye!")
