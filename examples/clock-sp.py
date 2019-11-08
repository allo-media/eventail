import sys
import time

from py_eda_tools.sync_publisher import Endpoint


if __name__ == "__main__":
    amqp_urls = sys.argv[1:]
    print("Starting…")
    api = Endpoint(amqp_urls, "clock")
    i = 1
    try:
        while True:
            api.publish_event("SecondTicked", {"unix_time": int(time.time())}, str(i))
            print("ticked!")
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("bye!")
