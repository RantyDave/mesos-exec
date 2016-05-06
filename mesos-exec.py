# (c) David Preece 2016
# davep@polymath.tech : https://polymath.tech/ : https://github.com/rantydave
# Licensed under CDDL https://opensource.org/licenses/CDDL-1.0
"""A very skeleton Mesos executor using the HTTP interface"""


import logging
import httplib
import time
import subscribe


logging.basicConfig(level=logging.DEBUG, format='%(asctime)-11s %(levelname)-8s %(message)s')


# Framework and executor ID set in marathon
class FakeExecutor(subscribe.SubscribeConnection):

    initial_post = {
        "executor_id": {
            "value": "2bb7f266-f12c-11e5-b7d4-2383f1339473"
        },
        "framework_id": {
            "value": "29d2c1e0-d9cb-4374-9a1a-3010b40e6c06-0003"
        },
        "subscribe": {
            "unacknowledged_tasks": [],
            "unacknowledged_updates": []
        }
    }

    def __init__(self, host, port):
        super(FakeExecutor, self).__init__(host, port, "executor", FakeExecutor.initial_post)

    def respond(self, obj):
        return None


executor = None
try:
    executor = FakeExecutor("mesos-slave", "5051")
    while True:
        time.sleep(10000)
except KeyboardInterrupt:
    logging.info("Caught KeyboardInterrupt, closing")
    if executor is not None:
        executor.stop()
except httplib.HTTPException as e:
    logging.critical("Received HTTP exception from subscribe connection: " + e.message)