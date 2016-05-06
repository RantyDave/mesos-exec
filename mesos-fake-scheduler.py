# (c) David Preece 2016
# davep@polymath.tech : https://polymath.tech/ : https://github.com/rantydave
# Licensed under CDDL https://opensource.org/licenses/CDDL-1.0
"""A very skeleton Mesos scheduler written to the HTTP API"""

import time
import logging
import httplib

import subscribe

logging.basicConfig(level=logging.DEBUG, format='%(asctime)-11s %(levelname)-8s %(message)s')


# scheduler.proto has the actual documentation on this
class FakeScheduler(subscribe.SubscribeConnection):

    initial_post = {
        "subscribe": {
            "framework_info": {
                "user": "",  # this is a username to launch under - blank means "current"
                "name": "Fake"
            }
        }
    }

    def __init__(self, host, port):
        super(FakeScheduler, self).__init__(host, port, "scheduler", FakeScheduler.initial_post)

    def respond(self, obj):
        # declining offers for now, gives the master something to do :)
        # if obj['type'] == 'OFFERS':
        #     reply = {
        #         'type': 'DECLINE',
        #         'framework_id': {
        #             'value': self.fwk_id
        #         },
        #         'decline': {
        #             "offer_ids": [{"value": offer['id']['value']} for offer in obj['offers']['offers']]
        #         }
        #     }
        #     logging.info("Declining Mesos offers: " + str(reply['decline']['offer_ids']))
        #     return reply

        # Let's bags some resources and see what happens there
        if obj['type'] == 'OFFERS':
            offer = obj['offers']['offers'][0]
            reply = {
                'type': 'ACCEPT',
                'framework_id': {
                    'value': self.fwk_id
                },
                'accept': {
                    'offer_ids': [{
                            'value': str(offer['id'])
                    }]
                },
                'operations': [{
                    'type': 'LAUNCH',
                    'launch': [{
                        'container_id': '123'
                    }]
                }]
            }
            logging.info("Accepting Mesos offer: " + str(reply))
            return reply

try:
    scheduler = FakeScheduler("mesos-master", "5050")
    while True:
        time.sleep(10000)
except KeyboardInterrupt:
    logging.info("Caught KeyboardInterrupt, closing")
    scheduler.stop()
except httplib.HTTPException as e:
    logging.critical("Received HTTP exception from subscribe connection: " + e.message)
