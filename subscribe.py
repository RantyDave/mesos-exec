# (c) David Preece 2016
# davep@polymath.tech : https://polymath.tech/ : https://github.com/rantydave
# Licensed under CDDL https://opensource.org/licenses/CDDL-1.0
"""Abstracts the runloop-esque connection from an executor to Mesos's HTTP API"""

import httplib
import threading
import json
import time
import logging
import socket


class SubscribeConnection(threading.Thread):

    def __init__(self, host, port, call, post_dict):
        """Create a 'subscribe' connection to mesos"""
        super(SubscribeConnection, self).__init__()
        # set stuff up
        self.host = host
        self.port = port
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        post_dict['type'] = 'SUBSCRIBE'
        self.url_root = 'http://' + host + ':' + str(port)
        self.url = self.url_root + '/api/v1/' + call
        self.fwk_id = None
        self.stream_id = None

        # actually call
        self.first_reply_failed = False
        self.connection = httplib.HTTPConnection(host, port)
        self.connection.set_debuglevel = logging.getLogger().getEffectiveLevel()
        try:
            self.connection.request('POST', self.url, json.dumps(post_dict), self.headers)
        except socket.error:
            raise httplib.HTTPException("Could not connect: " + self.url_root)
        self.start()

        # block until we have a reply
        logging.debug("Waiting for the subscribed message to be parsed")
        while self.fwk_id is None and self.stream_id is None and not self.first_reply_failed:
            time.sleep(0.01)
        if self.first_reply_failed:
            raise httplib.HTTPException("First reply from server failed")

    def respond(self, obj):
        """Overload to respond to incoming events, return a dictionary of the response (if there is one)"""

    def stop(self):
        self.connection.close()

    ##################################################################################################

    def run(self):
        """An event loop"""
        response = self._get_response(self.connection)  # remember this one response is held open
        if response.status < 200 or response.status >= 300:
            # In the absence of any real choice, just bin the thread
            logging.critical("Initial response failed: " + response.read())
            self.first_reply_failed = True
            return

        # The actual event loop
        while True:
            # collect the object from mesos
            try:
                length = SubscribeConnection._get_length(response)
            except AttributeError:
                logging.info("Cleanly disconnected from Mesos")
                return
            message = response.read(length)
            logging.debug('Incoming from Mesos: ' + message)
            try:
                obj = json.loads(message)
            except ValueError:
                logging.warning('Message received from Mesos was not json: ' + message)
                continue

            # deal with no-brainers
            if obj['type'] == 'HEARTBEAT':
                continue
            if obj['type'] == 'SUBSCRIBED':
                try:
                    objects = obj['subscribed']
                    self.fwk_id = objects['framework_id']['value']
                    self.stream_id = response.getheader('')
                except KeyError:
                    logging.critical("SUBSCRIBED message did not return subscribed objects (or maybe just fwk_id)")
                    return
                logging.info("Subscribed to: " + self.url)
                logging.info("Framework id: " + self.fwk_id)
                continue

            # pass more specific results to the subclass
            reply = self.respond(obj)
            if reply is not None:
                self._send(reply)

    def _send(self, obj):
        """Sends an action to Mesos over a fresh connection"""
        connection = httplib.HTTPConnection(self.host, self.port)
        connection.request('POST', self.url, json.dumps(obj), self.headers)
        resp = self._get_response(connection)
        logging.debug("Response from Mesos: " + resp.read())
        connection.close()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException("Failed in subscribe._send: " + resp.reason)

    @staticmethod
    def _get_response(conn):
        response = None
        loops = 0
        while response is None:
            try:
                response = conn.getresponse()
            except httplib.ResponseNotReady:
                if loops == 0:
                    logging.debug("Waiting for a response from Mesos")
                if loops == 500:
                    return None
                loops += 1
                time.sleep(0.01)
        return response

    @staticmethod
    def _get_length(response):
        len_string = ""
        byte = response.read(1)
        while '0' <= byte <= '9':
            len_string += byte
            byte = response.read(1)
        return int(len_string)
