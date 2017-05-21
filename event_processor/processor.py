#!/usr/bin/env python

import threading
import logging
import queue
import time
from sdn.lib import db


logger = logging.getLogger(__name__)


class Processor(threading.Thread):
    """Receive events from DB and process them."""

    EVENT_RATE = 1

    def __init__(self, event_queue):
        super(Processor, self).__init__()
        self.event_queue = event_queue
        self.shutting_down = threading.Event() 
        self.event_rate = Processor.EVENT_RATE


    def process_event(self, event):
        pass


    @staticmethod
    def save_result(result):
        pass


    def event_handler(self):
        """Locates new events to work on and processes them."""

        while not self.shutting_down.is_set():
            with db.Db() as db_conn:

                status = db.Event.STATUS_CODES['QUEUED']
                events = db_conn.get_events_by_status(
                    status, limit=self.event_rate)
                if not events:
                    logger.info('No events to process')
                    time.sleep(2)
                    continue

            for event in events:
                result = self.process_event(event)


    def run(self):
        self.event_handler()


    def shutdown(self):
        """Gracefully terminate threads."""

        logger.debug('Terminating process: {}'.format(
            threading.current_thread().getName()))
        self.shutting_down.set()
 
