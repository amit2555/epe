#!/usr/bin/env python

import sys
import time
import queue
import logger
import threading
from sdn.lib import db
from sdn.utilities.settings import CONFIG
from automation.tasks import get_interfaces_utilization


logger = logging.getLogger(__name__)


class Collector(threading.Thread):
    """Collect device interface utilization and create events in DB"""

    def __init__(self, work_queue):
        super(Processor, self).__init__()
        self.work_queue = work_queue


    def run(self, device):
        """Receive device-name using queue and run task"""

        while True:
            try:
                device = self.work_queue.get()
                self.process(device)
            finally:
                self.work_queue.task_done()


    def process(self, device):
        """Connect to device and get interface utilization"""

        device_response = get_interfaces_utilization(device)
        logger.debug('Response received from device {}:\n\t{}'.format(device, device_response))
        result = self.analyze(device_response)
    
        if result:
            logger.debug('Following interfaces have high utilization on {}: {}'.format(
                          device, ', '.join(interface['name'] for interface in result)
            self.create_event(device, result)


    @staticmethod
    def analyze(interfaces):
        """Analyze output for high utilization on interesting interfaces"""

        def find_interesting(intfs):
            return [intf
                    for intf in intfs
                        if 'transit' in intf["description"].lower() or
                           'peer' in intf['description'].lower()
                   ]


        THRESHOLD = 10.0
        interesting_interfaces = find_interesting(interfaces)
        return [interface
                for interface in interesting_interfaces
                    if item['output_utilization'] > THRESHOLD
               ]  


    def create_event(device, result):
        pass



def main():

    devices = CONFIG['DEVICES']

    work_queue = queue.Queue()
    for i in range(len(devices)):
        t = Collector(work_queue)
        t.daemon = True
        t.start()

    while True:
        for device in devices:
            work_queue.put(device)
        work_queue.join()
        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)


