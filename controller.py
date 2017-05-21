#!/usr/bin/env python

from utilities.settings import CONFIG
from event_processor.processor import Processor
import functools
import logging
import sys
import json
import select
import queue


NLRI = CONFIG['NLRI']
logger = logging.getLogger(__name__)
logging.basicConfig(filename=CONFIG['LOGFILE'], filemode='w', level=logging.DEBUG)


class Controller(object):

    def __init__(self):
        self.queue = queue.Queue()
        self.processor = Processor(self.queue)
        self.processor.start()


    def update_status(self, status):
        self.labeled_unicast_routes = list()


    def _parse_aspath(self, aspath):
        segments = list()
        as_sequence = aspath.split()
        segments.append({"as-sequence":as_sequence})
        return segments 


    def _parse_communities(self, communities):
        community_list = []
        for community in communities:
            community_list.append({"semantics":community[0], "as-number":community[1]})
        return community_list


    def _parse_attributes(self, attr):
        attributes = dict()
        attributes["origin"]= {"value":attr["origin"]}
        attributes["multi-exit-desc"] = {"med":attr["med"]}
        attributes["local-pref"] = {"pref":attr["local-preference"]}
        attributes["as-path"] = self._parse_aspath(attr["as-path"]) if attr.get("as-path") else {}
        attributes["communities"] = self._parse_communities(attr["community"]) if attr.get("community") else {}
        attributes["originator-id"] = {"originator":attr["originator-id"]} if attr.get("originator-id") else {}
        attributes["cluster-id"] = {"cluster":attr["cluster-list"]} if attr.get("cluster-list") else {}
        return attributes


    def _parse_labels(self, labels):
        labelstack = list()
        for label in labels["label"]:
            labelstack.append({"label-value":label})
        return labelstack


    @staticmethod
    def _route_key(prefix, nexthop):
        return prefix + "_" + nexthop


    @staticmethod
    def check_update_type(update_type):
        def wrapper(func):
            @functools.wraps(func)
            def inner(message):
                update_message = message[update_type][NLRI]
                attributes = message['attribute']
                return func(update_message, attributes)
            return inner
        return wrapper


    @check_update_type('announce')
    def prefix_announced(self, bgp_update, attributes):
        """Parse BGP Update in JSON format received by ExaBGP and transform into ODL format."""

        parsed_attributes = self._parse_attributes(attributes)
        for nexthop in bgp_update:
            parsed_attributes["ipv4-next-hop"] = {"global":nexthop}

            for prefix in bgp_update[nexthop]:
                route_key = self._route_key(prefix, nexthop)
                label_stack = self._parse_labels(bgp_update[nexthop][prefix])
                labeled_unicast_route = {
                                         "route-key":route_key,
                                         "prefix":prefix,
                                         "attributes":parsed_attributes,
                                         "label_stack":label_stack
                                        }

            self.labeled_unicast_routes.append(labeled_unicast_route)
        logger.debug(self.labeled_unicast_routes)


    @check_update_type('withdraw')
    def prefix_withdrawn(self, bgp_update, attributes):
        """Remove BGP-LU Prefixes withdrawn in BGP Update."""

        for nexthop in bgp_update:
            for prefix in bgp_update[nexthop]:
                route_key = self._route_key(prefix, nexthop)
                self.labeled_unicast_routes.remove( filter(lambda x: x["route-key"] == route_key,
                                                           self.labeled_unicast_routes)[0]
                                                  )
        logger.debug(self.labeled_unicast_routes)


    def process_update(self, bgp_update):
        # Accept only BGP Labeled-Unicast updates
        if "announce" in bgp_update and NLRI in bgp_update["announce"]:
            self.prefix_announced(bgp_update)

        if "withdraw" in bgp_update and NLRI in bgp_update["withdraw"]:
            self.prefix_withdrawn(bgp_update)


    def handle_message(self, message):
        if message["type"] == "update":
            bgp_update = message["neighbor"]["message"]["update"]
            self.process_update(bgp_update)

        if message["type"] == "state":
            status = message["neighbor"]["state"]
            self.update_status(status)


    def run(self):
        inputs = [sys.stdin]

        while True:
            read_ready, write_ready, except_ready = select.select(inputs, [], [], 0.01)
            for readable in read_ready:
                if readable is sys.stdin:
                    line = sys.stdin.readline().strip()
                    message = json.loads(line)
                    self.handle_message(message)


