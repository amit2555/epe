#!/usr/bin/env python

from controller import Controller


if __name__ == "__main__":
    app = Controller()
    app.run()


'''

{u'counter': 2, u'pid': u'6652', u'exabgp': u'3.4.8', u'host': u'amit-VirtualBox', u'neighbor': {u'ip': u'10.1.1.1', u'state': u'up', u'asn': {u'peer': u'100', u'local': u'100'}, u'address': {u'peer': u'10.1.1.1', u'local': u'10.1.1.10'}}, u'time': 1493863775, u'ppid': u'2844', u'type': u'state'}

{u'counter': 4, u'pid': u'6652', u'exabgp': u'3.4.8', u'host': u'amit-VirtualBox', u'neighbor': {u'ip': u'10.1.1.1', u'message': {u'update': {u'attribute': {u'origin': u'igp', u'med': 0, u'community': [[100, 3]], u'local-preference': 100, u'originator-id': u'3.3.3.3', u'cluster-list': [u'1.1.1.1']}, u'announce': {u'ipv4 nlri-mpls': {u'3.3.3.3': {u'192.168.35.5/32': {u'label': [18]}}}}}}, u'asn': {u'peer': u'100', u'local': u'100'}, u'address': {u'peer': u'10.1.1.1', u'local': u'10.1.1.10'}}, u'time': 1493863776, u'ppid': u'2844', u'type': u'update'}


'''
