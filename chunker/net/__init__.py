from chunker.net.local import Peer

from chunker.net.local import LocalPeerFinder
from chunker.net.dht import DHTPeerFinder
from chunker.net.exchange import ExchangePeerFinder


class MetaNet(object):
    def __init__(self, core):
        self.core = core
        self.local = LocalPeerFinder(core)
        self.dht = DHTPeerFinder(core)
        self.exchange = ExchangePeerFinder(core)

    def start(self):
        self.local.start()
        self.dht.start()
        self.exchange.start()
