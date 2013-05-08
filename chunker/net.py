from datetime import datetime
from chunker.util import log
from pydht import DHT


_default_peers = [
    {
        "host": "violet.shishnet.org",
        "port": 52525
    }
]


class MetaNet(object):
    def __init__(self, config):
        self._log("Connecting to DHT")
        self.dht = DHT("0.0.0.0", 52525)
        self.public_contact = ("127.0.0.1", 52525)
        for peer in config.get("peers", _default_peers):
            self.dht.bootstrap(peer["host"], peer["port"])

    def _log(self, msg):
        log("[MetaNet] %s" % msg)

    def offer(self, chunk):
        self._log("Offering %s" % chunk.id)

        if chunk.id not in self.dht:
            self.dht[chunk.id] = [self.public_contact]
        else:
            self.dht[chunk.id] = self.dht[chunk.id] + [self.public_contact]

    def request(self, chunk):
        self._log("Requesting %s" % chunk.id)
