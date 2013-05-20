from datetime import datetime
from pydht import DHT
from time import sleep
import stun
import json
import logging

from chunker.net import PeerFinder, Peer


log = logging.getLogger(__name__)


class DHTPeerFinder(PeerFinder):
    # TODO: rename MetaNet to this
    _default_peers = [
        {
            "host": "violet.shishnet.org",
            "port": 52525
        }
    ]

    def __init__(self, config):
        self._log("Getting external IP info")
        nat_type, external_ip, external_port = stun.get_ip_info()
        self._log("Public addr: %s:%s" % (external_ip, external_port))

        self._log("Connecting to DHT")
        self.dht = DHT("0.0.0.0", 52525)
        self.public_contact = (external_ip, external_port)
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
