from datetime import datetime
from chunker.util import log
from pydht import DHT
import stun
import json
from time import sleep
import logging

log = logging.getLogger(__name__)


class PeerFinder(object):
    def __init__(self, core):
        self.core = core

    def start(self):
        pass


class LocalPeerFinder(PeerFinder):
    def __init__(self, core):
        PeerFinder.__init__(self, core)

        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        self.socket.bind(("0.0.0.0", 54545))

        self.sender = Thread(target=self.run_send, name="LocalPeerFinder[Send]")
        self.recver = Thread(target=self.run_recv, name="LocalPeerFinder[Recv]")

        self.sender.daemon = True
        self.recver.daemon = True

    def start(self):
        self.sender.start()
        self.recver.start()

    def run_send(self):
        while True:
            log.info("Broadcasting local repos")
            for repo in self.core.repos:
                self.socket.sendto(
                    repo.encrypt(
                        json.dumps({
                            "uuid": repo.uuid,
                        })
                    ),
                    ("255.255.255.255", 54545)
                )
            sleep(60)

    def run_recv(self):
        while True:
            raw_data, addr = self.socket.recvfrom(4096)
            log.info("Got possible repo broadcast from %s" % (addr, ))
            try:
                for repo in self.core.repos:
                    data = repo.decrypt(raw_data)
                    struct = json.loads(data)
                    if data["uuid"] == repo.uuid:
                        repo.add_peer(addr)
            except Exception as e:
                print e



class PeerExPeerFinder(PeerFinder):
    pass


class DHTPeerFinder(PeerFinder):
    # TODO: rename MetaNet to this
    _default_peers = [
        {
            "host": "violet.shishnet.org",
            "port": 52525
        }
    ]


class MetaNet(object):
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
