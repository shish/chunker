from socket import *

class Peer(object):
    def __init__(self, addr):
        self.addr = addr
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)

    def send(self, msg):
        self.socket.sendto(msg, addr)

    def recv(self):
        data, ancdata, flags, address = self.socket.recvmsg()

    def fileno(self):
        return self.socket.fileno()
