import os, sys
import platform
import socket

# host platform - windows / linux
def getsystem():
    return platform.system()

# host name
def gethostname():
    return socket.gethostname()

# host ip addr
def gethostbyname():
    return socket.gethostbyname(socket.gethostname())

def gethostipaddr():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    hostaddr = s.getsockname()[0]
    s.close()
    return hostaddr

