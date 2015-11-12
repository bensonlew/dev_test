# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import hashlib
import web
from mainapp.models.clientkey import ClientKey
import datetime
import functools
from netaddr import IPNetwork, IPAddress
import random
import time


class Signature(object):

    def __init__(self):
        data = web.input()
        if not (hasattr(data, "signature") and hasattr(data, "timestamp") and hasattr(data, "nonce") and hasattr(data, "client")):
            raise web.badrequest

        self._signature = data.signature
        self._timestamp = data.timestamp
        self._nonce = data.nonce
        self._client = data.client
        self._ip = data.client

    def check(self):
        diff = datetime.datetime.now() - datetime.datetime.fromtimestamp(int(self._timestamp))
        client = ClientKey(self._client)
        if client.timelimit and abs(diff.seconds) > client.timelimit:
            raise web.notacceptable
        if client.ipmask:
            clientip = web.ctx.ip
            ip_list = client.ipmask.split(";")
            in_range = False
            for r in ip_list:
                if IPAddress(clientip) in IPNetwork(r):
                    in_range = True
            if in_range is False:
                raise web.unauthorized

        x_list = [client.key, self._timestamp, self._nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        hashcode = sha1.hexdigest()
        if hashcode == self._signature:
            return True
        else:
            return False


def check_sig(f):
    @functools.wraps(f)
    def wrapper(obj):
        sig = Signature()
        if sig.check():
            return f(obj)
        else:
            raise web.unauthorized
    return wrapper


class CreateSignature(object):
    def __init__(self, client):
        self.client = client
        self.nonce = str(random.randint(1000, 10000))
        self.timestamp = str(int(time.time()))
        self.signature = self.get_signature()

    def get_signature(self):
        data = ClientKey(self.client)
        x_list = [data.key, self.timestamp, self.nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        return sha1.hexdigest()
