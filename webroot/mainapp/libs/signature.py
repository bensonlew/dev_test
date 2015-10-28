# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import hashlib
import web
from mainapp.models.clientkey import ClientKey
import datetime
import functools


class Signature(object):

    def __init__(self):
        data = web.input()
        if not (hasattr(data, "signature") and hasattr(data, "timestamp") and hasattr(data, "nonce") and hasattr(data, "client")):
            raise web.badrequest

        self._signature = data.signature
        self._timestamp = data.timestamp
        self._nonce = data.nonce
        self._client = data.client
        self._key = ClientKey(self._client).get_key()

    def check(self):
        diff = datetime.datetime.now() - datetime.datetime.fromtimestamp(int(self._timestamp))
        if abs(diff.seconds) > 60:
            raise web.notacceptable
        list = [self._key, self._timestamp, self._nonce]
        list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, list)
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
            raise web.notacceptable
    return wrapper






