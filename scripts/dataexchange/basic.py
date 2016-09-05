# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import getpass
import socket
from logger import Wlog


class Basic(object):
    def __init__(self, identity, target_path, mode, port, stream_on):
        self._identity = identity
        self._target_path = target_path
        self._stream_on = stream_on
        self._port = port
        self._mode = mode
        self._ip = ""
        self._user = ""
        self._url = self.get_url(mode, port)
        self.get_ip()
        self.get_user()
        self._file_list = list()
        self.logger = Wlog(self).get_logger("")

    @property
    def user(self):
        return self._user

    @property
    def url(self):
        return self._url

    @property
    def ip(self):
        return self._ip

    @property
    def identity(self):
        return self._identity

    @property
    def target_path(self):
        return self._target_path

    @property
    def stream_on(self):
        return self._stream_on

    @property
    def port(self):
        return self._port

    @property
    def mode(self):
        return self._mode

    def get_ip(self):
        """
        获取客户端的ip
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("baidu.com", 80))
        self._ip = s.getsockname()[0]
        s.close()
        return self._ip

    def get_user(self):
        self._user = getpass.getuser()
        return self._user

    def get_url(self, mode, port):
        """
        获取服务器端的ip，需要在子类中进行重写
        """
        pass
