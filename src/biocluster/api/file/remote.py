# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import re
import importlib
from biocluster.config import Config


class RemoteFileManager(object):
    def __init__(self, remote_path):
        self._remote_path = remote_path
        self._path = None
        self._type = None
        self._md5 = None
        self._local_path = None
        self._check_type()
        self.config = Config()

    @property
    def local_path(self):
        """
        下载后的本地地址
        :return:
        """
        return self._local_path

    @property
    def path(self):
        return self._path

    @property
    def type(self):
        return self._type

    @property
    def md5(self):
        return self._md5

    def download(self, to_path):
        """
        将远程文件或文件夹下载到本地

        :return:
        """
        if self._type == "local":
            self._local_path = self._path
            return
        else:
            lib_obj = self._get_lib()
            self._local_path = lib_obj.download(to_path)
            return

    def upload(self, from_path):
        """
        将本地文件或文件夹上传到远程路径

        :param from_path:
        :return:
        """
        if self._type == "local":
            self._local_path = from_path
            return
        else:
            lib_obj = self._get_lib()
            self._local_path = lib_obj.upload(from_path)

    def _check_type(self):
        """
        检测文件来源类型，并找到

        :return:
        """
        m = re.match(r"^http://.*|^https://.*", self._remote_path)
        if m:
            self._type = "http"
            self._path = self._remote_path
            return

        m = re.match(r"^(\w+):/*(.*)$", self._remote_path)
        if m:
            self._type = m.group(1)
            self._path = m.group(2)
            return

        self._type = "local"
        self._path = self._remote_path

    def _get_lib(self):
        lib_name = self.config.get_netdata_lib(self._type)
        module = importlib.import_module("biocluster.api.file.%s" % lib_name.lower())
        lib_obj = getattr(module, lib_name.capitalize())(self._type, self._path)
        return lib_obj


class RemoteFile(object):
    def __init__(self, type_name, path):
        self._type = type_name
        self._path = path

    @property
    def type(self):
        return self._type

    @property
    def path(self):
        return self._path

    def download(self, to_path):
        pass

    def upload(self, from_path):
        pass

