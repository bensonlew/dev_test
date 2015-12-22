# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .remote import RemoteFile
from biocluster.config import Config
import os
import shutil


class NETSHARE(RemoteFile):
    def __init__(self, type_name, path):
        super(NETSHARE, self).__init__(type_name, path)
        self.config = Config().get_netdata_config(type_name)
        self._full_path = os.path.join(self.config[type_name + "_path"], path)

    def download(self, to_path):
        """
        将远程文件或文件夹下载到本地

        :param to_path: 将远程文件下载到本地的路径
        :return:
        """
        if not os.path.exists(self._full_path):
            raise Exception("文件路径%s不存存在，请确认网络数据连接正常" % self._full_path)
        if os.path.isfile(self._full_path):
            if os.path.isfile(to_path):
                    os.remove(to_path)
                    shutil.copy(self._full_path, to_path)
                    return to_path
            if not os.path.exists(to_path):
                os.makedirs(to_path)
            if os.path.isdir(to_path):
                shutil.copy(self._full_path, to_path)
                return os.path.join(to_path, os.path.basename(self._full_path))
        if os.path.isdir(self._full_path):
            if os.path.isfile(to_path):
                raise Exception("目标路径%s是文件，但是源路径%s是文件夹，无法复制!" % (to_path, self._full_path))
            basename = os.path.basename(self._full_path)
            target = os.path.join(to_path, basename)
            if os.path.exists(target):
                shutil.rmtree(target)
            shutil.copytree(self._full_path, target)
            return target

    def upload(self, from_path):
        if not os.path.exists(from_path):
            raise Exception("源文件%s不存存在" % from_path)
        if not os.path.exists(self._full_path):
            os.makedirs(self._full_path)
        if os.path.isfile(from_path):
            shutil.copy(from_path, self._full_path)
            return os.path.join(self._full_path, os.path.basename(from_path))
        if os.path.isdir(from_path):
            basename = os.path.basename(from_path)
            target = os.path.join(self._full_path, basename)
            if os.path.exists(target):
                shutil.rmtree(target)
            shutil.copytree(from_path, target)
            return target
