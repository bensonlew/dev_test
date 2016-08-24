# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
from basic import Basic


class UploadTask(Basic):
    def __init__(self, identity, target_path, mode, port, stream_on):
        super(UploadTask, self).__init__(identity, target_path, mode, port, stream_on)
        self._upload_url = self.get_upload_url(mode)
        self._file_list = list()

    @property
    def upload_url(self):
        return self._upload_url

    def get_url(self, mode, port):
        """
        上传因为是由客户端发起， 因此不需要事先与服务器端通信以获取文件结构
        """
        return ""

    def get_upload_url(self, mode):
        if mode == "sanger":
            self._download_url = "http://192.168.12.101/upload.php"
        if mode == "tsanger":
            self._download_url = "http://192.168.12.102/upload.php"
        return self._upload_url
