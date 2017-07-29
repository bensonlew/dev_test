# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import urllib
from .remote import RemoteFile
import re
import os


class Http(RemoteFile):
    def __init__(self, type_name, path):
        super(Http, self).__init__(type_name, path)

    def download(self, to_path):
        m = re.match(r"https?://.*/([^/^\?]+)/?\??.*$", self._path)
        basename = m.group(1)
        if os.path.isfile(to_path):
            os.remove(to_path)
        if not os.path.exists(to_path):
            os.makedirs(to_path)
        local_path = os.path.join(to_path, basename)
        urllib.urlretrieve(self._path, local_path)
        return local_path

    def upload(self, from_path):
        raise Exception("HTTP不支持上传！")