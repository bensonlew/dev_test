# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .remote import RemoteFile
from biocluster.config import Config
import time
import os
import re
import json
import shutil
import errno


class Netshare(RemoteFile):
    def __init__(self, type_name, path):
        super(Netshare, self).__init__(type_name, path)
        self.config = Config().get_netdata_config(type_name)
        self.type_name = type_name
        self.fileType = ""
        tmp = re.split(";;", path)
        if len(tmp) > 1:
            self.fileType = "dir"
            self._fileList = json.loads(tmp[1])
        else:
            self.fileType = "file"
        self._full_path = os.path.join(self.config[type_name + "_path"], tmp[0])

    def download(self, to_path):
        """
        将远程文件或文件夹下载到本地

        :param to_path: 将远程文件下载到本地的路径
        :return:
        """
        if self.fileType == "file":
            if not os.path.exists(self._full_path):
                raise Exception("文件路径%s不存存在，请确认网络数据连接正常" % self._full_path)
            if os.path.isfile(to_path):
                    os.remove(to_path)
                    shutil.copy(self._full_path, to_path)
                    return to_path
            if not os.path.exists(to_path):
                os.makedirs(to_path)
            if os.path.isdir(to_path):
                shutil.copy(self._full_path, to_path)
                return os.path.join(to_path, os.path.basename(self._full_path))
        if self.fileType == "dir":
            if os.path.isfile(to_path):
                raise Exception("目标路径%s是文件，但是源路径%s是文件夹，无法复制!" % (to_path, self._full_path))
            try:
                os.makedirs(to_path)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(to_path):
                    pass
                else:
                    raise OSError("创建目录{}失败".format(to_path))

            for myDict in self._fileList:
                source = os.path.join(self.config[self.type_name + "_path"], "rerewrweset", myDict["file_path"])
                target = os.path.join(to_path, myDict["alias"])
                if not os.path.exists(source):
                    raise Exception("文件{}不存在".format(source))
                shutil.copy(source, target)
            return to_path

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
                flag = 1
                while flag:
                    try:
                        shutil.rmtree(target)
                    except Exception:
                        time.sleep(2)
                    else:
                        flag = 0
            # shutil.copytree(from_path, target)
            self._copy_dir(from_path, target)
            return target

    # def _debug(self, str_):
    #     with open("/mnt/ilustre/users/sanger/sgBioinfo/xuting/webapi/netshare_debug.txt", 'ab') as a:
    #         a.write(str_)
    #         a.write("\n")

    def _copy_dir(self, src, dst, symlinks=False, ignore=None):
        if not os.path.exists(dst):
            os.makedirs(dst)
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                self._copy_dir(s, d, symlinks, ignore)
            else:
                shutil.copy2(s, d)
