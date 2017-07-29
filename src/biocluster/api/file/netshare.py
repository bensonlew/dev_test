# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .remote import RemoteFile
from biocluster.config import Config
import time
import os
import json
import shutil
import errno
import re


class Netshare(RemoteFile):
    def __init__(self, type_name, path):
        super(Netshare, self).__init__(type_name, path)
        self.config = Config().get_netdata_config(type_name)
        self.type_name = type_name
        self._file_list = None
        tmp = re.split(";;", path)
        if len(tmp) > 1:
            # m = re.sub("u'", "'", tmp[1])
            # m = tmp[1].decode("unicode_escape")  # modified by sj on 2016.11.17
            # self._fileList = eval(m)
            self._file_list = json.loads(tmp[1])
        self._full_path = os.path.join(self.config[type_name + "_path"], tmp[0])

    def download(self, to_path):
        """
        将远程文件或文件夹下载到本地

        :param to_path: 将远程文件下载到本地的路径
        :return:
        """
        # base_name = os.path.basename(self._full_path)
        # target_path = os.path.join(to_path, base_name)

        if not os.path.exists(self._full_path):
            raise Exception("文件路径%s不存存在，请确认网络数据连接正常" % self._full_path)
        if os.path.isfile(to_path):
            os.remove(to_path)
        if not os.path.exists(to_path):
            os.makedirs(to_path)
        if self._file_list:
            for my_dict in self._file_list:
                source = os.path.join(self._full_path, my_dict["file_path"])  # modified by sj on 2016.10.26
                if not os.path.exists(source):
                    raise Exception("文件{}不存在".format(source))
                if os.path.isdir(source):
                    target = os.path.join(os.path.join(to_path, my_dict["alias"]), os.path.basename(source))
                    if os.path.exists(target):
                        if os.path.islink(target):
                            os.remove(target)
                        elif os.path.isdir(target):
                            shutil.rmtree(target)
                        else:
                            os.remove(target)
                    shutil.copytree(source, target, symlinks=True)
                else:
                    target = os.path.join(to_path, my_dict["alias"])
                    if not os.path.exists(target):
                        os.mkdir(target)
                    shutil.copy(source, target)
                return to_path
        else:
            target_path = os.path.join(to_path, os.path.basename(self._full_path))
            if os.path.exists(target_path):
                if os.path.islink(target_path):
                    os.remove(target_path)
                elif os.path.isdir(target_path):
                    shutil.rmtree(target_path)
                else:
                    os.remove(target_path)
            if os.path.isdir(self._full_path):
                shutil.copytree(self._full_path, target_path, symlinks=False)
            else:
                shutil.copy(self._full_path, to_path)
            return target_path

    def upload(self, from_path):
        if not os.path.exists(from_path):
            raise Exception("源文件%s不存存在" % from_path)
        basename = os.path.basename(from_path)
        target = os.path.join(self._full_path, basename)
        if os.path.exists(target):
            if os.path.islink(target):
                os.remove(target)
            elif os.path.isdir(target):
                shutil.rmtree(target)
            else:
                os.remove(target)
        if not os.path.exists(self._full_path):
            os.makedirs(self._full_path)
        if os.path.isfile(from_path):
            shutil.copy(from_path, self._full_path)
            return target
        if os.path.isdir(from_path):
            shutil.copytree(from_path, target, symlinks=False)
            return target

    # def _debug(self, str_):
    #     with open("/mnt/ilustre/users/sanger/sgBioinfo/xuting/webapi/netshare_debug.txt", 'ab') as a:
    #         a.write(str_)
    #         a.write("\n")

    # def _copy_dir(self, src, dst, symlinks=False, ignore=None):
    #     if not os.path.exists(dst):
    #         os.makedirs(dst)
    #     for item in os.listdir(src):
    #         s = os.path.join(src, item)
    #         d = os.path.join(dst, item)
    #         if os.path.isdir(s):
    #             self._copy_dir(s, d, symlinks, ignore)
    #         else:
    #             shutil.copy2(s, d)
