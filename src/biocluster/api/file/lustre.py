# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .remote import RemoteFile
from biocluster.config import Config
import os
import json
import shutil
import re


class Lustre(RemoteFile):
    def __init__(self, type_name, path):
        super(Lustre, self).__init__(type_name, path)
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
        self._type = type_name

    def download(self, to_path):
        """
        将远程文件或文件夹下载到本地

        :param to_path: 将远程文件下载到本地的路径
        :return:
        """
        # 当为文件夹时，前端传入的文件夹为虚拟文件夹，可能不存在，检查放在只是文件的情况中 if not self._file_list
        # shenghe 20170210
        # if not os.path.exists(self._full_path):
        #     raise Exception("文件路径%s不存存在，请确认网络数据连接正常" % self._full_path)
        if os.path.isfile(to_path):
            os.remove(to_path)
        if not os.path.exists(to_path):
            os.makedirs(to_path)
        if self._file_list:
            for my_dict in self._file_list:
                # 前端传入文件列表的路径不带有 'rerewrweset'前缀 modified by sj on 2016.10.26
                if not re.match("rerewrweset", my_dict["file_path"]):
                    my_dict["file_path"] = "rerewrweset/" + my_dict["file_path"]
                source = os.path.join(self.config[self._type + "_path"], my_dict["file_path"])
                if not os.path.exists(source):
                    raise Exception("文件{}不存在".format(source))
                target = os.path.join(to_path, my_dict["alias"])
                if os.path.exists(target):
                    if os.path.islink(target):
                        os.remove(target)
                    elif os.path.isdir(target):
                        shutil.rmtree(target)
                    else:
                        os.remove(target)

                os.symlink(source, target)
            return to_path
        else:
            if not os.path.exists(self._full_path):
                raise Exception("文件路径%s不存存在，请确认网络数据连接正常" % self._full_path)
            target_path = os.path.join(to_path, os.path.basename(self._full_path))
            if os.path.exists(target_path):
                if os.path.isdir(target_path):
                    os.remove(target_path)
                elif os.path.isdir(target_path):
                    shutil.rmtree(target_path)
                else:
                    os.remove(target_path)
            os.symlink(self._full_path, target_path)
            # shutil.copy(self._full_path, to_path)
            return target_path

    def upload(self, from_path):
        if not os.path.exists(from_path):
            raise Exception("源文件%s不存存在" % from_path)
        # basename = os.path.basename(from_path)
        target = self._full_path  # 目标目录直接为上传目录，意思是不需要上传目录的的目录名  # shenghe 20170322
        if os.path.exists(target):
            if os.path.islink(target):
                os.remove(target)
            elif os.path.isdir(target):
                shutil.rmtree(target)
            else:
                os.remove(target)
        if not os.path.exists(self._full_path):
            os.makedirs(self._full_path)

        if os.path.isdir(from_path):
            for root, dirs, files in os.walk(from_path):
                # rel_path = os.path.relpath(root, os.path.dirname(from_path))
                rel_path = os.path.relpath(root, from_path)  # 去除路径中间相对路径，直接使用文件与上传目录差  shenghe 20170322
                for i_file in files:
                    i_file_path = os.path.join(root, i_file)
                    if os.path.islink(i_file_path):
                        real_path = os.path.realpath(i_file_path)
                        if os.path.exists(real_path):
                            dir_path = os.path.join(self._full_path, rel_path)
                            if not os.path.exists(dir_path):
                                os.makedirs(dir_path)
                            file_path = os.path.join(dir_path, i_file)
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            os.link(real_path, file_path)
                    else:
                        dir_path = os.path.join(self._full_path, rel_path)
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)
                        file_path = os.path.join(dir_path, i_file)
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        os.link(i_file_path, file_path)
        else:
            if os.path.islink(from_path):
                real_path = os.path.realpath(from_path)
                if not os.path.exists(real_path):
                    raise Exception("源文件%s是一个无效的软链接!" % from_path)
                os.link(real_path, os.path.join(self._full_path, os.path.basename(from_path)))
            else:
                os.link(from_path, os.path.join(self._full_path, os.path.basename(from_path)))
        return os.path.join(self._full_path, os.path.basename(from_path))

    # def _read_link(self, path):
    #     if os.path.islink(path):
    #         link_path = os.readlink(path)
    #         if not os.path.isabs(link_path):
    #             link_path = os.path.abspath(os.path.join(os.path.dirname(path), link_path))
    #         self._read_link(link_path)
    #     else:
    #         return os.readlink(path)
