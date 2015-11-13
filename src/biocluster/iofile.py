# encoding: utf-8
# __author__ = 'yuguo'

"""文件基本模块"""

import os
import hashlib
from .core.exceptions import FileError


class FileBase(object):
    """
    文件对象基类模块
    """

    def __init__(self):
        """
        """
        self._properties = {}
        self._is_set = False

    @property
    def prop(self):
        """
        返回文件对象所有属性，dict字典 key是属性名
        """
        return self._properties

    @property
    def is_set(self):
        """
        返回文件对象是否被设置过路径
        :return:
        """
        return self._is_set

    def set_property(self, name, value):
        """
        添加文件对象属性

        :param name: string 属性名称
        :param value: 属性值
        """
        self._properties[name] = value
        return self

    def get_info(self):
        """
        获取当前文件对象的所有属性,需在扩展子类中重写此方法
        """

    def set_path(self, path):
        """
        设置当前文件对象的路径，并获取所有属性

        :param path: 路径
        """
        self.set_property("path", path)
        self.get_info()
        self._is_set = True

    def check(self):
        """
        检测当前文件对象是否满足运行需求,需在扩展子类中重写
        """
        if not self.is_set:
            raise Exception("请先设置文件路径!")


class File(FileBase):
    """
    单个文件对象
    """
    def __init__(self):
        super(File, self).__init__()

    def get_info(self):
        """
        获取当前文件所有信息

        :return:
        """
        if 'path' in self.prop.keys() and os.path.isfile(self.prop['path']):
            self.set_property("size", self.get_size())
            self.set_property("md5", self.get_md5())
            self.set_property('dirname', os.path.dirname(self.prop['path']))
            self.set_property('basename', os.path.basename(self.prop['path']))
        else:
            raise FileError("请先设置正确的文件路径!")

    def check(self):
        """
        检测文件是否正常,并可用于后续分析

        :return:
        """
        super(File, self).check()
        if 'path' in self.prop.keys() and os.path.isfile(self.prop['path']):
            if self.prop['size'] > 0:
                return True
            else:
                raise FileError("文件大小为空")
                # return {'pass': False, "info": "文件大小为空!"}
        else:
            # return {'pass': False, "info": "!"}
            raise FileError("文件路径不在正确或文件不存在")

    def get_md5(self):
        """
        获取文件MD5值
        """
        md5 = hashlib.md5()
        with open(self.prop['path'], 'rb') as f:
            while 1:
                data = f.read(8096)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()

    def get_size(self):
        """
        获取文件大小
        """
        size = os.path.getsize(self.prop['path'])
        return size

    def link(self, link_path=None):
        """
        为当前文件对象创建软连接

        :param link_path:  生成链接的目标文件夹或希望生成的链接文件路径  所在文件夹必须已经存在
        :return: string 生成的链接路径
        """
        if "path" in self.prop.keys() and os.path.isfile(self.prop['path']):
            source_path = os.path.abspath(self.prop['path'])
            file_name = os.path.basename(source_path)
            if link_path:
                if os.path.isdir(link_path):   # 指定目标为文件夹
                    target_path = os.path.join(link_path, file_name)
                    os.symlink(source_path, target_path)
                    return target_path
                else:    # 指定目标为文件
                    os.symlink(source_path, link_path)
                    return link_path
            else:   # 未指定目标 默认链接到当前目录下
                os.symlink(source_path, file_name)
                return os.path.join(os.getcwd(), file_name)


class Directory(FileBase):
    """
    文件夹对象
    """
    def __init__(self):
        super(Directory, self).__init__()

    def check(self):
        """

        :return:
        """
        super(Directory, self).check()
        if not('path' in self.prop.keys() and os.path.isdir(self.prop['path'])):
            raise FileError("文件夹路径不正确，请设置正确的文件夹路径!")

    def get_info(self):
        if not ('path' in self.prop.keys() and os.path.exists(self.prop['path'])):
            raise FileError("文件夹路径不正确或文件夹不存在")
