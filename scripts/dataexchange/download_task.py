# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import urllib2
import urllib
import httplib
import sys
import json
import os
import errno
from basic import Basic


class DownloadTask(Basic):
    """
    用于内部下载一个任务的所有文件
    last_modified: 20160928
    """
    def __init__(self, identity, target_path, mode, port, stream_on):
        super(DownloadTask, self).__init__(identity, target_path, mode, port, stream_on)
        self._download_url = self.get_download_url(mode)
        self._file_list = list()

    @property
    def download_url(self):
        return self._download_url

    def get_url(self, mode, port):
        if mode == "sanger":
            self._url = "http://192.168.12.101:{}/app/dataexchange/download_task".format(port)
        elif mode == "tsanger":
            self._url = "http://192.168.12.102:{}/dataexchange/download_task".format(port)
        return self._url

    def get_download_url(self, mode):
        if mode == "sanger":
            self._download_url = "http://192.168.12.101/download.php"
        if mode == "tsanger":
            self._download_url = "http://192.168.12.102/download.php"
        return self._download_url

    def get_task_info(self):
        """
        获取编码相对应的task_id, 以及相关的目录结构
        """
        data = urllib.urlencode({"ip": self.ip, "identity": self.identity, "user": self.user, "mode": self.mode})
        req = urllib2.Request(self.url, data)
        try:
            self.logger.info("用户: {}, 验证码: {}".format(self.user, self.identity))
            self.logger.info("正在与远程主机通信，获取项目信息")
            response = urllib2.urlopen(req)
        except (urllib2.HTTPError, urllib2.URLError, httplib.HTTPException) as e:
            self.logger.info(e)
            if self._port != "2333":
                try:
                    self.logger.info("尝试使用2333端口重新进行连接")
                    self.get_url(self.mode, "2333")
                    req = urllib2.Request(self.url, data)
                    response = urllib2.urlopen(req)
                except (urllib2.HTTPError, urllib2.URLError, httplib.HTTPException) as e:
                    self.logger.info(e)
                    sys.exit(1)
                else:
                    info = response.read()
            else:
                sys.exit(1)
        else:
            info = response.read()

        info = json.loads(info)
        if not info["success"]:
            self.logger.info(info["info"])
            sys.exit(1)
        else:
            self.logger.info("通信成功，任务id为：{}。开始下载文件...".format(info["task_id"]))
            self._file_list = info["data"]
            print info["data"]

    def download_files(self):
        """
        遍历文件列表， 并下载文件
        """
        total_sum = len(self._file_list)
        count = 1
        for f_info in self._file_list:
            file_name = os.path.basename(f_info[0])
            dir_name = os.path.dirname(f_info[2])
            local_dir = os.path.join(self.target_path, dir_name)
            local_file = os.path.join(local_dir, file_name)
            self.logger.info("正在下载第 {}/{} 个文件: {}, 文件大小{}".format(count, total_sum, file_name, f_info[1]))
            count += 1
            post_info = urllib.urlencode({'indentity_code': self.identity, 'file': f_info[0], 'mode': self.mode})
            request = urllib2.Request(self.download_url, post_info)
            try:
                u = urllib2.urlopen(request)
                try:
                    os.makedirs(local_dir)
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise exc
                    else:
                        pass
            except (urllib2.HTTPError, urllib2.URLError, httplib.HTTPException) as e:
                self.logger.info(e)
                continue
            meta = u.info()
            file_size = int(meta.getheaders("Content-Length")[0])
            file_size_dl = 0
            block_sz = 51200
            f = open(local_file, "wb")
            while True:
                buffer = u.read(block_sz)
                if not buffer:
                    break

                file_size_dl += len(buffer)
                f.write(buffer)
                status = "{:10d}  [{:.2f}%]".format(file_size_dl, file_size_dl * 100 / file_size)
                self.logger.info(status)
            f.close()
