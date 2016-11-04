# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import urllib2
import urllib
import httplib
import sys
import json
import requests
import os
# import pprint
import re
import random
import time
import hashlib
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
from basic import Basic


class FileLimiter(object):
    def __init__(self, file_obj, read_limit):
        self.read_limit = read_limit
        self.amount_seen = 0
        self.file_obj = file_obj
        self.len = read_limit

    def read(self, amount=-1):
        if self.amount_seen >= self.read_limit:
            return b''
        remaining_amount = self.read_limit - self.amount_seen
        data = self.file_obj.read(remaining_amount if amount < 0 else min(amount, remaining_amount))
        self.amount_seen += len(data)
        return data


class UploadTask(Basic):
    def __init__(self, identity, target_path, mode, port, stream_on):
        super(UploadTask, self).__init__(identity, target_path, mode, port, stream_on)
        self._target_path = ""
        self._upload_url = self.get_upload_url(mode)  # 文件上传的php接口的地址
        self._file_info = list()
        self.source_path = ""
        self.no_prifix_path = ""
        self.post_url = ""  # 与前端通信的的接口的地址，用于告诉前端文件已经上传， 以及具体上传的了何处

    @property
    def upload_url(self):
        return self._upload_url

    def get_url(self, mode, port):
        if mode == "sanger":
            self._url = "http://192.168.12.101:{}/app/dataexchange/upload_task".format(port)
        elif mode == "tsanger":
            self._url = "http://192.168.12.102:{}/app/dataexchange/upload_task".format(port)
        return self._url  # 前置的接口地址，用于验证和获取上传文件的路径

    def get_upload_url(self, mode):
        if mode == "sanger":
            self._upload_url = "http://192.168.12.101/upload.php"
        if mode == "tsanger":
            self._upload_url = "http://192.168.12.102/upload.php"
        return self._upload_url  # 接受文件的接口地址，php

    def get_post_url(self, mode):
        if self.post_url != "":
            return self.post_url
        if mode == "sanger":
            self.post_url = "http://www.sanger.com/api/add_file_dir"
        if mode == "tsanger":
            self.post_url = "http://www.tsanger.com/api/add_file_dir"
        return self.post_url

    def get_task_info(self):
        """
        验证验证码， 获取上传的文件路径
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
            self.logger.info("通信成功，开始上传文件...")
            self._target_path = info["abs_path"]    # 经过config转化的绝对路径
            self.no_prifix_path = info["rel_path"]  # 即是数据库里的rel_dir的值
            return json.dumps(info)

    def get_file_info(self, path):
        with open(path, "rb") as r:
            line = r.next().rstrip()
            if not re.search("#source#", line):
                raise ValueError("输入的列表文件的格式错误！")
            else:
                self.source_path = re.sub("#source#", "", line)
            line = r.next()
            for line in r:
                line = line.rstrip().split("\t")
                info_dict = dict()
                info_dict["path"] = line[0]
                info_dict["size"] = line[1]
                info_dict["description"] = line[2]
                info_dict["locked"] = line[3]
                self._file_info.append(info_dict)
        return self._file_info

    def my_callback(self, monitor):
        upload_bite = monitor.bytes_read
        my_m = (upload_bite / 1024) / 1024
        print "已经上传{0:.2f}M".format(my_m)

    def upload_files(self):
        """
        上传文件
        """
        if len(self._file_info) == 0:
            raise ValueError("没有需要上传的文件")
        # multiple_files= [('sources', ("aa.txt", open(my_f, "rb"), "txt/plain")),
        #                 ("sources", ("bbb.txt", open(my_f2, "rb"), "txt/plain"))
        #    ]
        # 获取上传目标的文件夹名称， 为拼凑上传的目标路径做准备
        tmp_list = re.split("/", self.source_path)
        length = len(tmp_list)
        for i in range(length):
            soure_dir = tmp_list.pop()
            if soure_dir == "":
                continue
            else:
                break

        for d in self._file_info:
            # pprint.pprint(d)
            rel_path = re.sub(self.source_path, "", d["path"]).lstrip("/")
            rel_path = os.path.join(soure_dir, rel_path).lstrip("/")
            d["rel_path"] = rel_path
            full_path = os.path.join(self.target_path, rel_path)
            # psot_json = {"target_path": full_path, "mode": self.mode, "code": self.identity, "rel_dir": self.no_prifix_path}

            # my_file = {'sources': (os.path.basename(d["path"]), open(d["path"], "rb"), 'application/octet-stream'),
            #           'target': (None, json.dumps(psot_json), 'application/json')}

            m = MultipartEncoder(
                fields={'sources': (os.path.basename(d["path"]), open(d["path"], "rb"), 'application/octet-stream'),
                        "target_path": full_path, "mode": self.mode, "code": self.identity, "rel_dir": self.no_prifix_path})
            m = MultipartEncoderMonitor(m, self.my_callback)
            d["target_path"] = full_path
            if self.mode == "tsanger":
                prefix = "tsanger:"
            elif self.mode == "sanger":
                prefix = "sanger:"
            d["submit_path"] = prefix + os.path.join(self.no_prifix_path, rel_path)  # 上传到硬盘的哪个位置
            self.logger.info("开始上传文件{}".format(d["path"]))
            # with open(d["path"], 'rb') as file_obj:
            #    upload = FileLimiter(file_obj, 40960)
            #    r = requests.post(self._upload_url, data=upload, headers={'Content-Type': 'application/octet-stream'}, json=psot_json)
            r = requests.post(self._upload_url, data=m, headers={'Content-Type': m.content_type})
            # print r.text
            print d["submit_path"]
            if r.status_code == 200:
                self.logger.info("文件{}上传完成".format(d["path"]))
            else:
                print r.text
        self.post_data()

    def post_data(self):
        my_data = dict()
        my_data["data"] = dict()
        my_data["data"]["files"] = list()
        for d in self._file_info:
            my_data["data"]["files"].append({"path": d["submit_path"], "format": "", "description": d["description"], "locked": d["locked"], "size": d["size"]})
        my_data["data"]["dirs"] = self._get_dir_struct()
        if self.mode == "tsanger":
            client = "client03"
            client_key = "hM4uZcGs9d"
        elif self.mode == "sanger":
            client = "client01"
            client_key = "1ZYw71APsQ"
        my_data["client"] = client
        my_data["nonce"] = str(random.randint(1000, 10000))
        my_data["timestamp"] = str(int(time.time()))
        x_list = [client_key, my_data["timestamp"], my_data["nonce"]]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        my_data["signature"] = sha1.hexdigest()
        # pprint.pprint(my_data)
        my_data = json.dumps(my_data)
        request = urllib2.Request(self.get_post_url(self.mode), my_data)
        self.logger.info("与sanger网站通信， 将上传结果传递至sanger网站上")
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            self.logger.error(e)
            raise Exception(e)
        else:
            the_page = response.read()
            self.logger.info("Return Page:")
            self.logger.info(the_page)
            my_return = json.loads(the_page)
            if my_return["success"] in ["true", "True", True]:
                self.logger.info("文件上传已经全部结束！")
            else:
                raise Exception("文件信息写入数据库失败：{}".format(my_return["message"]))

    def _get_dir_struct(self):
        my_dict = list()
        dir_list = list()
        for d in self._file_info:
            dir_list.append(os.path.dirname(d["rel_path"]))
        dir_list = list(set(dir_list))
        if self.mode == "tsanger":
            prefix = "tsanger:"
        elif self.mode == "sanger":
            prefix = "sanger:"
        for l in dir_list:
            my_dict.append({"path": prefix + os.path.join(self.no_prifix_path, l), "format": "", "description": "", "locked": "", "size": ""})
        return my_dict
