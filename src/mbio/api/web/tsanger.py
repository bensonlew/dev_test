# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log
import random
import hashlib
import urllib
import time
import os
import json
import re


class Tsanger(Log):

    def __init__(self, data):
        super(Tsanger, self).__init__(data)
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://www.tsanger.com/api/add_task_log"
        parsered_data = self.parse_data(self.post_data)
        self._post_data = "%s&%s" % (self.get_sig(), parsered_data)

    def get_sig(self):
        nonce = str(random.randint(1000, 10000))
        timestamp = str(int(time.time()))
        x_list = [self._key, timestamp, nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        sig = sha1.hexdigest()
        signature = {
            "client": self._client,
            "nonce": nonce,
            "timestamp": timestamp,
            "signature": sig
        }
        return urllib.urlencode(signature)

    def parse_data(self, post_data):
        my_content = post_data["content"]
        try:
            my_content = json.loads(my_content)
        except Exception:
            pass
        if "stage" in my_content:
            my_stage = my_content["stage"]
            if my_stage["status"] == "finish":
                my_upload_files = post_data["upload_files"]
                target = my_upload_files[0]["target"]
                files = my_upload_files[0]["files"]
                new_files = list()
                new_dirs = list()
                for my_file in files:
                    if my_file["type"] == "file":
                        tmp_dict = dict()
                        tmp_dict["path"] = os.path.join(target, my_file["path"])
                        tmp_dict["size"] = my_file["size"]
                        tmp_dict["description"] = my_file["description"]
                        tmp_dict["format"] = my_file["format"]
                        new_files.append(tmp_dict)
                    elif my_file["type"] == "dir":
                        tmp_dict = dict()
                        tmpPath = re.sub("\.$", "", my_file["path"])
                        tmp_dict["path"] = os.path.join(target, tmpPath)
                        tmp_dict["size"] = my_file["size"]
                        tmp_dict["description"] = my_file["description"]
                        tmp_dict["format"] = my_file["format"]
                        new_dirs.append(tmp_dict)
                my_stage["files"] = new_files
                my_stage["dirs"] = new_dirs
                new_content = dict()
                new_content["stage"] = my_stage
                my_data = dict()
                my_data["content"] = json.dumps(new_content)
                return urllib.urlencode(my_data)
            else:
                my_content = json.dumps(my_content)
                post_data["content"] = my_content
                return urllib.urlencode(post_data)
        else:
            my_content = json.dumps(my_content)
            post_data["content"] = my_content
            return urllib.urlencode(post_data)

    @property
    def post_data(self):
        data = json.loads(self.data.data)
        return data
