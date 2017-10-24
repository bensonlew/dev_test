## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
#last_modify:20171012
"""
用于将本地的文件夹的路径发送给前端，然后可以在页面上面看到
example: python post_data_new.py -i 'tsanger:rerewrweset/files/m_5950/test01/170922_TPNB500180_0107_AHMV2WAFXX'
-c "QBHASV|c6dce8a78955424c2af0a84c3eed8f60" -m 'tsanger'
"""
import re
import random
import time
import hashlib
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
import socket
import json
import urllib
import urllib2
import argparse
import os

parser = argparse.ArgumentParser(description="根据验证码，上传一个文件夹至某一个项目下，仅供医学使用！")
parser.add_argument("-i", "--base_path", help="输入的文件的基础路径", required=True)
parser.add_argument("-c", "--code", help="输入上传文件的验证码",required=True)
parser.add_argument("-m", "--mode", help="输入是sanger还是tsanger",required=True)
args = vars(parser.parse_args())
path = ['/mnt/ilustre/upload/nextseq1', '/mnt/ilustre/upload/nextseq']
for m in path:
    datas = os.listdir(m)
    if str(args['base_path']) in datas:
        if os.path.isfile(m + '/' + args['base_path'] + '/RTAComplete.txt'):
            break
        else:
            raise Exception("板子还没有下机完成，请检查{}下面的RTAComplete.txt是否完成！".format(args['base_path']))
mode = args['mode']
if mode == "tsanger":
    client = "client03"
    client_key = "hM4uZcGs9d"
    url = 'http://api.tsanger.com/file/add_file_by_code'
    base_path = 'tsanger:rerewrweset/files/m_5950/hongdong/' + args['base_path']
elif mode == "sanger":
    client = "client01"
    client_key = "1ZYw71APsQ"
    url = 'http://api.sanger.com/file/add_file_by_code'
    base_path = 'sanger:rerewrweset/files/m_5950/hongdong/' + args['base_path']

post_data = dict()
post_data['param'] = {'code': args['code'], 'type': 'upload'}
# base_path:tsanger:rerewrweset/files/m_5950/test01/170922_TPNB500180_0107_AHMV2WAFXX
# sanger:rerewrweset/files/m_5950/170928_TPNB500180_0112_AHMWJKAFXX
post_data['base_path'] = base_path
post_data['files'] = list()
post_data['dirs'] = list()
post_data['files'].append({'path': 'RTAComplete.txt', 'format': '', 'lock': '0', 'size': '0', "description": ''})
my_data = dict()
# my_data["data"]["files"].append({'path': 'sanger:rerewrweset/files/m_5950/170928_TPNB500180_0112_AHMWJKAFXX/RTAComplete.txt', 'size': '', 'locked': 'False',
#                                  'description': '', 'format': ''})
# my_data["data"]["dirs"].append({"path": 'sanger:rerewrweset/files/m_5950/170928_TPNB500180_0112_AHMWJKAFXX', 'size': '',
#                                 'locked': '', 'description': '', 'format': ''})

my_data["client"] = client
my_data["nonce"] = str(random.randint(1000, 10000))
my_data["timestamp"] = str(int(time.time()))
x_list = [client_key, my_data["timestamp"], my_data["nonce"]]
x_list.sort()
sha1 = hashlib.sha1()
map(sha1.update, x_list)
my_data["signature"] = sha1.hexdigest()
my_data['sync_files'] = json.dumps(post_data)
print my_data
print url
# my_data = json.dumps(my_data)
request = urllib2.Request(url, urllib.urlencode(my_data))
print "与sanger网站通信， 将上传结果传递至sanger网站上"
try:
    response = urllib2.urlopen(request)
except urllib2.HTTPError as e:
    print e
    raise Exception(e)
else:
    the_page = response.read()
    print "Return Page:", the_page
    my_return = json.loads(the_page)
    if my_return["success"] in ["true", "True", True]:
        print "文件上传已经全部结束！"
    else:
        raise Exception("文件信息写入数据库失败：{}".format(my_return["message"]))