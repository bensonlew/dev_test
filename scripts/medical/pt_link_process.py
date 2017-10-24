## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
# last_modify:20161125

"""
用于实时监测下机数据是否完成，并实时link到磁盘中
"""
import time
import re
import os
import datetime
# 171018_TPNB500180_0124_AHT2H2AFXX
target = "/mnt/ilustre/data/rerewrweset/files/m_5950/hongdong/"
# new_time_data = "171018"
path = ["/mnt/clustre/upload/nextseq", "/mnt/clustre/upload/nextseq1"]
for i in xrange(10000000000):
    time_data = time.strftime("%Y-%m-%d").strip().split('-')  # 2017-10-20
    new_time_data = time_data[0][-2:] + time_data[1] + time_data[2]
    # new_time_data = "171017"
    for m in path:
        files = os.listdir(m)
        # print files
        for data in files:
            if re.match(r'^{}'.format(new_time_data), data) or re.match(r'^{}'.format(str(eval(new_time_data) - 1)), data):
                file_path = m + "/" + data
                # print file_path
                if not os.path.isdir(target + data):
                    if os.path.isfile(file_path + '/RTAComplete.txt'):
                        os.system('ln -s {} {}'.format(file_path, target + data))
                        print "link成功！"
                    else:
                        print "检测到文件{}，但是还没有下机完成！".format(file_path)
                else:
                    print "检测到文件{}，下机完成并成功连接！！".format(file_path)
    # time.sleep(2)
    now = str(datetime.datetime.now()).strip().split(' ')[1].split(':')[0]
    print now
    if 24 >= int(now) > 12:
        time.sleep(7200)   # 每天下午到24点，两个小时一刷
    elif 1 < int(now) < 8:
        time.sleep(1800)
    elif 8 <= int(now) <= 12:
        time.sleep(600)
    else:
        time.sleep(1801)





