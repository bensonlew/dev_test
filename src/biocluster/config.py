# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""系统配置，获取ip端口、日志、数据库"""

import ConfigParser
import socket
import random
import os
from .core.singleton import singleton
import struct
import platform
import re
import importlib
import web

#web.config.debug = False


@singleton
class Config(object):
    def __init__(self):
        self.rcf = ConfigParser.RawConfigParser()
        self.rcf.read(os.path.dirname(os.path.realpath(__file__))+"/main.conf")
        # basic
        self.WORK_DIR = self.rcf.get("Basic", "work_dir")
        # network
        self.LISTEN_IP = self.get_listen_ip()
        self.LISTEN_PORT = self.get_listen_port()
        # tool
        self.KEEP_ALIVE_TIME = int(self.rcf.get("Tool", "keep_alive_time"))
        self.MAX_KEEP_ALIVE_TIME = int(self.rcf.get("Tool", "max_keep_alive_time"))
        self.MAX_WAIT_TIME = int(self.rcf.get("Tool", "max_wait_time"))
        # log
        self.LOG_LEVEL = self.rcf.get("Log", "level")
        # self.LOG_DIR = self.rcf.get("Log", "log_dir")
        streem_on = self.rcf.get("Log", "stream")
        self.LOG_STREEM = True if streem_on and streem_on.lower() == "on" else False
        self.LOG_FORMAT = self.rcf.get("Log", "format")
        # command
        # self.STDOUT_DIR = self.rcf.get("Command", "stdout_dir")
        self.SOFTWARE_DIR = self.rcf.get("Command", "software_dir")
        # record = self.rcf.get("Resource", "record")
        # self.RECORD_RESOURCE_USE = True if record and record.lower() == "on" else False

        # job
        self.JOB_PLATFORM = self.rcf.get("Job", "platform")
        self.MAX_JOB_NUMBER = int(self.rcf.get("Job", 'max_job_number'))
        self.MAX_WORKFLOW_NUMBER = int(self.rcf.get("Job", 'max_workflow_number'))
        self.JOB_MASTER_IP = self.rcf.get(self.JOB_PLATFORM, "master_ip")

        #db
        self.DB_TYPE = self.rcf.get("DB", "dbtype")
        self.DB_HOST = self.rcf.get("DB", "host")
        self.DB_USER = self.rcf.get("DB", "user")
        self.DB_PASSWD = self.rcf.get("DB", "passwd")
        self.DB_NAME = self.rcf.get("DB", "db")
        self.DB_PORT = self.rcf.get("DB", "port")

        # service mode
        self.USE_DB = False  # daemon mode
        self.SERVICE_LOG = self.rcf.get("SERVICE", "log")
        self.SERVICE_LOOP = int(self.rcf.get("SERVICE", "loop"))
        self.SERVICE_PROCESSES = int(self.rcf.get("SERVICE", "processes"))
        self.SERVICE_PID = int(self.rcf.get("SERVICE", "pid"))

    def get_listen_ip(self):
        """
        获取配置文件中IP列表与本机匹配的IP作为本机监听地址
        """
        def getip(ethname):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            fcn = importlib.import_module("fcntl")
            return socket.inet_ntoa(fcn.ioctl(s.fileno(), 0X8915, struct.pack("256s", ethname[:15]))[20:24])
        if 'Windows' in platform.system():
            local_ip_list = socket.gethostbyname_ex(socket.gethostname())
            set_ipl_ist = self.rcf.get("Network", "ip_list")
            ip_list = re.split('\s*,\s*', set_ipl_ist)
            for lip in local_ip_list[2]:
                for sip in ip_list:
                    if lip == sip:
                        return lip
            return '127.0.0.1'
        if 'Linux' in platform.system():
            return getip("ib0")

    def get_listen_port(self):
        # writer = 'yuguo'
        """
        获取配置文件中start port，end_port ,
        在其之间随机生成一个端口，返回一个未被占用的端口
        """
        start_port = self.rcf.get("Network", "start_port")
        end_port = self.rcf.get("Network", "end_port")
        ip = self.LISTEN_IP
        lpt = random.randint(int(start_port), int(end_port)+1)
        # nm = nmap.PortScanner()
        # while 1:
        #     x = random.randint(int(start_port), int(end_port)+1)
        #     # 使用nmap检测端口状态
        #     nm.scan(ip, str(x))
        #     s = nm[ip]['tcp'][x]['state']
        #     if s == 'closed':
        #         return x
        #         break
        # while 1:
        #     try:
        #         ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #         ss.connect((ip, int(lpt)))
        #         ss.shutdown(2)
        #         # lpt is opened
        #         ss.close()
        #     except:
        #         # lpt is down
        #         return lpt
        #         break

        try:
            ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ss.connect((ip, int(lpt)))
            ss.shutdown(2)
            ss.close()
        except:
            return lpt
        else:
            return self.get_listen_port()

    def get_db(self):
        if self.DB_TYPE == "mysql":
            return web.database(dbn=self.DB_TYPE, host=self.DB_HOST, db=self.DB_NAME, user=self.DB_USER,
                                passwd=self.DB_PASSWD, port=int(self.DB_PORT))
