# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""系统配置，获取ip端口、日志、数据库"""

import ConfigParser
import socket
import random
import os
from .core.singleton import singleton
# import struct
import platform
import re
import importlib
#import web
# import subprocess
from pymongo import MongoClient
import subprocess
from IPy import IP

# web.config.debug = False


@singleton
class Config(object):
    def __init__(self):
        self.rcf = ConfigParser.RawConfigParser()
        self.rcf.read(os.path.dirname(os.path.realpath(__file__))+"/main.conf")
        # basic
        self.WORK_DIR = self.rcf.get("Basic", "work_dir")
        # network
        self._listen_ip = None
        self._listen_port = None
        # self.LISTEN_PORT = self.get_listen_port()
        # tool
        self.KEEP_ALIVE_TIME = int(self.rcf.get("Tool", "keep_alive_time"))
        self.MAX_KEEP_ALIVE_TIME = int(self.rcf.get("Tool", "max_keep_alive_time"))
        self.MAX_FIRE_KAO_TIMES = int(self.rcf.get("Tool", "max_fire_kao_times"))
        self.MAX_WAIT_TIME = int(self.rcf.get("Tool", "max_wait_time"))
        self.MAX_FIRE_WTO_TIMES = int(self.rcf.get("Tool", "max_fire_wto_times"))
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
        self.JOB_MASTER_IP = self.rcf.get(self.JOB_PLATFORM, "master_ip")
        self.JOB_QUEUE = self.rcf.get(self.JOB_PLATFORM, "queue")

        # db
        self.DB_TYPE = self.rcf.get("DB", "dbtype")
        self.DB_HOST = self.rcf.get("DB", "host")
        self.DB_USER = self.rcf.get("DB", "user")
        self.DB_PASSWD = self.rcf.get("DB", "passwd")
        self.DB_NAME = self.rcf.get("DB", "db")
        self.DB_PORT = self.rcf.get("DB", "port")

        # service mode
        # self.SERVICE_LOG = self.rcf.get("SERVICE", "log")
        # self.SERVICE_LOOP = int(self.rcf.get("SERVICE", "loop"))
        # self.SERVICE_PROCESSES = int(self.rcf.get("SERVICE", "processes"))
        # self.SERVICE_PID = self.rcf.get("SERVICE", "pid")

        # SSH
        self.SSH_DEFAULT_IP = self.rcf.get("SSH", "default_ip")
        # SSH1
        self.SSH1_MODE = self.rcf.get("SSH1", "mode")
        self.SSH1_IP_LIST = re.split('\s*,\s*', self.rcf.get("SSH1", "ip_list"))

        # PAUSE
        self.MAX_PAUSE_TIME = int(self.rcf.get("PAUSE", "max_time"))

        # API_UPDATE
        self.update_exclude_api = re.split('\s*,\s*', self.rcf.get("API_UPDATE", "exclude_api"))
        self.UPDATE_MAX_RETRY = int(self.rcf.get("API_UPDATE", "max_retry"))
        self.UPDATE_RETRY_INTERVAL = int(self.rcf.get("API_UPDATE", "retry_interval"))
        self.UPDATE_LOG = self.rcf.get("API_UPDATE", "log")
        # self.UPLOAD_LOG = self.rcf.get("API_UPDATE", "upload_log")

        # Mongo
        self.MONGO_URI = self.rcf.get("MONGO", "uri")
        self.MONGO_BIO_URI = self.rcf.get("MONGO", "bio_uri")
        self._mongo_client = None
        self.MONGODB = self.rcf.get("MONGO", "mongodb")

        # mysql
        self._db_client = None

        # WPM
        listen_data = re.split(":", self.rcf.get("WPM", "listen"))
        self.wpm_listen = (listen_data[0], int(listen_data[1]))
        self.wpm_authkey = self.rcf.get("WPM", "authkey")
        self.wpm_user = self.rcf.get("WPM", "user")
        log_listen_data = re.split(":", self.rcf.get("WPM", "logger_listen"))
        self.wpm_logger_listen = (log_listen_data[0], int(log_listen_data[1]))
        self.wpm_logger_authkey = self.rcf.get("WPM", "logger_authkey")
        self.wpm_log_file = self.rcf.get("WPM", "log_file")
        self.wpm_instant_timeout = int(self.rcf.get("WPM", "instant_timeout"))
        self.wpm_pid_dir = self.rcf.get("WPM", "pid_dir")

    @property
    def mongo_client(self):
        if not self._mongo_client:
            self._mongo_client = MongoClient(self.MONGO_URI)
        return self._mongo_client

    @property
    def LISTEN_IP(self):
        if self._listen_ip is None:
            self._listen_ip = self.get_listen_ip()
        return self._listen_ip

    def get_listen_ip(self):
        """
        获取配置文件中IP列表与本机匹配的IP作为本机监听地址
        """
        # def getip(ethname):
        #     s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #     fcn = importlib.import_module("fcntl")
        #     return socket.inet_ntoa(fcn.ioctl(s.fileno(), 0X8915, struct.pack("256s", ethname[:15]))[20:24])
        # set_ipl_ist = self.rcf.get("Network", "ip_list")
        ip_ranges = self.rcf.get("Network", "ip_range")
        range_list = re.split('\s*,\s*', ip_ranges)
        ip_range_lists = []
        for rg in range_list:
            ip_range_lists.append(IP(rg))
        if 'Windows' in platform.system():
            local_ip_list = socket.gethostbyname_ex(socket.gethostname())
            for lip in local_ip_list[2]:
                for sip in ip_range_lists:
                    if lip in sip:
                        return lip
        if platform.system() == 'Linux' or platform.system() == 'Darwin':
            # return getip("eth1")
            ipstr = '([0-9]{1,3}\.){3}[0-9]{1,3}'
            ipconfig_process = subprocess.Popen("/sbin/ifconfig", stdout=subprocess.PIPE)
            output = ipconfig_process.stdout.read()
            ip_pattern = re.compile('(inet %s)' % ipstr)
            if platform.system() == "Linux":
                ip_pattern = re.compile('(inet addr:%s)' % ipstr)
            pattern = re.compile(ipstr)
            for ipaddr in re.finditer(ip_pattern, str(output)):
                ip = pattern.search(ipaddr.group())
                # print ip.group()
                for sip in ip_range_lists:
                    if ip.group() in sip:
                        return ip.group()
        raise Exception("内网Network网段设置错误,没有找到IP属于网段%s!" % ip_ranges)

    @property
    def LISTEN_PORT(self):
        if self._listen_port:
            return self._listen_port
        else:
            return self.get_listen_port()

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
        if not self._db_client:
            if self.DB_TYPE == "mysql":
                # self._db_client = web.database(dbn=self.DB_TYPE, host=self.DB_HOST,
                # db=self.DB_NAME, user=self.DB_USER, passwd=self.DB_PASSWD, port=int(self.DB_PORT))
                import MySQLdb
                self._db_client = MySQLdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PASSWD,
                                                  db=self.DB_NAME, port=int(self.DB_PORT))
        return self._db_client

    def get_netdata_config(self, type_name):
        type_list = re.split(r"\s*,\s*", self.rcf.get("NETDATA", "types"))
        if type_name not in type_list:
            raise Exception("Unkown netdata %s" % type_name)
        options = self.rcf.options("NETDATA")
        type_dict = {}
        for opt in options:
            if re.match("^" + type_name, opt):
                type_dict[opt] = self.rcf.get("NETDATA", opt)
        return type_dict

    def get_netdata_lib(self, type_name):
        if type_name == "http":
            return "http"
        type_list = re.split(r"\s*,\s*", self.rcf.get("NETDATA", "types"))
        if type_name not in type_list:
            raise Exception("Unkown netdata %s" % type_name)
        return self.rcf.get("NETDATA", "%s_type" % type_name)

    def get_api_type(self, client):
        if self.rcf.has_option("API", client):
            return self.rcf.get("API", client)
        else:
            return None

    def get_use_api_clients(self):
        return self.rcf.options("API")

    @property
    def UPDATE_EXCLUDE_API(self):
        if self._update_exclude_api is not None:
            return self._update_exclude_api
        if self.rcf.has_option("API_UPDATE", "exclude_api"):
            exclude_api = self.rcf.get("API_UPDATE", "exclude_api")
            if exclude_api.strip() != "":
                self._update_exclude_api = re.split(r"\s*,\s*", exclude_api)
                return self._update_exclude_api
            else:
                self._update_exclude_api = []
                return self._update_exclude_api
        else:
            self._update_exclude_api = []
            return self._update_exclude_api
