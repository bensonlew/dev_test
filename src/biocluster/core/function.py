# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import importlib
import re
import os
import sys
import json
from datetime import datetime, date
import inspect
import ctypes
import setproctitle
from bson.objectid import ObjectId
from ctypes import Structure, c_char, c_int, addressof, memmove,  POINTER, sizeof, byref
import time
from .exceptions import MaxLengthError


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)


def get_clsname_form_path(path, tp="Agent"):
    """
    从path中获取ClassName

    :param path:  string 模块路径
    :param tp:  string 种类Agent Tool,Module,Workflow
    :return:  string
    """
    path = re.sub(r'[^a-zA-Z0-9\._]', '', path)
    name = path.split(".").pop()
    l = name.split("_")
    l.append(tp)
    l = [el.capitalize() for el in l]
    return "".join(l)


def load_class_by_path(path, tp="Agent"):
    """
    根据Path找到对应的类，并返回类对象

    :param path: string 模块路径
    :param tp:  string 种类Agent Tool,Module,Workflow
    :return: 对应的class对象
    """
    dir_name = {
        "Agent": "mbio.tools.",
        "Tool": "mbio.tools.",
        "Module": "mbio.modules.",
        "Workflow": "mbio.workflows.",
        "Package": "mbio.pacakages.",
        "File": "mbio.files."
    }
    class_name = get_clsname_form_path(path, tp)
    module_name = dir_name[tp] + path
    # try:
    #    sys.modules[module_name]
    # except KeyError:
    if module_name in sys.modules.keys():
        imp = sys.modules[module_name]
    else:
        imp = importlib.import_module(module_name)
    # else:
    #     del sys.modules[module_name]
    #     imp = importlib.import_module(module_name)
    return getattr(imp, class_name)


def get_classpath_by_object(obj):
    """
    通过对象获取类导入路径  包括自定义扩展的 Agent Tool Module  Workflow File子类属性
    :param obj:
    :return:
    """
    class_name = str(type(obj))
    m = re.match(r"<class\s\'(.*)\'>", class_name)
    class_name = m.group(1)
    paths = class_name.split(".")
    paths.pop()
    return ".".join(paths)


def daemonize(stdout='/dev/null', stderr='/dev/null'):

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # 从母体环境脱离
    # os.chdir("/")
    # os.umask(0)
    os.setsid()
    # 执行第二次fork
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)  # second parent out
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s]n" % (e.errno, e.strerror))
        sys.exit(1)

    # 进程已经是守护进程了，重定向标准文件描述符
    for f in sys.stdout, sys.stderr:
        f.flush()

    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def get_hostname():
    sys_name = os.name
    if sys_name == 'nt':
            host_name = os.getenv('computername')
            return host_name.replace('.local', '')
    elif sys_name == 'posix':
            with os.popen('echo $HOSTNAME') as f:
                host_name = f.readline()
                return host_name.strip('\n').replace('.local', '')
    else:
            return 'Unkwon hostname'


hostname = get_hostname()


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, ObjectId):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


def change_process_tile(tile):
    """
    更改当前进程名称
    :param tile: String 进程名称
    :return:
    """
    setproctitle.setproctitle(tile)


def filter_error_info(info):
    """
    过滤返回前端的错误信息，避免出现系统工作路径等敏感信息

    :param info: String 错误信息
    :return:
    """
    suberror = re.compile(r'/.+/.+/.+/')
    info = suberror.sub('', str(info))
    info.replace('\"', ' ').replace('\'', ' ')
    return info


def friendly_size(size):
    gb = 1024 * 1024 * 1024
    mb = 1024 * 1024
    kb = 1024
    if size >= gb:
        return "%sG" % (size / gb)
    elif size >= mb:
        return "%sM" % (size / mb)
    elif size >= kb:
        return "%sM" % (size / kb)
    else:
        return "%s" % size


class MessageData(Structure):
    _fields_ = [('action', c_char*50), ('id', c_char*1024), ("data", c_char*2048)]

    def __str__(self):
        return "<MessageData: action=%s, id=%s, other=%s, addr=%ld>" % \
               (self.action, self.id, self.other, addressof(self))


class LogData(Structure):
    _fields_ = [('action', c_char*50), ("data", c_char*20480), ("chunks", c_int)]

    def __str__(self):
        return "<MessageData: action=%s, data=%s, addr=%ld>" % \
               (self.action,  self.data, addressof(self))


class ActionData(Structure):
    _fields_ = [('action', c_char*50), ('id', c_char*50), ("timestamp", c_int)]

    def __str__(self):
        return "<ActionData: action=%s, id=%s, timestamp=%s, addr=%ld>" % \
               (self.action, self.data, self.timestamp, addressof(self))


def copier_factory(typ):
    def f(a, b):
        memmove(a, b, sizeof(typ))
    f.argtypes = [POINTER(typ), POINTER(typ)]
    return f


def copy_msg(a):
    b = MessageData()
    copy_func = copier_factory(MessageData)
    copy_func(byref(b), byref(a))
    return b


def copy_action(a):
    b = ActionData()
    copy_func = copier_factory(ActionData)
    copy_func(byref(b), byref(a))
    return b


def copy_log(a):
    b = LogData()
    copy_func = copier_factory(LogData)
    copy_func(byref(b), byref(a))
    return b


def add_run_queue(queue, action, ids, msg=None):
    # length = len(self.run_info_queue)
    is_full = True
    if len(action) > 50:
        raise MaxLengthError("action %s 长度超过最长50个限制!" % action)
    id_str = json.dumps(ids)
    if len(id_str) > 1024:
        raise MaxLengthError("ids %s 长度超过最长1024个限制!" % id_str)
    msg_str = json.dumps(msg, cls=CJsonEncoder) if msg else "\"\""
    if len(msg_str) > 2048:
        raise MaxLengthError("msg %s 长度超过最长1024个限制!" % msg_str)
    with queue.get_lock():
        for i, v in enumerate(queue):
            if v.action == "":
                is_full = False
                queue[i] = (action, id_str, msg_str)
                break
    if is_full:
        print "状态队列已满，请稍后...."
        sys.stdout.flush()
        time.sleep(1)
        return add_run_queue(queue, action, id_str, msg_str)


def add_log_queue(queue, action, data):
    length = len(queue)
    is_full = True
    size = 20480
    if len(action) > 50:
        raise MaxLengthError("action %s 长度超过最长50个限制!" % action)
    json_data = json.dumps(data, cls=CJsonEncoder) if data else '""'
    json_len = len(json_data)
    if json_len > size * 1000:
        raise MaxLengthError("data 长度超过最长10M系统不支持!")
    with queue.get_lock():
        for i, v in enumerate(queue):
            if queue[i].action == "":
                is_full = False
                if json_len > size * (length - i):
                    print "日志队列已满，请稍后...."
                    sys.stdout.flush()
                    is_full = True
                elif json_len > size:
                    if json_len % size > 0:
                        chunks = json_len // size + 1
                    else:
                        chunks = json_len // size
                    for m in xrange(chunks):
                        queue[i+m] = (action, json_data[(m*size):(m*size+size)], chunks)
                    break
                else:
                    queue[i] = (action, json_data, 0)
                    break
    if is_full:
        print "日志队列已满，请稍后...."
        sys.stdout.flush()
        time.sleep(1)
        return add_log_queue(queue, action, data)


def add_action_queue(queue, action, wid):
    time_stamp = int(time.time())
    is_full = True
    if len(action) > 50:
        raise MaxLengthError("action %s 长度超过最长50个限制!" % action)
    if len(wid) > 50:
        raise MaxLengthError("wid %s 长度超过最长50个限制!" % wid)
    with queue.get_lock():
        for i, v in enumerate(queue):
            if v.id == wid and v.action == action:
                return
        for i, v in enumerate(queue):
            if queue[i].action == "":
                is_full = False
                queue[i] = (action, wid, time_stamp)
                break
            if time_stamp - queue[i].timestamp > 200:
                is_full = False
                queue[i] = (action, wid, time_stamp)
                break
    if is_full:
        print "指令队列已满，请稍后...."
        sys.stdout.flush()
        time.sleep(1)
        return add_action_queue(queue, action, wid)


def get_run_queue(queue):
    msg_list = []
    with queue.get_lock():
        for i, v in enumerate(queue):
            if queue[i].action == "":
                break
            msg_list.append(copy_msg(queue[i]))
            queue[i] = ("", "", "")
    data = []
    for msg in msg_list:
        data.append((msg.action, json.loads(msg.id), json.loads(msg.data)))
    return data


def get_log_queue(queue):
    log_list = []
    with queue.get_lock():
        for i, v in enumerate(queue):
            if queue[i].action == "":
                break
            log_list.append(copy_log(queue[i]))
            queue[i] = ("", "", 0)
    data = []
    x = 0
    while x < len(log_list):

        big_data = ""
        if log_list[x].chunks > 1:
            chunks = log_list[x].chunks
            action = log_list[x].action
            for m in xrange(chunks):
                big_data += log_list[x].data
                x += 1
            data.append((action, json.loads(big_data)))
        else:
            data.append((log_list[x].action, json.loads(log_list[x].data)))
            x += 1
    return data


def get_action_queue(queue, wid):
    with queue.get_lock():
        for i, v in enumerate(queue):
            if v.id == wid:
                action = v.action
                queue[i] = ("", "", 0)
                return action
    return None


class StateData(Structure):
    _fields_ = [("id", c_char*500), ('state', c_char*50), ("data", c_char*2048),  ("version", c_int),
                ("timestamp", c_int)]

    def __str__(self):
        return "< StateData: id=%s, state=%s, version=%s data=%s, timestamp=%s, addr=%ld>" % \
               (self.id, self.state,  self.version, self.data, self.timestamp, addressof(self))

    def to_dict(self):
        return {
            "id": self.id,
            "state": self.state,
            "data": json.loads(self.data),
            "version": self.version
        }


def copy_state(a):
    b = StateData()
    copy_func = copier_factory(StateData)
    copy_func(byref(b), byref(a))
    return b


class CallbackData(Structure):
    _fields_ = [("id", c_char*500), ('action', c_char*50), ("data", c_char*2048),  ("version", c_int)]

    def __str__(self):
        return "< CallbackData: id=%s, action=%s,  data=%s,  version=%s, addr=%ld>" % \
               (self.id, self.action,  self.data, self.version, addressof(self))

    def to_dict(self):
        return {
            "id": self.id,
            "action": self.action,
            "data": json.loads(self.data),
            "version": self.version
        }


def copy_callback(a):
    b = CallbackData()
    copy_func = copier_factory(CallbackData)
    copy_func(byref(b), byref(a))
    return b
