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
            return host_name
    elif sys_name == 'posix':
            with os.popen('echo $HOSTNAME') as f:
                host_name = f.readline()
                return host_name.strip('\n')
    else:
            return 'Unkwon hostname'


hostname = get_hostname()


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


def change_process_tile(tile):
    """
    更改当前进程名称
    :param tile: String 进程名称
    :return:
    """
    setproctitle.setproctitle(tile)
