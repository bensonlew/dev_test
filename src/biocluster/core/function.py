# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import importlib
import re
import os
import sys


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
    imp = importlib.import_module(dir_name[tp] + path)
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


def daemonize(stdout='/dev/null', stderr='dev/null'):

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
