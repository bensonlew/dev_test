# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import importlib
import re


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
