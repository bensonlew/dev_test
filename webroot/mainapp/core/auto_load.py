# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import web
import importlib
import os
import types
import re


package_urls = dict()
package_urls["mainapp.controllers"] = ""
main_app = None


def check_auto_load_packages(parent_package):
    parent = importlib.import_module(parent_package)
    reload(parent)
    for pack_name in parent.AUTO_LOAD_SUB_PACKAGES:
        package = importlib.import_module("%s.%s" % (parent_package, pack_name))
        if hasattr(package, "PACKAGE_URL"):
            if not isinstance(package.PACKAGE_URL, types.StringTypes):
                raise Exception("%s PACKAGE_URL必须为字符串" % package.__name__)
            if len(package.PACKAGE_URL) == 0:
                raise Exception("%s PACKAGE_URL不能为空" % package.__name__)
            if package.PACKAGE_URL.find("\\") >= 0 or package.PACKAGE_URL.find("/") >= 0:
                raise Exception("%s PACKAGE_URL不能含有\"\\\"或\"/\"符号!" % package.__name__)
            sub_url = package.PACKAGE_URL
        else:
            sub_url = pack_name
        package_urls[package.__name__] = "%s/%s" % (package_urls[parent.__name__], sub_url)
        if hasattr(package, "AUTO_LOAD_SUB_PACKAGES"):
            check_auto_load_packages(package.__name__)


def register(app):
    web.app = app
    app.add_processor(web.loadhook(load_hook))


def load_hook():
    check_auto_load_packages("mainapp.controllers")
    urls = dict((v.lower(), k) for k, v in package_urls.iteritems())
    del urls[""]
    path = web.ctx.path.lower()
    path_list = re.split("/+", path)
    module_name = path_list.pop()
    if module_name == "":
        module_name = path_list.pop()
    p_url = "/".join(path_list)
    if p_url in urls.keys():
        try:
            classname = get_class_name(module_name)
            modulename = "%s.%s" % (urls[p_url], module_name)
            module = importlib.import_module(modulename)
            if hasattr(module, classname):
                web.app.add_mapping(path, "%s.%s" % (modulename, classname))
        except:
            pass


def get_class_name(path):
    l = path.split("_")
    l.append("action")
    l = [el.capitalize() for el in l]
    return "".join(l)
