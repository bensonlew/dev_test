# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import json
from collections import OrderedDict


def param_pack(param):
    if not isinstance(param, dict):
        raise Exception("传入的param不是一个字典")
    new_param = OrderedDict(sorted(param.items()))
    params = json.dumps(new_param)
    params = re.sub(':\s+', ':', params)
    params = re.sub(',\s+', ',', params)
    return params


def group_detail_sort(detail):
    table_dict = json.loads(detail)
    if not isinstance(table_dict, dict):
        raise Exception("传入的table_dict不是一个字典")
    for keys in table_dict.keys():
        table_dict[keys] = sorted(table_dict[keys])
    sort_key = dict(OrderedDict(sorted(table_dict.items(), key=lambda t: t[0])))
    sort_key = json.dumps(sort_key)
    return sort_key
