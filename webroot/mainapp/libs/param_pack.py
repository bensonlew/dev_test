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


def sub_group_detail_sort(detail):
    table_list = json.loads(detail)
    result_list = []
    for table_dict in table_list:
        if not isinstance(table_dict, dict):
            raise Exception("传入的table_dict不是一个字典")
        for keys in table_dict.keys():
            table_dict[keys] = sorted(table_dict[keys])
        sort_key = OrderedDict(sorted(table_dict.items(), key=lambda t: t[0]))
        result_list.append(sort_key)
    result_list = json.dumps(result_list, sort_keys=True, separators=(',', ':'))
    return result_list


def group_detail_sort(detail):
    table_dict = json.loads(detail)
    if not isinstance(table_dict, dict):
        raise Exception("传入的table_dict不是一个字典")
    for keys in table_dict.keys():
        table_dict[keys] = sorted(table_dict[keys])
    sort_key = OrderedDict(sorted(table_dict.items(), key=lambda t: t[0]))
    table_dict = sort_key
    table_dict = json.dumps(table_dict, sort_keys=True, separators=(',', ':'))
    return table_dict
