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
