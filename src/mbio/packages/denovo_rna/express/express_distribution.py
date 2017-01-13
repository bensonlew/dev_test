#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "qiuping"


from mako.template import Template
from biocluster.config import Config
import os


def distribution(rfile, input_matrix, outputfile):
    """
    生成表达量分布图的数据
    :param input_matrix: 表达量矩阵
    :param outputfile: 输出文件路径
    """
    this_file_dir = os.path.dirname(os.path.realpath(__file__))
    f = Template(filename=this_file_dir + '/express_distribution.r')
    distribution = f.render(input_matrix=input_matrix, outputfile=outputfile)
    with open(rfile, 'w') as rf:
        rf.write("%s" % distribution)
