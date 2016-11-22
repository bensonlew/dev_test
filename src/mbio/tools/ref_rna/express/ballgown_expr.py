#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"


from mako.template import Template
from biocluster.config import Config
import os


def clust(input_matrix, feature="gene", meas="FPKM", group="group"):
    """
    生成并运行R脚本，进行两组样品的差异性分析，包括student T检验，welch T检验，wilcox秩和检验
    :param input_matrix: 差异表达量文件
    :param feature: 种类，"gene", "exon", "intron", "transcript"
    :param meas：选择使用数据 "cov", "FPKM", "rcount", "ucount", "mrcount", "mcov"
    """
    this_file_dir = os.path.dirname(os.path.realpath(__file__))
    f = Template(filename=this_file_dir + '/ballgown.r')
    clust = f.render(input_matrix=input_matrix,feature=feature,meas=meas,group=group)
    with open("ballgown", 'w') as rfile:
        rfile.write("%s" % clust)
