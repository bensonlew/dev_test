#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"


from mako.template import Template
from biocluster.config import Config
import os


def clust(input_matrix, method="all", lognorm=10, distance_method="euclidean", cltype="both"):
    """
    生成并运行R脚本，进行两组样品的差异性分析，包括student T检验，welch T检验，wilcox秩和检验
    :param input_matrix: 差异表达量矩阵
    :param method: 聚类方法选择，hclust，kmeans
    :param lognorm: 对数底数选择，10或2
    :param cltype：热图排序方式
    :param sub_num: 子聚类数目
    :param distance_method: 距离矩阵算法
    """
    this_file_dir = os.path.dirname(os.path.realpath(__file__))
    f = Template(filename=this_file_dir + '/sample_analysis.r')
    clust = f.render(input_matrix=input_matrix, lognorm=lognorm, distance_method=distance_method, method=method, cltype=cltype)
    with open("sample_analysis", 'w') as rfile:
        rfile.write("%s" % clust)
