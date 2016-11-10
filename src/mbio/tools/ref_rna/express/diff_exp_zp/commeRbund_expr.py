#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"


from mako.template import Template
from biocluster.config import Config
import os


def clust(input_matrix, feature="gene"):
    this_file_dir = os.path.dirname(os.path.realpath(__file__))
    f = Template(filename=this_file_dir + '/commeRbund_R.r')
    clust = f.render(input_matrix=input_matrix, feature=feature)
    with open("commeRbund_R", 'w') as rfile:
        rfile.write("%s" % clust)
