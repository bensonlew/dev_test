# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import copy
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class OtuTaxonStatAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.03
    """
    def __init__(self, parent):
        super(OtuTaxonStatAgent, self).__init__(parent)
        options = [
                {'name': 'otu_seqids', 'type': 'infile', 'format': 'otuseqids'},  # 输入的seqids文件
                {'name': 'taxon_file', 'type': 'infile', 'format': 'seq_taxon'},  # 输入的taxon文件
                {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'otu_taxon_dir'}  # 输出的otu_taxon_dir文件夹，包含16个文件
                ]
        self.set_options(options)

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 1
        self._memory = ''

class OtuTaxonStatTool(Tool):
    """
    otu taxon stat tool
    需要
    """
