# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import re
import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config


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

    def check_options(self):
        """
        参数检测
        """
        if not self.option("otu_seqids").is_set:
            raise OptionError("参数otu_seqids不能为空")
        if not self.option("taxon_file").is_set:
            raise OptionError("参数taxon_file不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 1
        self._memory = ''


class OtuTaxonStatTool(Tool):
    """
    otu taxon stat tool
    需要软件biom
    需要脚本make_otu_table.py,summarize_taxa.pl,sum_tax.pl,
    """
    def __init__(self, config):
        super(OtuTaxonStatTool, self).__init__(config)
        self._version = 1.0
        self._biom_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/biom")
        self._make_otu_table_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/make_otu_table.py")
        self._summarize_taxa_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/summarize_taxa.pl")
        self._sum_tax_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/sum_tax.pl")
        self.otu_taxon_dir = os.path.join(self.work_dir, "otu_taxon_dir")

    def get_biom_otu(self):
        """
        根据otu_seqids和seq_taxon文件生成biom表和otu表

        :return: 生成的biom和otu文件的路径
        """
        biom = os.path.join(self.otu_taxon_dir, "otu_taxon.biom")
        otu_table_tmp = os.path.join(self.otu_taxon_dir, "otu_taxon.otu_table.tmp")
        otu_table = os.path.join(self.otu_taxon_dir, "otu_taxon.otu_table")
        cmd = "python " + self._make_otu_table_path + " -i " + self.option("otu_seqids").prop['path']\
            + " -t " + self.option("taxon_file") + " -o " + biom
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            raise Exception("运行make_otu_table.py出错")
        cmd = self._biom_path + " convert -i " + biom + " -o " + otu_table_tmp\
            + " --header-key taxonomy --table-type \"OTU table\" --to-tsv"
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            raise Exception("运行biom出错")
        row = 0
        with open(otu_table, 'w') as w:
            with open(otu_table_tmp, "r") as r:
                line = r.readline()
                row += 1
                if row == 2:
                    line = re.sub(r"#", "", line)
                if row > 2:
                    line = re.sub(r"\.0", "", line)
                w.write(line + "\n")
        return (biom, otu_table)

    def get_diff_level(self, biom):
        """
        :param biom: biom文件路径
        """
        tax_summary_a_dir = os.path.join(self.otu_taxon_dir, "tax_summary_a")
        cmd = "perl" + self._summarize_taxa_path + " -i " + biom + tax_summary_a_dir\
            + "-L 1,2,3,4,5,6,7 -a"
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            raise Exception("运行summarize_taxa.pl出错")
        list_ = os.listdir(tax_summary_a_dir)
        for my_otu_table in list_:
            otu_basename = os.path.basename(my_otu_table)
            otu_basename = re.sub(r'\.txt$', r'\.otu_table', otu_basename)
            otu_name = os.path.join(tax_summary_a_dir, otu_basename)
            cmd = self._sum_tax_path + " -i " + my_otu_table + " -o " + otu_name
            try:
                subprocess.check_call(cmd)
            except subprocess.CalledProcessError:
                raise Exception("运行sum_tax.pl出错")

    def run(self):
        """
        运行
        """
        (biom, otu_table) = self.get_biom_otu()
        self.get_diff_level(biom)
        self.option("otu_taxon_dir").set_path(self.otu_taxon_dir)
