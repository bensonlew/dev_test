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
            {'name': 'otu_seqids', 'type': 'infile', 'format': 'meta.otu.otu_seqids'},  # 输入的seqids文件
            {'name': 'taxon_file', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 输入的taxon文件
            {'name': 'otu_taxon_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 输出的biom文件
            {'name': 'otu_taxon_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出的otu表文件
            {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'meta.otu.tax_summary_abs_dir'}]  # 输出的otu_taxon_dir文件夹
        self.add_option(options)

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
    需要脚本make_otu_table.py,summarize_taxa.py,sum_tax.pl,
    """
    def __init__(self, config):
        super(OtuTaxonStatTool, self).__init__(config)
        self._version = 1.0
        self._biom_path = "Python/bin/biom"
        self._make_otu_table_path = "Python/bin/make_otu_table.py"
        self._summarize_taxa_path = "Python/bin/summarize_taxa.py"
        self._sum_tax_path = os.path.join(Config().SOFTWARE_DIR, "meta/scripts/sum_tax.fix.pl")
        self.otu_taxon_dir = os.path.join(self.work_dir, "output", "tax_summary_a")

    def get_biom_otu(self):
        """
        根据otu_seqids和seq_taxon文件生成biom表和otu表

        :return: 生成的biom和otu文件的路径
        """
        biom = os.path.join(self.work_dir, "output", "otu_taxon.biom")
        otu_table_tmp = os.path.join(self.work_dir, "output", "otu_taxon.otu_table.tmp")
        otu_table = os.path.join(self.work_dir, "output", "otu_taxon.otu_table")
        cmd = self._make_otu_table_path + " -i " + self.option("otu_seqids").prop['path']\
            + " -t " + self.option("taxon_file").prop['path'] + " -o " + biom
        create_biom = self.add_command("create_biom", cmd)
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + "/gcc/5.1.0/lib64:$LD_LIBRARY_PATH")
        self.logger.info("开始生成biom")
        create_biom.run()
        self.wait(create_biom)
        if create_biom.return_code == 0:
            self.logger.info("biom生成成功")
        else:
            self.set_error("biom生成出错!")

        cmd = self._biom_path + " convert -i " + biom + " -o " + otu_table_tmp\
            + " --header-key taxonomy --table-type \"OTU table\" --to-tsv"
        create_otu_table = self.add_command("create_otu_table", cmd)
        self.logger.info("开始转换生成otu_table")
        create_otu_table.run()
        self.wait(create_otu_table)
        if create_otu_table.return_code == 0:
            self.logger.info("otu_table生成成功")
        else:
            self.set_error("otu_table生成失败")
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
        tax_summary_a_dir = os.path.join(self.work_dir, "output", "tax_summary_a")
        cmd = self._summarize_taxa_path + " -i " + biom + ' -o ' + tax_summary_a_dir\
            + " -L 1,2,3,4,5,6,7 -a "
        create_tax_summary = self.add_command("create_tax_summary", cmd)
        self.logger.info("开始生成tax_summary_a文件夹")
        create_tax_summary.run()
        self.wait(create_tax_summary)
        if create_tax_summary.return_code == 0:
            self.logger.info("文件夹生成成功")
        else:
            self.logger.info("文件夹生成失败")
        list_ = os.listdir(tax_summary_a_dir)
        for my_otu_table in list_:
            if re.search(r"txt", my_otu_table):
                my_otu_table = os.path.join(tax_summary_a_dir, my_otu_table)
                otu_basename = os.path.basename(my_otu_table)
                otu_basename = re.sub(r'\.txt$', r'.xls', otu_basename)
                otu_name = os.path.join(tax_summary_a_dir, otu_basename)
                cmd = self._sum_tax_path + " -i " + my_otu_table + " -o " + otu_name
            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError:
                raise Exception("运行sum_tax.pl出错")

    def run(self):
        """
        运行
        """
        super(OtuTaxonStatTool, self).run()
        (biom, otu_table) = self.get_biom_otu()
        self.option("otu_taxon_biom").set_path(biom)
        self.option("otu_taxon_table").set_path(otu_table)
        self.get_diff_level(biom)
        self.option("otu_taxon_dir").set_path(self.otu_taxon_dir)
        self.logger.info("otu_taxon完成，即将退出程序")
        self.end()
