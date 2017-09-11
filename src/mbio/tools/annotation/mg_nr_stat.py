# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.annotation.nr_stat import nr_stat
import os
import threading
import re


class MetagenNrAgent(Agent):
    """
    nr_stat.py 等功能 v1.0
    author: zhouxuan
    last_modify: 2017.0602
    """

    def __init__(self, parent):
        super(MetagenNrAgent, self).__init__(parent)
        options = [
            {"name": "taxon_out", "type": "infile", "format": "annotation.nr.nr_taxon"},
            # 比对到nr库的结果文件query_taxons_detail.xls
            {"name": "reads_profile_table", "type": "infile", "format": "sequence.profile_table"}
            ]
        self.add_option(options)

    def check_options(self):
        if not self.option("taxon_out").is_set:
            raise OptionError("必须设置输入文件")
        return True

    def set_resource(self):
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['query_taxons_detail.xls', 'xls', '序列详细物种分类文件']
            ])
        super(MetagenNrAgent, self).end()


class MetagenNrTool(Tool):
    def __init__(self, config):
        super(MetagenNrTool, self).__init__(config)
        self._version = "1.0"
        self.python_path = "program/Python/bin/python"
        self.python_script = self.config.SOFTWARE_DIR + '/bioinfo/taxon/scripts/nr_profile.py'
        self.python_script_2 = self.config.SOFTWARE_DIR + '/bioinfo/taxon/scripts/metagen_nr_taxlevel.py'

    def run(self):
        """
        运行
        :return:
        """
        super(MetagenNrTool, self).run()
        self.run_nr_stat()
        self.set_output()
        self.end()

    def run_nr_stat(self):
        nr = nr_stat()
        self.logger.info("start nr_stat(detail_to_level)")
        try:
            nr.detail_to_level(detail_file=self.option('taxon_out').prop['path'], out_dir=self.work_dir)
        except Exception as e:
            self.logger.info("nr_stat(detail_to_level) failed")
            raise Exception("nr_stat(detail_to_level) failed {}".format(e))
        os.remove(self.work_dir + "/gene_taxons.xls")   # 删除没有用的结果文件
        os.remove(self.work_dir + "/gene_taxons_detail.xls")
        self.new_query_taxons = os.path.join(self.work_dir, "new_query_taxons.xls")
        self.rm_1(self.work_dir + "/query_taxons.xls", self.new_query_taxons)
        os.remove(self.work_dir + "/query_taxons.xls")
        self.tax_profile()

    def rm_1(self, old_path, new_path):
        with open(old_path, "r") as r, open(new_path, "a") as w:
            for line in r:
                line = line.strip('\n').split("_1\t")
                new_line = ('\t').join(line)
                w.write(new_line + "\n")

    def tax_profile(self):
        self.logger.info("start nr_tax_profile")
        cmd1 = "{} {} -i {} -r {} -o {}".format(self.python_path, self.python_script, self.new_query_taxons,
                                                self.option('reads_profile_table').prop['path'], self.output_dir)
        command1 = self.add_command('tax_profile', cmd1).run()
        self.wait(command1)
        if command1.return_code == 0:
            self.logger.info("tax_profile succeed")
        else:
            self.set_error("tax_profile failed")
            raise Exception("tax_profile failed")
        # cmd2 = self.python_path + ' {} -i {} -l 1,2,3,4,5,6,7 -o {}'.\
        #     format(self.python_script_2, self.output_dir + "/tax_profile.xls", self.output_dir)
        # command2 = self.add_command('tax_level', cmd2).run()
        # self.wait(command2)
        # if command2.return_code == 0:
        #     self.logger.info("tax_level succeed")
        # else:
        #     self.set_error("tax_level failed")
        #     raise Exception("tax_level failed")

    def set_output(self):
        self.logger.info("start set_output")