# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""数据拆分"""

import os
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError


class DatasplitWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        """
        self._sheet = wsheet_object
        super(DatasplitWorkflow, self).__init__(wsheet_object)
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'}  # 样本拆分信息表
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.option('sample_info').get_info()
        if self.option('sample_info').prop['program'] == "bcl2fastq":
            self.bcl2fastq = self.add_tool("datasplit.bcl2fastq")
            self.fastx = self.add_tool("datasplit.fastx")
        sec_program_tmp = list()
        sec_program = list()
        for p in self.option('sample_info').prop['parent_sample']:
            sec_program_tmp.append(p["program"])
        sec_program = list(set(sec_program_tmp))
        for program in sec_program:
            if program == "miseq_split":
                self.second_split = self.add_tool("datasplit.second_split")
                self.backup = self.add_tool("datasplit.backup")
                self.split_stat = self.add_tool("datasplit.split_stat")
        self.logger.debug(sec_program)
        self.logger.debug(self.option('sample_info').prop['program'])
        if (sec_program == [u'']) and self.option('sample_info').prop['program'] == "bcl2fastq":
            self.second_split = self.add_tool("datasplit.second_split")
            self.backup = self.add_tool("datasplit.backup")
            self.split_stat = self.add_tool("datasplit.split_stat")

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('sample_info').is_set:
            raise OptionError("必须设置参数sample_info")
        return True

    def run_bcl2fastq(self):
        self.logger.info("开始运行bcl2fastq")
        self.bcl2fastq.set_options({
            "sample_info": self.option('sample_info')
        })
        self.on_rely(self.bcl2fastq, self.run_fastx)
        self.bcl2fastq.run()

    def run_fastx(self):
        self.logger.info("开始运行fastx")
        self.fastx.set_options({
            "sample_info": self.option('sample_info'),
            "data_path": os.path.join(self.bcl2fastq.work_dir, "soft_output")
        })
        self.on_rely(self.fastx, self.run_second_split)
        self.fastx.run()

    def run_second_split(self):
        self.logger.info("开始进行二次拆分")
        self.second_split.set_options({
            "sample_info": self.option('sample_info'),
            "unzip_path": os.path.join(self.fastx.work_dir, "unzip")
        })
        self.on_rely(self.second_split, self.run_backup)
        self.second_split.run()

    def run_backup(self):
        self.logger.info("开始备份")
        self.backup.set_options({
            "sample_info": self.option('sample_info'),
            "parent_path": os.path.join(self.fastx.work_dir, "unzip"),
            "fastx_path": os.path.join(self.fastx.work_dir, "fastx"),
            "child_path": os.path.join(self.second_split.work_dir, "child_seq"),
            "report_path": os.path.join(self.bcl2fastq.work_dir, "soft_output", "Reports")
        })
        self.on_rely(self.backup, self.run_split_stat)
        self.backup.run()

    def run_split_stat(self):
        self.logger.info("开始统计信息")
        self.split_stat.set_options({
            "sample_info": self.option('sample_info'),
            "fastx_path": os.path.join(self.fastx.work_dir, "fastx"),
            "time": self.backup.option('time'),
            "stat_dir": os.path.join(self.second_split.work_dir, "stat")
        })
        self.split_stat.run()

    def split_end(self):
        json_file = os.path.join(self.split_stat.work_dir, "output", "stat.json")
        with open(json_file, 'r') as r:
            json_str = r.read()
        self.logger.debug(json_str)
        self.step.add_api_data("data", json_str)
        self.end()

    def run(self):
        self.run_bcl2fastq()
        self.on_rely(self.split_stat, self.split_end)
        super(DatasplitWorkflow, self).run()
