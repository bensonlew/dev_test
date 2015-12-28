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
            self.bcl2fastq = self.add_tool("datasplit.bcl2fstq")
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

    def check_options(self):
        """
        检查参数设置
        """
        if self.option('sample_info').is_set:
            raise OptionError("必须设置参数sample_info")

    def run_bcl2fastq(self):
        self.bcl2fastq.set_options({
            "sample_info": self.option('sample_info')
        })
        self.on_rely(self.bcl2fastq, self.run_fastx)
        self.bcl2fastq.run()

    def run_fastx(self):
        self.fastx.set_options({
            "sample_info": self.option('sample_info'),
            "data_path": os.path.join(self.bcl2fastq.work_dir, "soft_output")
        })
        self.on_rely(self.fastx, self.run_second_split)
        self.fastx.run()

    def run_second_split(self):
        self.second_split.set_options({
            "sample_info": self.option('sample_info'),
            "data_path": os.path.join(self.fastx.work_dir, "unzip")
        })
        self.on_rely(self.second_split, self.run_backup)
        self.second_split.run()

    def run_backup(self):
        self.backup.set_options({
            "sample_info": self.option('sample_info'),
            "parent_path": os.path.join(self.fastx.work_dir, "unzip"),
            "fastx_path": os.path.join(self.fastx.work_dir, "fastx"),
            "child_path": os.path.join(self.second_split.work_dir, "child_seq"),
            "report_path": os.path.join(self.bcl2fastq.work_dir, "Reports")
        })
        self.on_rely(self.backup, self.run_split_stat)
        self.backup.run()

    def run_split_stat(self):
        self.backup.set_options({
            "sample_info": self.option('sample_info'),
            "fastx_path": os.path.join(self.fastx.work_dir, "fastx"),
            "time": self.backup.option('time'),
            "stat": os.path.join(self.second_split.work_dir, "stat")
        })
        self.split_stat.run()

    def run(self):
        super(DatasplitWorkflow, self).run()
        self.run_bcl2fastq()
        self.on_rely(self.split_stat, self.end)
