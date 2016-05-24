# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""数据拆分"""

import os
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError


class DatasplitWorkflow(Workflow):
    def __init__(self, wsheet_object):
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
            self.unzip = self.add_tool("datasplit.unzip")
            self.fastx = self.add_tool("datasplit.fastx")
        sec_program_tmp = list()
        sec_program = list()
        for p in self.option('sample_info').prop['parent_sample']:
            sec_program_tmp.append(p["program"])
        sec_program = list(set(sec_program_tmp))
        for program in sec_program:
            if program == "miseq_split":
                self.rawValid = self.add_tool("datasplit.raw_valid")
                self.qualControl = self.add_tool("datasplit.qual_control")
                self.chompSeq = self.add_tool("datasplit.chomp_seq")
                self.merge = self.add_tool("datasplit.merge")
                self.markSeq = self.add_tool("datasplit.mark_seq")
                self.lenControl = self.add_tool("datasplit.len_control")
                self.seqExtract = self.add_tool("datasplit.seq_extract")
                self.backup = self.add_tool("datasplit.backup")
                self.splitStat = self.add_tool("datasplit.split_stat")

        ###
        if (sec_program == [u'']) and self.option('sample_info').prop['program'] == "bcl2fastq":
            self.rawValid = self.add_tool("datasplit.raw_valid")
            self.qualControl = self.add_tool("datasplit.qual_control")
            self.chompSeq = self.add_tool("datasplit.chomp_seq")
            self.merge = self.add_tool("datasplit.merge")
            self.markSeq = self.add_tool("datasplit.mark_seq")
            self.lenControl = self.add_tool("datasplit.len_control")
            self.seqExtract = self.add_tool("datasplit.seq_extract")
            self.backup = self.add_tool("datasplit.backup")
            self.splitStat = self.add_tool("datasplit.split_stat")

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
        self.bcl2fastq.run()

    def run_unzip(self):
        self.logger.info("开始解压文库")
        self.unzip.set_options({
            "sample_info": self.option('sample_info'),
            "data_path": os.path.join(self.bcl2fastq.work_dir, "soft_output")
        })
        self.unzip.run()

    def run_fastx(self):
        self.logger.info("开始运行fastx")
        self.fastx.set_options({
            "sample_info": self.option('sample_info'),
            "unzip_path": os.path.join(self.unzip.work_dir, "unzip")
        })
        self.fastx.run()

    def run_raw_valid(self):
        self.logger.info("开始运行raw_valid")
        self.rawValid.set_options({
            "sample_info": self.option('sample_info'),
            "unzip_path": os.path.join(self.unzip.work_dir, "unzip")
        })
        self.rawValid.run()

    def run_qual_control(self):
        self.logger.info("开始运行qual_control")
        self.qualControl.set_options({
            "sample_info": self.option('sample_info'),
            "var2end_path": os.path.join(self.rawValid.work_dir, "var2end")
        })
        self.qualControl.run()

    def run_chomp_seq(self):
        self.logger.info("开始对序列进行截短")
        self.chompSeq.set_options({
            "sample_info": self.option('sample_info'),
            "trim_path": os.path.join(self.qualControl.work_dir, "trimmo")
        })
        self.chompSeq.run()

    def run_merge(self):
        self.logger.info("开始对序列进行merge")
        self.merge.set_options({
            "sample_info": self.option('sample_info'),
            "chomp_path": os.path.join(self.chompSeq.work_dir, "chomped")
        })
        self.merge.run()

    def run_mark_seq(self):
        self.logger.info("开始标记序列的样本名称")
        self.markSeq.set_options({
            "sample_info": self.option('sample_info'),
            "merge_path": os.path.join(self.merge.work_dir, "merge")
        })
        self.markSeq.run()

    def run_len_control(self):
        self.logger.info("开始滤去过短的序列")
        self.lenControl.set_options({
            "sample_info": self.option('sample_info'),
            "marked_path": os.path.join(self.markSeq.work_dir, "markedSeq")
        })
        self.lenControl.run()

    def run_seq_extract(self):
        self.logger.info("开始从文库中抽提样本序列")
        self.seqExtract.set_options({
            "sample_info": self.option('sample_info'),
            "LenControled_path": os.path.join(self.lenControl.work_dir, "LenControled"),
            "rawValid_path": os.path.join(self.rawValid.work_dir, "var2end")
        })
        self.seqExtract.run()

    def run_backup(self):
        self.logger.info("开始备份数据")
        self.backup.set_options({
            "sample_info": self.option('sample_info'),
            "parent_path": os.path.join(self.unzip.work_dir, "unzip"),
            "fastx_path": os.path.join(self.fastx.work_dir, "fastx"),
            "extract_path": os.path.join(self.seqExtract.work_dir, "child_sample"),
            "report_path": os.path.join(self.bcl2fastq.work_dir, "soft_output", "Reports")
        })
        self.backup.run()

    def run_split_stat(self):
        self.logger.info("开始统计拆分结果")
        self.splitStat.set_options({
            "sample_info": self.option('sample_info'),
            "bcl2fastq_path": self.bcl2fastq.work_dir,
            "fastx_path": self.fastx.work_dir,
            "rawValid_path": self.rawValid.work_dir,
            "qualcontrol_path": self.qualControl.work_dir,
            "merge_path": self.merge.work_dir,
            "markSeq_path": self.markSeq.work_dir,
            "lenControl_path": self.lenControl.work_dir,
            "seqExtract_path": self.seqExtract.work_dir,
            "time": self.backup.option("time")
        })
        self.splitStat.run()

    def split_end(self):
        json_file = os.path.join(self.splitStat.work_dir, "output", "stat.json")
        with open(json_file, 'r') as r:
            json_str = r.read()
        self.step.add_api_data("data", json_str)
        self.end()

    def run(self):
        self.bcl2fastq.on("end", self.run_unzip)
        self.unzip.on("end", self.run_fastx)
        self.unzip.on("end", self.run_raw_valid)
        self.rawValid.on("end", self.run_qual_control)
        self.qualControl.on("end", self.run_chomp_seq)
        self.chompSeq.on("end", self.run_merge)
        self.merge.on("end", self.run_mark_seq)
        self.markSeq.on("end", self.run_len_control)
        self.lenControl.on("end", self.run_seq_extract)
        self.on_rely([self.seqExtract, self.fastx], self.run_backup)
        self.backup.on("end", self.run_split_stat)
        self.splitStat.on("end", self.split_end)
        self.run_bcl2fastq()
        super(DatasplitWorkflow, self).run()
