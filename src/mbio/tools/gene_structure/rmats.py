# -*- coding: utf-8 -*-
# __author__ = 'linfang.jin'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import shutil
import re


class RmatsAgent(Agent):
    '''
    rmats 可变剪切分析的一款软件
    version 3.2.5
    author: linfang.jin
    last_modify: 2016.9.8
    '''

    def __init__(self, parent):
        super(RmatsAgent, self).__init__(parent)
        options = [
            {"name": "sequencing_type", "type": "string"},  # paired or single
            {"name": "analysis_mode", "type": "string", "default": "U"}, #'P' is for paired analysis and 'U' is for unpaired analysis，Type of analysis to perform
            {"name": "sequencing_read_length", "type": "int"}, #The length of each read
            {"name": "condition_1_bam_file", "type": "string"},
            {"name": "condition_2_bam_file", "type": "string"},
            {"name": "genome_annotation_file", "type": "string"}, #必须设置
            {"name": "whether_to_find_novel_splice_sites", "type": "int", "default": 0}, #1 is for detection of novel splice sites
            {"name": "sequencing_library_type", "type": "string", "default": "fr-unstranded"}, # unstranded (fr-unstranded). Use fr-firststrand or fr-secondstrand for strand-specific data.
            {"name": "The_cutoff_splicing_difference", "type": "float", "default": 0.0001}# cutoff splicing difference。he default is 0.0001 for 0.01% difference. Valid: 0 ≤ cutoff < 1 ，the cutoff used in the null hypothesis test for differential splicing.
            ]
        self.add_option(options)
        self.step.add_steps('rmats')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.rmats.start()
        self.step.update()

    def step_end(self):
        self.step.rmats.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('sequencing_type'):
            raise OptionError("必须设置测序类型： Paired-End or Single-End data")
        if not self.option('condition_1_bam_file'):
            raise OptionError("必须设置第一种条件下的样品的bam文件")
        if not self.option('condition_2_bam_file'):
            raise OptionError('必须设置第二种条件下的样品的bam文件')
        number_of_condition_1_bam_file = len(self.option('condition_1_bam_file').split(","))
        number_of_condition_2_bam_file = len(self.option('condition_2_bam_file').split(","))  # 计算条件2下指定重复样本的bam文件个数
        if number_of_condition_1_bam_file == 0:
            raise OptionError('您指定的第一种条件下的样品bam文件个数为空')
        if number_of_condition_2_bam_file == 0:
            raise OptionError('您指定的第二种条件下的样品bam个数为空')
        suitable_for_P_mode_analysis = (self.option('analysis_mode') == 'P') and ((
                                                                                      number_of_condition_1_bam_file != number_of_condition_2_bam_file) or number_of_condition_2_bam_file < 3 or number_of_condition_1_bam_file < 3)

        if suitable_for_P_mode_analysis:
            raise OptionError('您指定的第一种或第二种条件下的样品bam文件不大于3,或者两组bam文件个数不相等')
        # 考虑两种条件下的重复个数不相等的情况 需要测试
        if not self.option('genome_annotation_file'):
            raise OptionError('必须设置参考基因组注释文件（ref_genome.gtf）')
        if not self.option('sequencing_read_length'):
            raise OptionError('必须设置测序读长的长度')
        if self.option('The_cutoff_splicing_difference') < 0 or self.option('The_cutoff_splicing_difference') >= 1:
            raise OptionError('差异剪接假设检验的置信度p应该: 0=< p <1')

        return True

    def set_resource(self):
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        """
        agent结束后一些文件德操作

        :return:
        """
        # result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #     [".", "", "结果输出目录"],
        # ])
        # result_dir.add_regexp_rules([
        #     ["new_gtf", "gtf", "新转录本注释文件"],
        #     ["new_fa", "fa", "新转录本序列文件"],
        # ])

        super(RmatsAgent, self).end()


class RmatsTool(Tool):
    def __init__(self, config):
        super(RmatsTool, self).__init__(config)
        self._version = "v3.2.5"
        self.cmd_path = "/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/rMATS.3.2.5/RNASeq-MATS.py "
        self.Python_path = 'program/Python/bin/python '
        self.output = os.path.join(self.work_dir, "rmats_out")

    def run_rmats(self):
        """
        运行rmats
        :return:
        """
        cmd = "{} {}  -b1 {} -b2 {} -gtf {} -o {}rmats_out -t {}  -len {}  -novelSS {}  -analysis {} -c {}  -libType {}".format(self.Python_path, self.cmd_path, self.option('condition_1_bam_file'),
                   self.option('condition_2_bam_file'),self.option('genome_annotation_file'), self.output_dir, self.option('sequencing_type'),self.option('sequencing_read_length'), self.option('whether_to_find_novel_splice_sites'),
                   self.option('analysis_mode'), self.option('The_cutoff_splicing_difference'),self.option('sequencing_library_type'))
        self.logger.info('运行rmats')
        command = self.add_command("rmats_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("rmats运行完成")
        else:
            self.set_error("rmats运行出错!")

    def run(self):
        """
        运行
         :return:
        """
        super(RmatsTool, self).run()
        self.run_rmats()
        self.end()
