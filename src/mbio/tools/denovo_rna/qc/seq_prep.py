#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class SeqPrepAgent(Agent):
    """
    seqprep:用于对PE序列去接头的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(SeqPrepAgent, self).__init__(parent)
        options = [
            {"name": "fastq_r", "type": "infile", "format": "sequence.fastq"},  # 输入文件PE的右端序列
            {"name": "fastq_l", "type": "infile", "format": "sequence.fastq"},  # PE的左端序列
            {"name": "seqprep_r", "type": "outfile", "format": "sequence.fastq"},  # PE的右端输出结果
            {"name": "seqprep_l", "type": "outfile", "format": "sequence.fastq"},  # PE的左端输出结果
            {"name": "quality", "type": "int", "default": 13},
            {"name": "length", "type": "int", "default": 20},
            {"name": "adapter_a", "type": "string", "default": "AGATCGGAAGAGCGGTTCAG"},
            {"name": "adapter_b", "type": "string", "default": "AGATCGGAAGAGCGTCGTGT"},
            # {"name": "quality_sys", "type": "int", "default": 33}
        ]
        self.add_option(options)
        self.step.add_steps('seqprep')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.seqprep.start()
        self.step.update()

    def step_end(self):
        self.step.seqprep.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("fastq_r").is_set:
            raise OptionError("请选择PE序列右端文件")
        if not self.option("fastq_l").is_set:
            raise OptionError("请选择PE序列左端文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''


class SeqPrepTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(SeqPrepTool, self).__init__(config)
        self.seqprep_path = 'rna/SeqPrep-master/'

    def seqprep(self):
        fq_r_path = self.option("fastq_r").prop['path']
        fq_l_path = self.option("fastq_l").prop['path']
        cmd = self.seqprep_path + "SeqPrep -f {} -r {} -1 {} -2 {} -q {} -L {} -A {} -B {}".\
            format(fq_r_path, fq_l_path, 'seqprep_r.gz', 'seqprep_l.gz', self.option('quality'),
                   self.option('length'), self.option("adapter_a"), self.option("adapter_b"))
        self.logger.info(cmd)
        self.logger.info("开始运行seqprep")
        command = self.add_command("seqprep", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行seqprep完成")
            self.set_output()
        else:
            self.set_error("运行seqprep运行出错!")
            return False

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        os.link(self.work_dir+'/seqprep_r', self.output_dir+'/seqprep_r')
        self.option('seqprep_r').set_path(self.output_dir+'/seqprep_r')
        os.link(self.work_dir+'/seqprep_l', self.output_dir+'/seqprep_l')
        self.option('seqprep_l').set_path(self.output_dir+'/seqprep_l')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(SeqPrepTool, self).run()
        self.seqprep()
        self.end()
