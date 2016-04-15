#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.qc.fastq_stat import fastq_stat


class DrawFastqInfoAgent(Agent):
    """
    seqprep:用于做fastq序列质量统计的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(DrawFastqInfoAgent, self).__init__(parent)
        options = [
            {"name": "fastq", "type": "infile", "format": "sequence.fastq"}  # 输入文件fastq序列
        ]
        self.add_option(options)
        self.step.add_steps('fastx_clipper')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.fastx_clipper.start()
        self.step.update()

    def step_end(self):
        self.step.fastx_clipper.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("fastq").is_set:
            raise OptionError("请选择SE序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''


class DrawFastqInfoTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(DrawFastqInfoTool, self).__init__(config)
        self.fastxtoolkit_path = 'fastxtoolkit/bin/'

    def fastx_quality_stats(self):
        fq_s_path = self.option("fastq").prop['path']
        cmd = self.fastxtoolkit_path + 'fastx_quality_stats -i {} -o {}'.format(fq_s_path, 'qual.stat')
        self.logger.info(cmd)
        self.logger.info("开始运行fastx_quality_stats")
        command = self.add_command("fastx_quality_stats", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行fastx_quality_stats完成")
            fastq_stat('qual.stat')
            self.set_output()
        else:
            self.set_error("运行fastx_quality_statsr运行出错!")
            return False

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        os.link(self.work_dir+'/qual.stat', self.output_dir+'/qual.stat')
        os.link(self.work_dir+'/qual.stat.base', self.output_dir+'/qual.stat.base')
        os.link(self.work_dir+'/qual.stat.err', self.output_dir+'/qual.stat.err')
        os.link(self.work_dir+'/qual.stat.qaul', self.output_dir+'/qual.stat.qaul')

    def run(self):
        """
        运行
        """
        super(DrawFastqInfoTool, self).run()
        self.fastx_quality_stats()
        self.end()
