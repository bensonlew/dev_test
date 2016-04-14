#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class FastxClipperAgent(Agent):
    """
    seqprep:用于对SE序列做去接头的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(FastxClipperAgent, self).__init__(parent)
        options = [
            {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # 输入文件PE的右端序列
            {"name": "clip_s", "type": "outfile", "format": "sequence.fastq"},  # SE输出结果
            {"name": "length", "type": "int", "default": 5},
            {"name": "adapter", "type": "string", "default": 'CCTTAAGG'}
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
        if not self.option("fastq_s").is_set:
            raise OptionError("请选择SE序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''


class FastxClipperTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(FastxClipperTool, self).__init__(config)
        self.fastxtoolkit_path = 'fastxtoolkit/bin/'

    def fastxclipper(self):
        fq_s_path = self.option("fastq_s").prop['path']
        cmd = self.fastxtoolkit_path + 'fastx_clipper -i {} -a {} -l {} -o clip_s.fastq'.\
            format(fq_s_path, self.option('adapter'), self.option('length'))
        self.logger.info(cmd)
        self.logger.info("开始运行fastx_clipper")
        command = self.add_command("fastx_clipper", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行fastx_clipper完成")
            self.set_output()
        else:
            self.set_error("运行fastx_clipper运行出错!")
            return False

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        os.link(self.work_dir+'/clip_s.fastq', self.output_dir+'/clip_s.fastq')
        self.option('clip_s').set_path(self.output_dir+'/clip_s.fastq')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(FastxClipperTool, self).run()
        self.fastxclipper()
        self.end()
