#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class SickleAgent(Agent):
    """
    seqprep:用于对SE/PE序列做质量剪切的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(SickleAgent, self).__init__(parent)
        options = [
                {"name": "fastq_r", "type": "infile", "format": "sequence.fastq"},  # 输入文件PE的右端序列
                {"name": "fastq_l", "type": "infile", "format": "sequence.fastq"},  # PE的左端序列
                {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # SE序列
                {"name": "sickle_r", "type": "outfile", "format": "sequence.fastq"},  # PE的右端输出结果
                {"name": "sickle_l", "type": "outfile", "format": "sequence.fastq"},  # PE的左端输出结果
                {"name": "sickle_un", "type": "outfile", "format": "sequence.fastq"},  # PE的未配对输出结果
                {"name": "sickle_s", "type": "outfile", "format": "sequence.fastq"},  # SE输出结果
                {"name": "quality", "type": "int", "default": 20},
                {"name": "length", "type": "int", "default": 20},
                {"name": "qual_type", "type": "string", "default": 'sanger'},
                # {"name": "no_fiveprime", "type": "int", "default": '-x'},
                {"name": "truncate-n", "type": "bool", "default": True},
                {"name": "fq_type", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps('sickle')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        self.fq_type = ''

    def step_start(self):
        self.step.sickle.start()
        self.step.update()

    def step_end(self):
        self.step.sickle.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """

        if self.option('fq_type') not in ['pe', 'se']:
            raise OptionError("请说明序列类型，PE or SE?")
        if self.option('fq_type') is 'pe':
            if not self.option('fastq_r') and self.option('fastq_l').is_set:
                raise OptionError("请选择输入文件")
        elif self.option('fq_type') is 'se':
            if not self.option("fastq_s").is_set:
                raise OptionError("请选择输入文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''


class SickleTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(SickleTool, self).__init__(config)
        self.sickle_path = 'rna/sickle-master/'

    def sickle(self):
        if self.option('truncate-n') is True:
            truncate_n = '-n'
        else:
            truncate_n = ''
        if self.option('fq_type') is 'pe':
            fq_r_path = self.option("fastq_r").prop['path']
            fq_l_path = self.option("fastq_l").prop['path']
            cmd = self.sickle_path + 'sickle pe -f {} -r {} -o {} -p {} -s {} -t {} -q {} -l {} {}'.\
                format(fq_r_path, fq_l_path, 'sickle_r.fastq', 'sickle_l.fastq', 'sickle_un', self.option('qual_type'),
                       self.option('quality'), self.option('length'), truncate_n)
        else:
            fq_s_path = self.option("fastq_s").prop['path']
            cmd = self.sickle_path + 'sickle se -f {} -o {} -t {} -q {} -l {} {}'.format(
                fq_s_path, 'sickle_s.fastq', self.option('qual_type'), self.option('quality'),
                self.option('length'), truncate_n)
        self.logger.info(cmd)
        self.logger.info("开始运行sickle")
        command = self.add_command("sickle", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行sickle完成")
            self.set_output()
        else:
            self.set_error("运行sickle运行出错!")
            return False

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        if self.option('fq_type') is 'pe':
            os.link(self.work_dir+'/sickle_r', self.output_dir+'/sickle_r')
            self.option('sickle_r').set_path(self.output_dir+'/sickle_r')
            os.link(self.work_dir+'/sickle_l', self.output_dir+'/sickle_l')
            self.option('sickle_l').set_path(self.output_dir+'/sickle_l')
            os.link(self.work_dir+'/sickle_un', self.output_dir+'/sickle_un')
            self.option('sickle_un').set_path(self.output_dir+'/sickle_un')
        else:
            os.link(self.work_dir+'/sickle_s', self.output_dir+'/sickle_s')
            self.option('sickle_s').set_path(self.output_dir+'/sickle_s')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(SickleTool, self).run()
        self.sickle()
        self.end()
