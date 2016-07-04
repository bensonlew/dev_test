#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


class FastxClipperAgent(Agent):
    """
    seqprep:用于对SE序列做去接头的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(FastxClipperAgent, self).__init__(parent)
        options = [
            {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # 输入文件SE序列
            {"name": "clip_s", "type": "outfile", "format": "sequence.fastq"},  # SE去接头输出结果
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "length", "type": "int", "default": 30},
            {"name": "adapter", "type": "string", "default": 'AGATCGGAAGAGCACACGTC'}
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
        if not self.option("fastq_dir").is_set or self.option("fastq_s").is_set:
            raise OptionError("请传入PE序列文件或者文件夹")

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
        cmd = self.fastxtoolkit_path + 'fastx_clipper -i {} -a {} -Q 35 -v -l {} -o clip_s.fastq'.\
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

    def multi_fastxclipper(self):
        fq_dir = self.option("fastq_dir").prop["path"]
        commands = []
        for f in os.listdir(fq_dir):
            if f == "list.txt":
                pass
            else:
                fq_s_path = os.path.join(fq_dir, f)
                cmd = self.fastxtoolkit_path + 'fastx_clipper -i {} -a {} -Q 35 -v -l {} -o {}_clip_s.fastq'.\
                    format(fq_s_path, self.option('adapter'), self.option('length'), f)
                self.logger.info(cmd)
                self.logger.info("开始运行fastx_clipper")
                command = self.add_command("fastx_clipper_{}".format(f), cmd)
                command.run()
                commands.append(command)
        return commands

    def write_list(self):
        fq_list = os.path.join(self.option("fastq_dir"), "list.txt")
        output_list = os.path.join(self.output_dir, "list.txt")
        output_files = os.listdir(self.output_dir)
        with open(fq_list, "rb") as f, open(output_list, "wb") as w:
            write_line = ""
            for line in f:
                line = line.strip().split()
                for f in output_files:
                    if line[0] in f:
                        write_line = "{}\t{}\n".format(f, line[1])
            self.logger.info(write_line)
            w.write(write_line)

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        file_path = glob.glob(r"*_clip_s*")
        print(file_path)
        for f in file_path:
            output_dir = os.path.join(self.output_dir, f)
            if os.path.exists(output_dir):
                os.remove(output_dir)
                os.link(os.path.join(self.work_dir, f), output_dir)
            else:
                os.link(os.path.join(self.work_dir, f), output_dir)
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(FastxClipperTool, self).run()
        if self.option("fastq_dir").is_set:
            commands = self.multi_fastxclipper()
            self.wait()
            for cmd in commands:
                if cmd.return_code == 0:
                    self.logger.info("运行fastx_clipper完成")
                else:
                    self.set_error("运行fastx_clipper运行出错!")
                    return False
            self.set_output()
            self.write_list()
        else:
            self.fastxclipper()
            self.set_output()
        self.end()
