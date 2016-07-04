#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.qc.fastq_stat import fastq_qual_stat
import glob
import os


class DrawFastqInfoAgent(Agent):
    """
    DrawFastqInfo:用于做fastq序列质量统计的工具
    version 1.0
    author: qindanhua
    """

    def __init__(self, parent):
        super(DrawFastqInfoAgent, self).__init__(parent)
        options = [
            {"name": "fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"}  # 输入文件fastq序列
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
            raise OptionError("请选择序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
            # ["./fastq_stat.xls", "xls", "fastq信息统计表"]
        ])
        super(DrawFastqInfoAgent, self).end()


class DrawFastqInfoTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(DrawFastqInfoTool, self).__init__(config)
        self.fastxtoolkit_path = 'fastxtoolkit/bin/'
        self.fastq_name = self.option("fastq").prop['path'].split("/")[-1]

    def fastq_quality_stats(self, fastq, outfile):
        fastq_name = fastq.split("/")[-1]
        fastq_name = fastq_name.lower()
        cmd = self.fastxtoolkit_path + 'fastx_quality_stats -i {} -o {}'.format(fastq, outfile)
        self.logger.info(cmd)
        self.logger.info("开始运行fastx_quality_stats")
        command = self.add_command("{}_quality_stats".format(fastq_name), cmd)
        command.run()
        return command

    def multi_fastq_quality_stats(self):
        commands = []
        file_dir = self.option("fastq").prop["path"]
        for f in os.listdir(file_dir):
            if f == "list.txt":
                pass
            else:
                file_path = os.path.join(file_dir, f)
                command = self.fastq_quality_stats(file_path, f + "_qual_stat")
                commands.append(command)
        return commands

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        file_path = glob.glob(r"*qual_stat*")
        print(file_path)
        for f in file_path:
            fastq_qual_stat(f)
        file_path = glob.glob(r"*qual_stat*")
        for f in file_path:
            output_dir = os.path.join(self.output_dir, f)
            if os.path.exists(output_dir):
                os.remove(output_dir)
                os.link(os.path.join(self.work_dir, f), output_dir)
            else:
                os.link(os.path.join(self.work_dir, f), output_dir)
        # os.link(self.work_dir+'/qual.stat', self.output_dir+'/{}_qual.stat'.format(self.fastq_name))
        # os.link(self.work_dir+'/qual.stat.base', self.output_dir+'/{}_qual.stat.base'.format(self.fastq_name))
        # os.link(self.work_dir+'/qual.stat.err', self.output_dir+'/{}_qual.stat.err'.format(self.fastq_name))
        # os.link(self.work_dir+'/qual.stat.qual', self.output_dir+'/{}_qual.stat.qaul'.format(self.fastq_name))

    def run(self):
        """
        运行
        """
        super(DrawFastqInfoTool, self).run()
        if self.option("fastq").format == "sequence.fastq":
            fq_path = self.option("fastq").prop['path']
            command = self.fastq_quality_stats(fq_path, "qual_stat")
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("运行{}完成")
                fastq_qual_stat("qual_stat")
            else:
                self.set_error("运行{}运行出错!")
                return False
        elif self.option("fastq").format == "sequence.fastq_dir":
            commands = self.multi_fastq_quality_stats()
            self.wait()
            for cmd in commands:
                self.logger.info(cmd)
                if cmd.return_code == 0:
                    self.logger.info("运行{}完成")
                else:
                    self.set_error("运行{}运行出错!")
                    return False
        self.set_output()
        self.end()
