#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


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
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "quality", "type": "int", "default": 20},
            {"name": "length", "type": "int", "default": 30},
            {"name": "adapter_a", "type": "string", "default": "AGATCGGAAGAGCACACGTC"},
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
        if not self.option("fastq_dir").is_set or self.option("fastq_r").is_set:
            raise OptionError("请传入PE序列文件或者文件夹")
        if self.option("fastq_r").is_set and not self.option("fastq_l").is_set:
            raise OptionError("缺少PE序列左端文件")
        if self.option("fastq_l").is_set and not self.option("fastq_r").is_set:
            raise OptionError("缺少PE序列右端文件")

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
        if self.option("fastq_r").is_set and self.option("fastq_l").is_set:
            self.sample_name_r = self.option("fastq_r").prop["path"].split("/")[-1]
            self.sample_name_l = self.option("fastq_l").prop["path"].split("/")[-1]

    def seqprep(self):
        fq_r_path = self.option("fastq_r").prop['path']
        fq_l_path = self.option("fastq_l").prop['path']
        cmd = self.seqprep_path + "SeqPrep -f {} -r {} -1 {} -2 {} -q {} -L {} -A {} -B {}".\
            format(fq_l_path, fq_r_path, '{}seqprep_r.gz'.format(self.sample_name_r), '{}seqprep_l.gz'.format(self.sample_name_l), self.option('quality'),
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

    def multi_seqprep(self):
        fq_dir = self.option("fastq_dir").prop["path"]
        samples = self.get_list()
        self.logger.info(samples)
        commands = []
        for sample in samples:
            fq_r_path = os.path.join(fq_dir, samples[sample]["r"])
            fq_l_path = os.path.join(fq_dir, samples[sample]["l"])
            cmd = self.seqprep_path + "SeqPrep -f {} -r {} -1 {} -2 {} -q {} -L {} -A {} -B {}".\
                format(fq_l_path, fq_r_path, '{}_seqprep_l.gz'.format(sample), '{}_seqprep_r.gz'.format(sample),
                       self.option('quality'), self.option('length'), self.option("adapter_a"), self.option("adapter_b"))
            self.logger.info(cmd)
            self.logger.info("开始运行seqprep_{}".format(sample.lower()))
            command = self.add_command("seqprep_{}".format(sample.lower()), cmd)
            command.run()
            commands.append(command)
            # self.logger.info(commands)
        return commands

    def get_list(self):
        list_path = self.option("fastq_dir").prop["path"] + "/list.txt"
        output_list = os.path.join(self.output_dir, "list.txt")
        self.logger.info(list_path)
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        if os.path.exists(list_path):
            self.logger.info(list_path)
        sample = {}
        with open(list_path, "rb") as l, open(output_list, "wb") as w:
            for line in l:
                line = line.strip().split()
                if len(line) == 3:
                    write_line = "{}\t{}\t{}\n".format(line[1] + "_seqprep_" + line[2] + ".gz", line[1], line[2])
                    w.write(write_line)
                    if line[1] not in sample:
                        sample[line[1]] = {line[2]: line[0]}
                    else:
                        sample[line[1]][line[2]] = line[0]
                if len(line) == 2:
                    if line[1] not in sample:
                        sample[line[1]] = line[0]
        return sample

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        file_path = glob.glob(r"*.gz")
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
        super(SeqPrepTool, self).run()
        if self.option("fastq_dir").is_set:
            self.logger.info("where")
            commands = self.multi_seqprep()
            self.wait()
            for cmd in commands:
                self.logger.info(cmd)
                if cmd.return_code == 0:
                    self.logger.info("运行{}完成".format(cmd.name))
                else:
                    self.set_error("运行{}运行出错!".format(cmd.name))
                    return False
            self.set_output()
        else:
            self.seqprep()
            self.set_output()
        self.end()
