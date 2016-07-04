#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


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
                {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
                {"name": "quality", "type": "int", "default": 30},
                {"name": "length", "type": "int", "default": 30},
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
        if not self.option("fastq_dir").is_set or self.option("fastq_r").is_set or self.option("fastq_s").is_set:
            raise OptionError("请传入fastq序列文件或者文件夹")
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError("请说明序列类型，PE or SE?")
        if self.option('fq_type') in ["PE"] and not self.option("fastq_r").is_set:
            raise OptionError("请传入PE右端序列文件")
        if self.option('fq_type') in ["PE"] and not self.option("fastq_l").is_set:
            raise OptionError("请传入PE左端序列文件")
        if self.option('fq_type') in ["SE"] and not self.option("fastq_s").is_set:
            raise OptionError("请传入SE序列文件")

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
        if self.option('truncate-n') is True:
            self.truncate_n = '-n'
        else:
            self.truncate_n = ''

    def sickle(self):
        if self.option('fq_type') in ["PE"]:
            fq_r_path = self.option("fastq_r").prop['path']
            fq_l_path = self.option("fastq_l").prop['path']
            cmd = self.sickle_path + 'sickle pe -f {} -r {} -o {} -p {} -s {} -t {} -q {} -l {} {}'.\
                format(fq_r_path, fq_l_path, 'sickle_r.fastq', 'sickle_l.fastq', 'sickle_un', self.option('qual_type'),
                       self.option('quality'), self.option('length'), self.truncate_n)
        else:
            fq_s_path = self.option("fastq_s").prop['path']
            cmd = self.sickle_path + 'sickle se -f {} -o {} -t {} -q {} -l {} {}'.format(
                fq_s_path, 'sickle_s.fastq', self.option('qual_type'), self.option('quality'),
                self.option('length'), self.truncate_n)
        self.logger.info(cmd)
        self.logger.info("开始运行sickle")
        command = self.add_command("sickle", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行sickle完成")
            # self.set_output()
        else:
            self.set_error("运行sickle运行出错!")
            return False

    def multi_sickle(self):
        fq_dir = self.option("fastq_dir").prop["path"]
        samples = self.get_list()
        self.logger.info(samples)
        commands = []
        if self.option("fq_type") in ["PE"]:
            for sample in samples:
                fq_r_path = os.path.join(fq_dir, samples[sample]["r"])
                fq_l_path = os.path.join(fq_dir, samples[sample]["l"])
                cmd = self.sickle_path + 'sickle pe -f {} -r {} -o {} -p {} -s {} -t {} -q {} -l {} {}'.\
                    format(fq_r_path, fq_l_path, '{}_sickle_r.fastq'.format(sample), '{}_sickle_l.fastq'.format(sample),
                           '{}_sickle_un'.format(sample), self.option('qual_type'), self.option('quality'),
                           self.option('length'), self.truncate_n)
                self.logger.info(cmd)
                self.logger.info("开始运行sickle_{}".format(sample))
                command = self.add_command("sickle_{}".format(sample), cmd)
                command.run()
                commands.append(cmd)
        elif self.option("fq_type") in ["SE"]:
            for sample in samples:
                fq_s_path = os.path.join(fq_dir, samples[sample])
                cmd = self.sickle_path + 'sickle se -f {} -o {} -t {} -q {} -l {} {}'.\
                    format(fq_s_path, '{}_sickle_s.fastq'.format(sample), self.option('qual_type'),
                           self.option('quality'), self.option('length'), self.truncate_n)
                self.logger.info(cmd)
                self.logger.info("开始运行sickle")
                command = self.add_command("sickle_{}".format(sample), cmd)
                command.run()
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
                    write_line = "{}\t{}\t{}\n".format(line[1] + "_sickle_" + line[2] + ".fastq", line[1], line[2])
                    w.write(write_line)
                    if line[1] not in sample:
                        sample[line[1]] = {line[2]: line[0]}
                    else:
                        sample[line[1]][line[2]] = line[0]
                if len(line) == 2:
                    write_line = "{}\t{}\n".format(line[1] + "_sickle_s.fastq", line[1])
                    w.write(write_line)
                    if line[1] not in sample:
                        sample[line[1]] = line[0]
        return sample

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        file_path = glob.glob(r"*_sickle_*")
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
        super(SickleTool, self).run()
        if self.option("fastq_dir").is_set:
            commands = self.multi_sickle()
            self.wait()
            for cmd in commands:
                if cmd.return_code == 0:
                    self.logger.info("运行sickle完成")
                else:
                    self.set_error("运行sickle运行出错!")
                    return False
        else:
            self.sickle()
        self.end()
