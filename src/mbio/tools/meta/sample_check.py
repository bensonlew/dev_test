# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from __future__ import division
import math
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fastq import FastqFile
from mbio.files.sequence.fastq_dir import FastqDirFile
import re
import shutil

class SampleCheckAgent(Agent):
    """
    根据前端信息，修改fastq文件与fastq文件夹，以与sample列表对应
    """
    def __init__(self, parent):
        super(SampleCheckAgent, self).__init__(parent)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},
            {"name": "file_list", "type": "string"},
            {"name": "in_fastq_modified","type": "outfile", "format": "sequence.fastq"},
            {"name": "fastq_dir_modified","type": "outfile", "format": "sequence.fastq_dir"}
        ]
        self.add_option(options)
        self.step.add_steps("sample_check")
        self.on('start', self.start_sample_check)
        self.on("end", self.end_sample_check)

    def start_sample_check(self):
        self.step.sample_check.start()
        self.step.update()

    def end_sample_check(self):
        self.step.sample_check.finish()
        self.step.update()

    def check_options(self):
        if not self.option("in_fastq").is_set:
            raise OptionError("参数in_fastq不能为空")
        

    def set_resource(self):
        self._cpu = 2
        if self.get_option_object("in_fastq").format == 'sequence.fastq_dir':
            self._memory = "2G"
        if self.get_option_object("in_fastq").format == 'sequence.fastq':
            total = os.path.getsize(self.option("in_fastq").prop["path"])
            total = total / (1024 * 1024 * 1024)
            total = total * 2
            total = math.ceil(total)
            self._memory = '{}G'.format(int(total))


class SampleCheckTool(Tool):
    def __init__(self, config):
        super(SampleCheckTool, self).__init__(config)
        self._file_list = eval(self.option("file_list"))

    def create_new_fastq(self):
        self.logger.info("开始对fastq文件进行编辑")
        self.logger.info(self._file_list)
        path = os.path.join(self.work_dir, "new.fastq")
        with open(path,"w") as w:
            with open(self.option("in_fastq").prop["path"],"r") as f:
                # self.option("in_fastq").check_content()
                i = -1
                for line in f:
                    m = re.match("@(.+)(_\d+.+)",line)
                    if m:
                        old_sample_name = m.group(1)
                        other_things = m.group(2)
                        old_lst = self._file_list.keys()
                        new_lst = [x.split("::")[1] for x in old_lst]
                        file_name = old_lst[0].split("::")[0]
                        if old_sample_name in new_lst:
                            new_sample_name = self._file_list[file_name + "::" + old_sample_name][0]
                            w.write("@" + new_sample_name + other_things + "\n")
                            line2 = f.next()
                            w.write(line2)
                            line3 = f.next()
                            w.write(line3)
                            line4 = f.next()
                            w.write(line4)
        fastq = FastqFile()
        fastq.set_path(path)
        fastq.check_content()
        self.logger.info("新fastq文件编辑完成")
                
                
    def create_new_fastqdir(self):
        self.logger.info("开始对fastq文件夹进行编辑")
        self.option("in_fastq").get_info()
        self.logger.info(self._file_list)
        os.mkdir(self.work_dir + "/fq_dir")
        path = os.path.join(self.work_dir, "fq_dir")
        if self.option("in_fastq").has_list_file:
            self.logger.info("文件夹中含有list.txt文件")
            list_path = os.path.join(path,"list.txt")
            with open(list_path,"w") as w:
                for item in self._file_list.keys():
                    old_path = os.path.join(self.option("in_fastq").prop["path"],item.split("::")[0])
                    old_sample_name = item.split("::")[1]
                    new_sample_name = self._file_list[item][0]
                    new_path = os.path.join(path,new_sample_name + ".fq")
                    if os.path.exists(new_path):
                        raise OptionError("样本名称重复")
                    # shutil.copyfile(old_path,new_path)
                    with open(new_path,"a") as a:
                        with open(old_path,"r") as r:
                            for line in r:
                                m = re.match("@(.+)(_\d+.+)",line)
                                if m:
                                    if old_sample_name == m.group(1):
                                        other_things = m.group(2)
                                        other_things = re.sub("_","",other_things,1)
                                        a.write("@" + other_things + "\n")
                                        line2 = r.next()
                                        a.write(line2)
                                        line3 = r.next()
                                        a.write(line3)
                                        line4 = r.next()
                                        a.write(line4)
                    w.write(new_path + "\t" + new_sample_name + "\n")
        else:
            list_path = os.path.join(path,"list.txt")
            self.logger.info("文件夹中不含有list.txt文件")
            new_path_list = []
            with open(list_path,"w") as w:
                for item in self._file_list.keys():
                    old_path = os.path.join(self.option("in_fastq").prop["path"],item.split("::")[0])
                    old_sample_name = item.split("::")[1]
                    new_sample_name = self._file_list[item][0]
                    new_path = os.path.join(path,new_sample_name + ".fq")
                    with open(new_path,"a") as a:
                        with open(old_path,"r") as r:
                            for line in r:
                                m = re.match("@(.+)(_\d+.+)",line)
                                if m:
                                    if old_sample_name == m.group(1):
                                        other_things = m.group(2)
                                        other_things = re.sub("_","",other_things,1)
                                        a.write("@" + other_things + "\n")
                                        line2 = r.next()
                                        a.write(line2)
                                        line3 = r.next()
                                        a.write(line3)
                                        line4 = r.next()
                                        a.write(line4)
                    if new_path not in new_path_list:
                        new_path_list.append(new_path)
                        w.write(new_path + "\t" + new_sample_name + "\n")

    def run(self):
        super(SampleCheckTool, self).run()
        self.logger.info(self.get_option_object('in_fastq').format)
        if self.get_option_object('in_fastq').format == 'sequence.fastq':
            self.create_new_fastq()
            self.option("in_fastq_modified").set_path(self.work_dir + "/new.fastq")
        if self.get_option_object('in_fastq').format == 'sequence.fastq_dir':
            self.create_new_fastqdir()
            fq_dir = FastqDirFile()
            fq_dir.set_path(self.work_dir + "/fq_dir")
            fq_dir.check()
            self.option("fastq_dir_modified").set_path(self.work_dir + "/fq_dir")
        self.end()
