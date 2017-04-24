#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "shijin"

from __future__ import division
import os,shutil,re
from biocluster.module import Module
from biocluster.config import Config


class SampleExtractModule(Module):
    """
    version 1.0
    author: shijin
    last_modify: 2017.04.11
    """
    def __init__(self, work_id):
        super(SampleExtractModule, self).__init__(work_id)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},
            {"name": "file_list", "type": "string", "default": "null"},
            {"name": "table_id", "type": "string", "default": ""},
            {"name": "info_txt", "type":"infile", "format": "sequence.info_txt"},
            {"name": "file_sample_list", "type": "outfile", "format": "sequence.info_txt"}
        ]
        self.add_option(options)
        self.samples = []
        self.tools = []
        self.info_path = []
        
    def check_options(self):
        return True
        
    def fastq_run(self, fastqfile):
        opts = {
            "in_fastq": fastqfile
        }
        run_tool = self.add_tool("meta.fastq_sample_extract")
        run_tool.set_options(opts)
        run_tool.on("end", self.end)
        run_tool.run()
        
    def fastq_dir_run(self):
        # if self.option("table_id") != "":
        self.samples = self.option("in_fastq").prop["fastq_basename"]
        for f in self.samples:
            fq_path = self.option("in_fastq").prop["path"] + "/" + f
            opts = {
                "in_fastq": fq_path
            }
            run_tool = self.add_tool("meta.fastq_sample_extract")
            run_tool.set_options(opts)
            self.tools.append(run_tool)
        if len(self.tools) >= 1:
            self.on_rely(self.tools, self.paste)
        elif len(self.tools) == 1:
            self.tools[0].on("end", self.paste)
        for tool in self.tools:
            tool.run()
        
    def run(self):
        super(SampleExtractModule, self).run()
        if self.option("in_fastq").format == "sequence.fastq":
            self.fastq_run(self.option("in_fastq"))
        else:
            self.logger.info("run fastq dir")
            self.fastq_dir_run()

    def paste(self):
        for dir in os.listdir(self.work_dir):
            if dir.startswith("FastqSampleExtract"):
                dir_path = os.path.join(self.work_dir, dir + "/" + "info.txt")
                self.info_path.append(dir_path)
        list_path = os.path.join(self.work_dir, "info.txt")
        self.logger.info("already here")
        with open(list_path, "a") as w:
            w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
            for path in self.info_path:
                with open(path, "r") as r:
                    r.readline()
                    info_part = r.read()
                    w.write(info_part)
        self.end()

    def end(self):
        if self.option("file_list") == "null":
            if not os.path.exists(self.work_dir + "/info.txt"):
                os.link(self.work_dir + "/FastqSampleExtract/info.txt", self.work_dir + "/info.txt")
        if self.option("file_list") == "null" and self.option("table_id") != "":
            self.logger.info(self.option("table_id"))
            self.set_sample_db()
            self.option("file_sample_list").set_path(Config().WORK_DIR + "/sample_data/" +
                                                     self.option("table_id") + "/info.txt")
        else:
            self.option("file_sample_list").set_path(self.work_dir + "/info.txt")
        with open(self.option("file_sample_list").prop["path"], "r") as file:
            file.readline()
            try:
                next(file)
            except:
                raise Exception("样本检测没有找到样本，请重新检查文件的改名信息")
        super(SampleExtractModule, self).end()

    def set_sample_db(self):
        os.mkdir(Config().WORK_DIR + "/sample_data/" + self.option("table_id"))
        table_dir = os.path.join(Config().WORK_DIR + "/sample_data", self.option("table_id"))
        new_info_path = os.path.join(table_dir, "info.txt")
        old_info_path = self.work_dir + "/info.txt"
        with open(new_info_path, "w") as w:
            with open(old_info_path, "r") as r:
                w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
                r.readline()
                for line in r:
                    line = line.strip()
                    lst = line.split("\t")
                    sample_name = lst[1]
                    """
                    file_name = os.path.basename(lst[0])
                    sample_name = lst[1]
                    key = file_name + "::" + sample_name
                    if key in file_list.keys():
                    """
                    new_tool_lst = lst[2].split("/")
                    new_tool_path = table_dir + "/" + new_tool_lst[-1]
                    self.mv(lst[2], new_tool_path, sample_name)
                    w.write(lst[0] + "\t" + sample_name + "\t" + new_tool_path + "\t" + lst[3] + "\t" + lst[
                        4] + "\t" + lst[5] + "\t" + lst[6] + "\t" + lst[7] + "\n")
