#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "shijin"

from __future__ import division
import os
import shutil
from biocluster.module import Module


class FastqExtractModule(Module):
    """
    version 1.0
    author: shijin
    last_modify: 2017.2.13
    """
    def __init__(self, work_id):
        super(FastqExtractModule, self).__init__(work_id)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},
        ]
        self.add_option(options)
        self.samples = []
        self.tools = []
        self.info_path = []

    def check_options(self):
        return True

    def fastq_run(self):
        opts = {
            "in_fastq": self.option("in_fastq")
        }
        run_tool = self.add_tool("sample_base.fastq_extract")
        run_tool.set_options(opts)
        self.tools.append(run_tool)
        run_tool.on("end", self.move_to_output)
        run_tool.run()

    def fastq_dir_run(self):
        if self.option("in_fastq").format == "sequence.fastq_dir":
            self.samples = self.option("in_fastq").prop["fastq_basename"]
        for f in self.samples:
            self.logger.info("开始运行样本{}".format(f))
            fq_path = self.option("in_fastq").prop["path"] + "/" + f
            opts = {
                "in_fastq": fq_path
            }
            run_tool = self.add_tool("sample_base.fastq_extract")
            run_tool.set_options(opts)
            self.tools.append(run_tool)
        if len(self.tools) >= 1:
            self.on_rely(self.tools, self.move_to_output)
        elif len(self.tools) == 1:
            self.tools[0].on("end", self.move_to_output)
        for tool in self.tools:
            tool.run()

    def run(self):
        super(FastqExtractModule, self).run()
        if self.option("in_fastq").format == "sequence.fastq":
            self.fastq_run()
        else:
            self.logger.info("输入文件为文件夹，开始进行并行运行")
            self.fastq_dir_run()

    def end(self):
        repaths = []
        repaths.append([".", "", "拆分结果文件目录"])
        for file in os.listdir(self.output_dir):
            file_ = "/" + file
            repaths.append([file_, "fastq", "拆分输出文件" + file])
        updir = self.add_upload_dir(self.output_dir)
        updir.add_relpath_rules(repaths)
        super(FastqExtractModule, self).end()

    def move_to_output(self):
        self.logger.info("进入移动文件过程")
        shutil.rmtree(self.output_dir)
        os.mkdir(self.output_dir)
        for tool in self.tools:
            for file in os.listdir(tool.output_dir + "/fastq"):
                file_path = os.path.join(tool.output_dir + "/fastq", file)
                file_name = os.path.split(file_path)[1]
                if not os.path.exists(self.output_dir + "/" + file_name):
                    os.link(file_path, self.output_dir + "/" + file_name)
                    self.logger.info(self.output_dir + "/" + file_name)
                else:
                    with open(self.output_dir + "/" + file_name, "a") as a:
                        content = open(file_path, "r").read()
                        a.write(content)
            list_path = tool.work_dir + "/info.txt"
            with open(self.work_dir + "/info.txt", "a") as a:
                f = open(list_path, "r")
                f.readline()
                a.write(f.read())
        self.end()
