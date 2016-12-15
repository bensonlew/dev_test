#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "shijin"

import os
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile
import glob

class SampleExtractModule(Module):
    """
    version 1.0
    author: sj
    last_modify: 2016.09.13
    """
    def __init__(self, work_id):
        super(SampleExtractModule, self).__init__(work_id)
        options = [
            {"name": "in_fastq", "type": "infile", "format":"sequence.fastq,sequence.fastq_dir"},
            {"name": "file_sample_list", "type": "outfile", "format": "sequence.info_txt"}
        ]
        self.add_option(options)
        self.samples = []
        self.tools = []
        self.info_path = []
        
    def check_options(self):
        """
        ¼ì²é²ÎÊý
        """
        if self.option("in_fastq").format == "sequence.fastq_dir":
            # self.option("in_fastq").get_info()
            self.samples = self.option("in_fastq").prop["fastq_basename"]
            # self.logger.info(str(lx))
        return True
        
    def fastq_run(self):
        opts = {
            "in_fastq" : self.option("in_fastq")
        }
        run_tool = self.add_tool("meta.fastq_sample_extract")
        run_tool.set_options(opts)
        run_tool.on("end",self.end)
        run_tool.run()
        
    def fastq_dir_run(self):
        for f in self.samples:
            fq_path = self.option("in_fastq").prop["path"] + "/" + f
            opts = {
                "in_fastq" : fq_path
            }
            run_tool = self.add_tool("meta.fastq_sample_extract")
            run_tool.set_options(opts)
            self.tools.append(run_tool)
        self.on_rely(self.tools, self.paste)
        for tool in self.tools:
            tool.run()
        
    def run(self):
        if self.option("in_fastq").format == "sequence.fastq":
            self.fastq_run()
        else:
            self.fastq_dir_run()
        super(SampleExtractModule,self).run()
        
    def paste(self):
        # self.logger.info(str(self.work_dir))
        for dir in os.listdir(self.work_dir):
            if dir.startswith("FastqSampleExtract"):
                dir_path = os.path.join(self.work_dir,dir + "/" + "info.txt")
                self.info_path.append(dir_path)
        #self.logger.info(str(self.info_path))
        list_path = os.path.join(self.work_dir,"info.txt")
        with open(list_path,"a") as w:
            w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
            for path in self.info_path:
                with open(path,"r") as r:
                    r.readline()
                    info_part = r.read()
                    w.write(info_part)
        self.end()
        
    def end(self):
        if self.option("in_fastq").format == "sequence.fastq":
            self.option("file_sample_list").set_path(self.work_dir + "/FastqSampleExtract/info.txt")
        else:
            self.option("file_sample_list").set_path(self.work_dir + "/info.txt")
        super(SampleExtractModule, self).end()
