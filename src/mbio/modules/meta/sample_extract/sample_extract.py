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
    last_modify: 2016.12.27
    """
    def __init__(self, work_id):
        super(SampleExtractModule, self).__init__(work_id)
        options = [
            {"name": "in_fastq", "type": "infile", "format":"sequence.fastq,sequence.fastq_dir"},
            {"name": "file_sample_list", "type": "outfile", "format": "sequence.info_txt"},
            {"name": "workdir_sample", "type": "string", "default": "null"},
            {"name": "file_list", "type": "string", "default": "null"},
            {"name": "table_id", "type": "string", "default": ""}
        ]
        self.add_option(options)
        self.samples = []
        self.tools = []
        self.info_path = []
        
    def check_options(self):
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
        if self.option("in_fastq").format == "sequence.fastq_dir":
            self.samples = self.option("in_fastq").prop["fastq_basename"]
        for f in self.samples:
            fq_path = self.option("in_fastq").prop["path"] + "/" + f
            opts = {
                "in_fastq" : fq_path
            }
            run_tool = self.add_tool("meta.fastq_sample_extract")
            run_tool.set_options(opts)
            self.tools.append(run_tool)
        if len(self.tools) >= 1:
            self.on_rely(self.tools, self.paste)
        elif len(self.tools) == 1:
            self.tools[0].on("end",self.paste)
        for tool in self.tools:
            #self.logger.info("fastq dir is running at the same time")
            tool.run()
        
    def run(self):
        super(SampleExtractModule,self).run()
        if self.option("file_list") == "null":
            if self.option("in_fastq").format == "sequence.fastq":
                self.fastq_run()
            else:
                self.logger.info("run fastq dir")
                self.fastq_dir_run()
        else:
            workdir_sample = eval(self.option("workdir_sample"))
            tmp_lst = workdir_sample[0][workdir_sample[0].keys()[0]].split("/")
            tmp_lst.pop()
            workdir_sample = "/".join(tmp_lst)
            old_info_path = workdir_sample + "/info.txt"
            if not os.path.exists(old_info_path):
                old_workdir_sample = eval(self.option("workdir_sample"))
                info_path = old_workdir_sample[0][workdir_sample[0].keys()[0]] + "/info.txt"
                os.link(info_path,old_info_path)
            new_info_path = os.path.join(self.work_dir,"info_tmp.txt")
            self.info_rename(old_info_path,new_info_path)
        # super(SampleExtractModule,self).run()
        
    def paste(self):
        # self.logger.info(str(self.work_dir))
        for dir in os.listdir(self.work_dir):
            if dir.startswith("FastqSampleExtract"):
                dir_path = os.path.join(self.work_dir,dir + "/" + "info.txt")
                self.info_path.append(dir_path)
        #self.logger.info(str(self.info_path))
        list_path = os.path.join(self.work_dir,"info.txt")
        self.logger.info("already here")
        with open(list_path,"a") as w:
            w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
            for path in self.info_path:
                with open(path,"r") as r:
                    r.readline()
                    info_part = r.read()
                    w.write(info_part)
        self.end()
        
    def end(self):
        if self.option("file_list") == "null" and self.option("in_fastq").format == "sequence.fastq":
            os.link(self.work_dir + "/FastqSampleExtract/info.txt", self.work_dir + "/info.txt")
        if self.option("file_list") == "null" and self.option("table_id") != "":
            self.logger.info(self.option("table_id"))
            self.set_sample_db()
            self.option("file_sample_list").set_path(Config().WORK_DIR + "/sample_data/" + self.option("table_id") + "/info.txt")
        else:
            self.option("file_sample_list").set_path(self.work_dir + "/info.txt")
        super(SampleExtractModule, self).end()

    def info_rename(self,old_info_path,new_info_path):
        file_list = eval(self.option("file_list"))
        with open(new_info_path,"w") as w:
            with open(old_info_path,"r") as r:
                w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
                r.readline()
                for line in r:
                    line = line.strip()
                    lst = line.split("\t")
                    file_name = os.path.basename(lst[0])
                    sample_name = lst[1]
                    key = file_name + "::" + sample_name
                    if key in file_list.keys():
                        new_tool_lst = lst[2].split("/")
                        new_tool_path = self.work_dir + "/" + new_tool_lst[-1]  
                        self.mv(lst[2],new_tool_path,key)
                        w.write(lst[0] + "\t" +file_list[key][0] + "\t" + new_tool_path + "\t" + lst[3] + "\t" + lst[4] + "\t" + lst[5] + "\t" + lst[6] + "\t" + lst[7] + "\n")
        self.create_info(new_info_path)

    def mv(self,old_path,new_path,key):
        if self.option("file_list") != "null":
            file_list = eval(self.option("file_list"))
            old_name = file_list[key][1]
            new_name = file_list[key][0]
        else:
            old_name = key
            new_name = key
        if not os.path.exists(new_path):
            os.mkdir(new_path)
        output_path = new_path + "/output"
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        output_length_path = output_path + "/length"
        output_fa_path = output_path + "/fa"
        if not os.path.exists(output_length_path):
            os.mkdir(output_length_path)
        if not os.path.exists(output_fa_path):
            os.mkdir(output_fa_path)
        for file in os.listdir(old_path + "/output/length"):
            #self.logger.info(file)
            sample = re.match("(.+)\.length_file",file).group(1)
            #self.logger.info(old_name)
            if sample == old_name:
                old_file = os.path.join(old_path + "/output/length",file)
                new_file = os.path.join(output_length_path,new_name + ".length_file")
                #self.logger.info(old_file)
                #self.logger.info(new_file)
                if os.path.exists(new_file):
                    with open(new_file,"a") as a:
                        with open(old_file,"r") as r:
                            for line in r:
                                a.write(line)
                else:
                    shutil.copy(old_file,new_file)
        for file in os.listdir(old_path + "/output/fa"):
            #self.logger.info(file)
            sample = re.match("(.+)\.fa",file).group(1)
            if sample == old_name:
                old_file = os.path.join(old_path + "/output/fa",file)
                new_file = os.path.join(output_fa_path,new_name + ".fasta")
                if os.path.exists(new_file):
                    with open(new_file,"a") as a:
                        with open(old_file,"r") as r:
                            for line in r:
                                a.write(line)
                else:
                    shutil.copy(old_file,new_file)

    def create_info(self,new_info_path):
        sample_lst = []
        sample_workdir = {}
        sample_reads = {}
        sample_bases = {}
        sample_avg = {}
        sample_min = {}
        sample_max = {}
        if len(eval(self.option("workdir_sample"))) != 1:
            with open(new_info_path,"r") as r:
                with open(self.work_dir + "/info.txt", "w") as w:
                    w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
                    r.readline()
                    for line in r:
                        line = line.strip()
                        lst = line.split("\t")
                        file_name = lst[0]
                        sample_name = lst[1]
                        if sample_name not in sample_lst:
                            sample_lst.append(sample_name)
                            sample_workdir[sample_name] = lst[2]
                            sample_reads[sample_name] =  int(lst[3])
                            sample_bases[sample_name] = int(lst[4])
                            sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                            sample_min[sample_name] = int(lst[6])
                            sample_max[sample_name] = int(lst[7])
                        else:
                            sample_workdir[sample_name] = self.dir_join(sample_workdir[sample_name],lst[2],sample_name)
                            sample_reads[sample_name] +=  int(lst[3])
                            sample_bases[sample_name] += int(lst[4])
                            sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                            sample_min[sample_name] = min(int(lst[6]),sample_min[sample_name])
                            sample_max[sample_name] = max(int(lst[7]),sample_max[sample_name])
                    for sample in sample_lst:
                        w.write("-" + "\t" + sample + "\t" + sample_workdir[sample] + "\t" +str(sample_reads[sample]) + "\t" + str(sample_bases[sample]) + "\t" + str(sample_avg[sample]) + "\t" + str(sample_min[sample])\
                        + "\t" + str(sample_max[sample]) + "\n")
        else:
            with open(new_info_path, "r") as r:
                with open(self.work_dir + "/info.txt", "w") as w:
                    w.write(
                        "#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
                    r.readline()
                    for line in r:
                        line = line.strip()
                        lst = line.split("\t")
                        file_name = lst[0]
                        sample_name = lst[1]
                        if sample_name not in sample_lst:
                            sample_lst.append(sample_name)
                            sample_workdir[sample_name] = lst[2]
                            sample_reads[sample_name] = int(lst[3])
                            sample_bases[sample_name] = int(lst[4])
                            sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                            sample_min[sample_name] = int(lst[6])
                            sample_max[sample_name] = int(lst[7])
                        else:
                            sample_reads[sample_name] += int(lst[3])
                            sample_bases[sample_name] += int(lst[4])
                            sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                            sample_min[sample_name] = min(int(lst[6]), sample_min[sample_name])
                            sample_max[sample_name] = max(int(lst[7]), sample_max[sample_name])
                    for sample in sample_lst:
                        w.write("-" + "\t" + sample + "\t" + sample_workdir[sample] + "\t" + str(
                            sample_reads[sample]) + "\t" + str(sample_bases[sample]) + "\t" + str(
                            sample_avg[sample]) + "\t" + str(sample_min[sample]) \
                                + "\t" + str(sample_max[sample]) + "\n")
                    #self.logger.info("end")
        self.logger.info("end")
        self.end()

    def dir_join(self,fdir,sdir,sample_name):
        self.logger.info(fdir)
        if fdir == sdir:
            return fdir
        with open(fdir + "/output/fa/" + sample_name + ".fasta","a") as a:
            with open(sdir+ "/output/fa/" + sample_name + ".fasta","r") as r:
                for line in r:
                    a.write(line)
        with open(fdir + "/output/length/" + sample_name + ".length_file","a") as a:
            with open(sdir+ "/output/length/" + sample_name + ".length_file","r") as r:
                for line in r:
                    a.write(line)
        return fdir

    def set_sample_db(self):
        os.mkdir(Config().WORK_DIR + "/sample_data/" + self.option("table_id"))
        table_dir = os.path.join(Config().WORK_DIR + "/sample_data", self.option("table_id"))
        new_info_path = os.path.join(table_dir,"info.txt")
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
                    self.mv(lst[2], new_tool_path,sample_name)
                    w.write(lst[0] + "\t" + sample_name + "\t" + new_tool_path + "\t" + lst[3] + "\t" + lst[
                        4] + "\t" + lst[5] + "\t" + lst[6] + "\t" + lst[7] + "\n")
