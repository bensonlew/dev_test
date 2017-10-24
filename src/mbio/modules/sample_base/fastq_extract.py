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
    last_modify: 2017.9.25
    """
    def __init__(self, work_id):
        super(FastqExtractModule, self).__init__(work_id)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},
            {"name": "output_fq", "type": "outfile", "format": "sequence.fastq_dir"},
            {"name": "output_length", "type": "outfile", "format": "sample.data_dir"},
            {"name": "output_list", "type": "outfile", "format": "sequence.info_txt"}

        ]
        self.add_option(options)
        self.samples = []
        self.tools = []
        self.info_path = []
        self.list_path = os.path.join(self.work_dir, "info.txt")  # 所有序列所有样本信息
        self.final_info_path = os.path.join(self.output_dir, "info.txt")  # 合并样本后的样本信息文件

    def check_options(self):
        return True

    def fastq_run(self):
        opts = {
            "in_fastq": self.option("in_fastq")
        }
        run_tool = self.add_tool("sample_base.fastq_extract")
        run_tool.set_options(opts)
        self.tools.append(run_tool)
        run_tool.on("end", self.combined_info)
        run_tool.run()

    def fastq_dir_run(self):
        # if self.option("table_id") != "":
        self.samples = self.option("in_fastq").prop["fastq_basename"]
        for f in self.samples:
            fq_path = self.option("in_fastq").prop["path"] + "/" + f
            opts = {
                "in_fastq": fq_path
            }
            run_tool = self.add_tool("sample_base.fastq_extract")
            run_tool.set_options(opts)
            self.tools.append(run_tool)
        if len(self.tools) >= 1:
            self.on_rely(self.tools, self.combined_info)
        elif len(self.tools) == 1:
            self.tools[0].on("end", self.combined_info)
        for tool in self.tools:
            tool.run()

    def combined_info(self):
        """
        统计每条序列每个样本的序列信息,并将样本名相同的信息合并
        :return:
        """
        for tool in self.tools:
        # for dir in os.listdir(self.work_dir):
            # if dir.startswith("FastqExtract"):
            dir_path = os.path.join(tool.work_dir, "info.txt")
            self.info_path.append(dir_path)
        with open(self.list_path, "a") as w:
            w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
            for path in self.info_path:
                with open(path, "r") as r:
                    r.readline()
                    info_part = r.read()
                    w.write(info_part)
        self.sample_workdir = self.create_info(self.list_path)  # 合并信息，序列合并
        self.cat_fastas(self.sample_workdir)

    def cat_fastas(self, sample_workdir):  # sample_workdir为字典，键值为样本名，值为列表
        os.mkdir(self.output_dir + "/fastq")
        for sample in sample_workdir.keys():
            path_list = sample_workdir[sample]
            path_unrepeat = []
            length_unrepeat = []
            for path in path_list:
                path = path + "/output/fastq/" + sample + ".fq"
                if path not in path_unrepeat:
                    path_unrepeat.append(path)  # 对列表进行去重，属于同一目录的样本不再进行合并
            if len(path_unrepeat) != 1:
                path = " ".join(path_unrepeat)
                os.system("cat {} > {}/{}.fq".format(path, self.output_dir + '/fastq/', sample))
                # os.system("mv {}/tmp.fa {}".format(self.work_dir, path_unrepeat[0]))
                self.logger.info("来自于不同文件的样本的序列文件{}，合并完成".format(sample))
                self.logger.info(path_unrepeat[0])
            else:
                path = path_unrepeat[0]
                os.link(path, self.output_dir + '/fastq/' + sample + '.fq')
        self.end()
            # for path in path_list:  # 对长度文件进行合并
            #     path = path + "/output/length/" + sample + ".length_file"
            #     if path not in length_unrepeat:
            #         length_unrepeat.append(path)
            # if len(length_unrepeat) != 1:
            #     str = " ".join(length_unrepeat)
            #     os.system("cat {} > {}/tmp.length".format(str, self.work_dir))
            #     os.system("mv {}/tmp.length {}".format(self.work_dir, length_unrepeat[0]))
            #     self.logger.info("来自于不同文件的样本的长度文件{}，合并完成".format(sample))

    # def combined_fq(self):
    #     """
    #     合并序列
    #     :return:
    #     """
    #     self.logger.info("进入移动文件过程")
    #     shutil.rmtree(self.output_dir)
    #     os.mkdir(self.output_dir)
    #     os.mkdir(self.output_dir + "/fastq")
    #     fq_dir = self.output_dir + "/fastq"
    #     os.mkdir(self.output_dir + "/length")
    #     length_dir = self.output_dir + "/length"
    #     for tool in self.tools:
    #         for file in os.listdir(tool.output_dir + "/fastq"):
    #             file_path = os.path.join(tool.output_dir + "/fastq", file)
    #             file_name = os.path.split(file_path)[1]
    #             if not os.path.exists(fq_dir + "/" + file_name):
    #                 os.link(file_path, fq_dir + "/" + file_name)
    #             else:
    #                 with open(fq_dir + "/" + file_name, "a") as a:
    #                     content = open(file_path, "r").read()
    #                     a.write(content)
    #         for files in os.listdir(tool.output_dir + "/length"):
    #             file_path = os.path.join(tool.output_dir + "/length", files)
    #             file_name = os.path.split(file_path)[1]
    #             if not os.path.exists(length_dir + "/" + file_name):
    #                 os.link(file_path, length_dir + "/" + file_name)
    #             else:
    #                 with open(length_dir + "/" + file_name, "a") as a:
    #                     content = open(file_path, "r").read()
    #                     a.write(content)
    #     self.option('output_fq').set_path(self.output_dir + '/fastq')
    #     self.option('output_list').set_path(self.final_info_path)
    #     self.end()

    def run(self):
        # self.length_stat.on("end", self.end)  # change by wzy 20170925, 注释掉
        if self.option("in_fastq").format == "sequence.fastq":
            self.fastq_run()
        else:
            self.logger.info("输入文件为文件夹，开始进行并行运行")
            self.fastq_dir_run()
        super(FastqExtractModule, self).run()

    def end(self):
        # super(FastqExtractModule, self).end()
        self.option("output_fq").set_path(self.output_dir + "/fastq")
        # self.option("output_length").set_path(self.output_dir + "/length")
        self.option("output_list").set_path(self.work_dir + "/info.txt")
        super(FastqExtractModule, self).end()

    def create_info(self, old_info_path):
        sample_lst = []
        sample_workdir = {}  # sample_workdir[sample] = [path1,path2]
        sample_list_path = {}  # sample_list_path[sample] = [fq1,fq2]
        sample_reads = {}
        sample_bases = {}
        sample_avg = {}
        sample_min = {}
        sample_max = {}
        with open(old_info_path, "r") as r:
            with open(self.final_info_path, "w") as w:
                w.write("#file_path\tsample\twork_dir_path\tseq_num\t"
                        "base_num\tmean_length\tmin_length\tmax_length\n")
                r.readline()
                for line in r:
                    line = line.strip()
                    lst = line.split("\t")
                    sample_name = lst[1]
                    if sample_name not in sample_lst:
                        sample_lst.append(sample_name)
                        sample_list_path[sample_name] = [lst[0]]
                        sample_workdir[sample_name] = [lst[2]]
                        sample_reads[sample_name] = int(lst[3])
                        sample_bases[sample_name] = int(lst[4])
                        sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                        sample_min[sample_name] = int(lst[6])
                        sample_max[sample_name] = int(lst[7])
                    else:
                        sample_list_path[sample_name].append(lst[0])
                        sample_workdir[sample_name].append(lst[2])
                        sample_reads[sample_name] += int(lst[3])
                        sample_bases[sample_name] += int(lst[4])
                        sample_avg[sample_name] = int(sample_bases[sample_name]) / int(sample_reads[sample_name])
                        sample_min[sample_name] = min(int(lst[6]), sample_min[sample_name])
                        sample_max[sample_name] = max(int(lst[7]), sample_max[sample_name])
                for sample in sample_lst:
                    file_path = ",".join(sample_list_path[sample])
                    work_dir = ",".join(sample_workdir[sample])
                    w.write(file_path + "\t" + sample + "\t" + work_dir + "\t"
                            + str(sample_reads[sample]) + "\t" + str(sample_bases[sample]) + "\t"
                            + str(sample_avg[sample]) + "\t" + str(sample_min[sample])
                            + "\t" + str(sample_max[sample]) + "\n")
        return sample_workdir
        self.logger.info("样本信息合并成功")

