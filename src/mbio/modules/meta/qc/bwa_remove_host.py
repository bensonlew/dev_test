# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify:2017.08.23

from biocluster.module import Module
import os
import shutil
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fasta_dir import FastaDirFile
from mbio.files.sequence.file_sample import FileSampleFile


class BwaRemoveHostModule(Module):
    def __init__(self, work_id):
        super(BwaRemoveHostModule, self).__init__(work_id)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},
            # 输入质控后的fastq文件夹其中包含list文件
            {"name": "fq_type", "type": "string", "default": "PSE"},  # fq类型，PE、SE、PSE（即PE+SE，单端加双端）
            {"name": "ref_database", "type": "string", "default": ""},  # 宿主参考序列库中对应的物种名，eg：E.coli ,B.taurus
            {"name": "ref_undefined", "type": "infile", "format": "sequence.fasta_dir"},
            # 未定义的宿主序列所在文件夹，多个宿主cat到一个文件，并作为tool:align.bwa的输入文件
            {"name": "head", "type": "string", "default": ""},  # 设置结果头文件
            {"name": "result_fq_dir", "type": "outfile", "format": "sequence.fastq_dir"},
            # 去宿主结果文件夹，内含各样品的fq文件和对应list文件
        ]
        self.add_option(options)
        self.extract_fastq = self.add_tool("sequence.extract_fastq_by_sam")
        self.tools = []

    def check_options(self):
        """
        检查参数
        """
        if self.option("ref_database") == "" and not self.option("ref_undefined").is_set:
            raise OptionError("请传入参考序列")
        if self.option("ref_database") != "" and self.option("ref_undefined").is_set:
            raise OptionError("不能同时提供数据库和未定义的参考序列")
        if not self.option("fastq_dir").is_set:
            raise OptionError("请输入fastq序列文件夹")
        if self.option("fastq_dir").is_set and not os.path.exists(self.option("fastq_dir").prop["path"] + "/list.txt"):
            raise OptionError("fastq序列文件夹需还有list文件")
        if self.option('fq_type') not in ['PE', 'SE', 'PSE']:
            raise OptionError("请说明序列类型，PE or SE or 'PSE'?")
        return True

    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples

    def run_bwa(self):
        self.samples = self.get_list()
        reslut_path = os.path.join(self.work_dir, "sam_dir")
        if not os.path.exists(reslut_path):
            os.mkdir(reslut_path)
        sam_list_path = os.path.join(reslut_path, "list.txt")
        with open(sam_list_path, "wb") as w:
            for f in self.samples:
                bwa_tool = self.add_tool("align.bwa")
                bwa_s_tool = self.add_tool("align.bwa")
                if self.option("fq_type") in ["PE", "PSE"]:
                    w.write(f + ".sam\t" + f + "\tpe\n")
                    fq_l = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["l"])
                    fq_r = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["r"])
                    if self.option("ref_database") != "":
                        bwa_tool.set_options({
                            "ref_database": self.option("ref_database"),
                            "fq_type": "PE",
                            "fastq_r": fq_r,
                            "fastq_l": fq_l,
                            "result_path": reslut_path,
                            "head": self.option("head")
                        })
                    else:
                        bwa_tool.set_options({
                            "ref_undefined": self.option("ref_undefined"),
                            "fq_type": "PE",
                            "fastq_r": fq_r,
                            "fastq_l": fq_l,
                            "result_path": reslut_path,
                            "head": self.option("head")
                        })
                    self.tools.append(bwa_tool)
                if self.option("fq_type") in ["SE", "PSE"]:
                    w.write(f + "_s.sam\t" + f + "\tse\n")
                    fq_s = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["s"])
                    if self.option("ref_database") != "":
                        bwa_s_tool.set_options({
                            "ref_database": self.option("ref_database"),
                            "fq_type": "SE",
                            "fastq_s": fq_s,
                            "result_path": reslut_path,
                            "head": self.option("head")
                        })
                    else:
                        bwa_s_tool.set_options({
                            "ref_undefined": self.option("ref_undefined"),
                            "fq_type": "SE",
                            "fastq_s": fq_s,
                            "result_path": reslut_path,
                            "head": self.option("head")
                        })
                    self.tools.append(bwa_s_tool)
        if len(self.tools) > 1:
            self.on_rely(self.tools, self.run_extract_fastq)
        else:
            self.tools[0].on('end', self.run_extract_fastq)
        for tool in self.tools:
            self.logger.info(tool)
            tool.run()

    def run_extract_fastq(self):
        self.extract_fastq.set_options({
            "fq_type": self.option('fq_type'),
            "sam": os.path.join(self.work_dir, "sam_dir"),
        })
        # self.extract_fastq.on('end', self.set_output, 'extract_fastq')  # modified by guhaidong 20170918
        self.extract_fastq.run()

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.work_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                if os.path.isfile(newfile):
                    os.remove(newfile)
                else:
                    os.system('rm -r %s' % newfile)
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            elif os.path.isdir(oldfiles[i]):
                oldfile_basename = os.path.basename(oldfiles[i])
                self.linkdir(oldfiles[i], os.path.join(newdir, oldfile_basename))

    def set_output(self):
        self.option("result_fq_dir", self.extract_fastq.option("reasult_dir"))
        self.linkdir(self.extract_fastq.option("reasult_dir").prop['path'],
                     self.output_dir)  # modified by guhaidong 20170918
        self.end()

    def run(self):
        super(BwaRemoveHostModule, self).run()
        self.on_rely(self.extract_fastq, self.set_output)
        self.run_bwa()
