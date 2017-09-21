# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify:2017.08.23

from biocluster.module import Module
import os
import shutil
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fasta_dir import FastaDirFile


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
            # 去宿主结果文件夹，内涵各样平fq文件和对应list文件
        ]
        self.add_option(options)
        self.bwa = self.add_tool("align.bwa")
        self.extract_fastq = self.add_tool("sequence.extract_fastq_by_sam")

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

    def run_bwa(self):
        if self.option("ref_database") != "":
            self.bwa.set_options({
                "ref_database": self.option("ref_database"),
                "fq_type": self.option('fq_type'),
                "fastq_dir": self.option("fastq_dir"),
                "head": self.option("head")
            })
        else:
            #ref_undefined = os.path.join(self.option("ref_undefined").prop["path"], "*.fasta")
            #all_ref_undefined = os.path.join(self.option("ref_undefined").prop["path"],
             #                                "ref_undefined/ref_undefined.fasta")
            #os.system('mkdir -p '+self.option("ref_undefined").prop["path"] + '/ref_undefined')
            #os.system('cat ' + ref_undefined + ' >' + all_ref_undefined)
            #all_ref_undefined_path = os.path.join(self.option("ref_undefined").prop["path"], "ref_undefined")
            self.bwa.set_options({
                "ref_undefined": self.option("ref_undefined"),
                "fq_type": self.option('fq_type'),
                "fastq_dir": self.option("fastq_dir"),
            })
        self.bwa.on('end',  self.run_extract_fastq)
        self.bwa.run()

    def run_extract_fastq(self):
        self.extract_fastq.set_options({
            "fq_type": self.option('fq_type'),
            "sam": self.bwa.option('sam'),
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
        self.linkdir(self.extract_fastq.option("reasult_dir").prop['path'], self.output_dir)  #modified by guhaidong 20170918
        # self.option("result_fq_dir", self.output_dir)  # modified by guhaidong 20170918
        self.end()

    def run(self):
        super(BwaRemoveHostModule, self).run()
        self.on_rely([self.bwa, self.extract_fastq], self.set_output)
        self.run_bwa()

