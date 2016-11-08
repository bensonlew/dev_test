# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from __future__ import division
import math
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fastq import FastqFile


class FastqSampleExtractAgent(Agent):
    """
    从fastq或者fastq文件夹里提取样本的信息
    """
    def __init__(self, parent):
        super(FastqSampleExtractAgent, self).__init__(parent)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},
            {"name": "file_sample_list", "type": "outfile", "format": "sequence.file_sample"}
        ]
        self.add_option(options)
        self.step.add_steps("sample_extract")
        self.on('start', self.start_sample_extract)
        self.on("end", self.end_sample_extract)

    def start_sample_extract(self):
        self.step.sample_extract.start()
        self.step.update()

    def end_sample_extract(self):
        self.step.sample_extract.finish()
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


class FastqSampleExtractTool(Tool):
    def __init__(self, config):
        super(FastqSampleExtractTool, self).__init__(config)

    def create_file_sample(self):
        path = os.path.join(self.output_dir, "list.txt")
        with open(path, 'wb') as w:
            if self.get_option_object("in_fastq").format == 'sequence.fastq_dir':
                if self.option("in_fastq").prop["has_list_file"]:
                    for file_ in self.option("in_fastq").prop["file_sample"]:
                        w.write("{}\t{}\n".format(file_, self.option("in_fastq").prop["file_sample"][file_]))
                else:
                    for file_ in self.option("in_fastq").prop["file_sample"]:
                        new_file = FastqFile()
                        new_file.set_path(file_)
                        new_file.check_content()
                        for sp in new_file.prop["samples"]:
                            w.write("{}\t{}\n".format(file_, sp))
            else:
                if self.get_option_object('in_fastq').format == 'sequence.fastq':
                    self.option("in_fastq").check_content()
                    for sp in self.option('in_fastq').prop["samples"]:
                        w.write("{}\t{}\n".format(self.option("in_fastq").prop["path"], sp))
        self.option("file_sample_list").set_path(path)

    def run(self):
        super(FastqSampleExtractTool, self).run()
        self.create_file_sample()
        self.end()
