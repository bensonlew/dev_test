# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class FileDenovoAgent(Agent):
    """
    version 1.0
    author: qindanhua
    last_modify: 2016.06.22
    用于在denovoRNA的workflow开始之前对所输入的文件进行详细的内容检测
    """

    def __init__(self, parent):
        super(FileDenovoAgent, self).__init__(parent)
        options = [
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"}  # PE OR SE

        ]
        self.add_option(options)
        self.step.add_steps("file_check")
        self.on('start', self.start_file_check)
        self.on('end', self.end_file_check)

    def start_file_check(self):
        self.step.file_check.start()
        self.step.update()

    def end_file_check(self):
        self.step.file_check.finish()
        self.step.update()

    def check_option(self):
        if not self.option('fastq_dir').is_set:
            raise OptionError("必须输入in_fastq参数")
        # self.option('fastq_dir').get_info()
        if not self.option('fastq_dir').prop['has_list_file']:
            raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class FileDenovoTool(Tool):
    """

    """
    def __init__(self, config):
        super(FileDenovoTool, self).__init__(config)

    def check_fastq(self):
        self.logger.info("正在检测fastq_dir文件")
        # self.option('fastq_dir').get_info()
        if not self.option("fastq_dir").prop["has_list_file"]:
            raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')
        sample = self.option("fastq_dir").prop["samples"]
        self.logger.info(sample)
        col_num = self.get_list_info()
        if self.option("fq_type") in ["PE"] and col_num != 3:
            raise OptionError("PE文件夹的list应该包含三行信息，文件名-样本名-左端OR右端")
        # elif self.option("fq_type") in ["SE"] and col_num != :
        #     raise OptionError("PE文件夹的list应该包含三行信息，文件名-样本名-左端OR右端")

    def get_list_info(self):
        list_path = self.option("fastq_dir").prop["path"] + "/list.txt"
        with open(list_path, "r") as l:
            col_num = len(l.readline().strip().split())
        return col_num

    def run(self):
        super(FileDenovoTool, self).run()
        self.check_fastq()
        self.end()
