# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from mbio.files.sequence.fasta import FastaFile
from biocluster.core.exceptions import OptionError


class SamplesInfoAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.06
    """
    def __init__(self, parent):
        super(SamplesInfoAgent, self).__init__(parent)
        options = [
            {"name": "fasta_path", "type": "infile", "format": "sequence.fasta_dir"}]  # 输入文件夹
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("fasta_path").is_set:
            raise OptionError("参数fasta_path不能为空")
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class SamplesInfoTool(Tool):
    def __init__(self, config):
        super(SamplesInfoTool, self).__init__(config)
        self._version = 1.0

    def create_table(self):
        """
        生成samples_info表
        """
        self.logger.info('生成fasta文件夹')
        output_dir = os.path.join(self.work_dir, 'output', 'fasta')
        self.option('fasta_path').get_full_info(output_dir)
        self.logger.info('成功生成fasta文件夹,开始统计样本信息')
        file_name = os.path.join(output_dir, self.id + ".samples_info")
        with open(file_name, "w") as f:
            head = ["sample", "reads", "bases", "avg", "min", "max"]
            f.write("\t".join(head) + "\n")
            for fasta in self.option("fasta_path").prop["fasta_fullname"]:
                fastafile = FastaFile()
                fastafile.set_path(fasta)
                if fastafile.check():
                    info_ = list()
                    info_.append(fastafile.prop["sample_name"])
                    info_.append(fastafile.prop["seq_number"])
                    info_.append(fastafile.prop["bases"])
                    avg = int(fastafile.prop["bases"]) / int(fastafile.prop["seq_number"])
                    avg = str(avg)
                    info_.append(avg)
                    info_.append(fastafile.prop["shortest"])
                    info_.append(fastafile.prop["longest"])
                    f.write("\t".join(info_) + "\n")
        self.logger.info('样本信息统计完毕！')

    def run(self):
        """
        运行
        """
        super(SamplesInfoTool, self).run()
        self.create_table()
        self.logger.info("退出样本信息统计模块")
        self.end()
