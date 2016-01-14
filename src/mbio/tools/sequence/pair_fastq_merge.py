# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import subprocess


class PairFastqMergeAgent(Agent):
    """
    调用pear软件将pair_fastq文件根据overlap的关系将read1和read2拼接在一起
    version v1
    author：qiuping
    last_modify:2015.01.11
    """
    def __init__(self, parent):
        super(PairFastqMergeAgent, self).__init__(parent)
        options = [
            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"},
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"}
        ]
        self.add_option(options)
        self.step.add_steps("pair_merge")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.pair_merge.start()
        self.step.update()

    def stepfinish(self):
        self.step.pair_merge.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数设置
        :return:
        """
        if not self.option('fastq_input1'):
            raise OptionError("必须设置输入的fastq1文件")
        if not self.option('fastq_input2'):
            raise OptionError("必须设置输入的fastq2文件")

    def set_resource(self):
        """
        设置所需资源
        :return:
        """
        self._cpu = 10
        self._memory = ''


class PairFastqMergeTool(Tool):
    def __init__(self, config):
        super(PairFastqMergeTool, self).__init__(config)
        self._version = "v1"
        self.pear_path = "/mnt/ilustre/users/sanger/app/pear/bin/"

    def run(self):
        """
        运行
        :return:
        """
        super(PairFastqMergeTool, self).run()
        self.merge()
        self.set_merge_output()
        self.end()

    def merge(self):
        """
        调用pear软件进行merge
        :return:
        """
        self.logger.info("merge_cmd开始运行")
        cmd = self.pear_path + "pear -f %s -r %s -o merge" % (self.option('fastq_input1').prop["path"],
                                                              self.option('fastq_input2').prop["path"])
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("merge_cmd运行完成")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("merge运行出错")
            return False

    def set_merge_output(self):
        """
        将结果文件链接至output
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        os.link(self.work_dir + '/merge.assembled.fastq', self.output_dir + '/merge.assembled.fastq')
