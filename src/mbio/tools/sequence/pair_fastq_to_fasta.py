# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os


class PairFastqToFastaAgent(Agent):
    """
    将成对fastq文件转换成fasta格式的文件
    version v1
    author：qiuping
    last_modify:2015.01.06
    """
    def __init__(self, parent):
        super(PairFastqToFastaAgent, self).__init__(parent)
        options = [
            {"name": "fastq_input1", "type": "infile", "format": "sequence.fastq"},
            {"name": "fastq_input2", "type": "infile", "format": "sequence.fastq"},
            {"name": "fq1_to_fasta_id", "type": "string", "default": "none"},
            {"name": "fq2_to_fasta_id", "type": "string", "default": "none"}
        ]
        self.add_option(options)
        self.step.add_steps("pair_to_fasta")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.pair_to_fasta.start()
        self.step.update()

    def stepfinish(self):
        self.step.pair_to_fasta.finish()
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


class PairFastqToFastaTool(Tool):
    def __init__(self, config):
        super(PairFastqToFastaTool, self).__init__(config)
        self._version = "v1"
        self.cmd = "program/Anaconda2/bin/"
        self.script = self.config.SOFTWARE_DIR + "/bioinfo/seq/scripts/"

    def run(self):
        """
        运行
        :return:
        """
        super(PairFastqToFastaTool, self).run()
        self.pair_fastq_to_fasta()
        self.end()

    def pair_fastq_to_fasta(self):
        """
        运行pair_fastq_to_fasta.py程序，将成对fastq文件转换成fasta文件
        """
        self.logger.info("开始运行pair_fastq_to_fasta命令")
        cmd = self.cmd + "python %spair_fastq_to_fasta.py -f %s -r %s -o fasta -n %s -m " \
                         "%s" % (self.script, self.option('fastq_input1').prop['path'], self.option('fastq_input2').prop['path'],
                                 self.option('fq1_to_fasta_id'), self.option('fq2_to_fasta_id'))
        self.logger.info(cmd)
        pair_command = self.add_command("pair_fastq_to_fasta_cmd", cmd).run()
        self.wait(pair_command)
        if pair_command.return_code == 0:
            self.logger.info("pair_fastq_to_fasta_cmd运行完成")
        else:
            self.set_error("pair_fastq_to_fasta_cmd运行出错!")
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        os.link(self.work_dir + '/fasta', self.output_dir + '/fasta')
