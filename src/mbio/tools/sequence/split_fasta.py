# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from Bio import SeqIO


class SplitFastaAgent(Agent):
    """
    SplitFasta:将fasta文件按行数拆分
    version 1.0
    author: qiuping
    last_modify: 2016.11.15
    """

    def __init__(self, parent):
        super(SplitFastaAgent, self).__init__(parent)
        options = [
            {"name": "fasta", "type": "infile", "format": "sequence.fasta"},
            {"name": "lines", "type": "int", "default": 100000},  # 序列数
        ]
        self.add_option(options)
        self.step.add_steps('splitfasta')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.splitfasta.start()
        self.step.update()

    def step_end(self):
        self.step.splitfasta.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("fasta").is_set:
            raise OptionError("请传入fasta序列文件")
        if not isinstance(self.option('lines'), int):
            raise OptionError("行数必须为整数")
        # if self.option('lines') % 2:
        #     raise OptionError("行数必须为整除2")
        if self.option('lines') <= 0:
            raise OptionError("行数小于等于0，请重设！")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 2
        self._memory = '3G'


class SplitFastaTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(SplitFastaTool, self).__init__(config)

    def split_fasta(self):
        """
        """
        line = 1
        i = 1
        w = open(self.output_dir + '/fasta_1', 'wb')
        for seq_record in SeqIO.parse(self.option('fasta').prop['path'], "fasta"):
            if line <= self.option('lines'):
                w.write('>{}\n{}\n'.format(seq_record.id, seq_record.seq))
                line += 1
            else:
                i += 1
                w.close()
                line = 1
                w = open(self.output_dir + '/fasta_%s' % i, 'wb')
        w.close()

    def run(self):
        super(SplitFastaTool, self).run()
        self.split_fasta()
        self.end()
