# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


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

    def run(self):
        """
        运行
        :return:
        """
        super(PairFastqToFastaTool, self).run()
        self.pair_fastq_to_fasta(self.option('fastq_input1').prop["path"], self.option('fastq_input2').prop["path"], self.option('fq1_to_fasta_id'), self.option('fq2_to_fasta_id'))
        self.end()

    def pair_fastq_to_fasta(self, fq1, fq2, id1, id2):
        """
        将fastq文件转换成fasta文件
        :param fq1:成对fastq文件1
        :param fq2:成对fastq文件2
        :return:
        """
        self.logger.info("开始运行fastq_to_fasta函数")
        n = 1
        with open(fq1, 'r') as r1:
            with open(fq2, 'r') as r2:
                with open(self.output_dir + '/fasta', 'w') as w:
                    file1 = r1.readlines()
                    file2 = r2.readlines()
                    length = len(file1)
                    for i in range(1, length+1):
                        if (i-1) % 4 == 0:
                            if id1 == "none" and id2 == "none":
                                w.write('%s' % file1[i-1].replace('@', '>'))
                                w.write(file1[i])
                                w.write('%s' % file2[i-1].replace('@', '>'))
                                w.write(file2[i])
                            elif id1 != "none" and id2 != "none":
                                w.write('%s%s\n' % (id1, n))
                                w.write(file1[i])
                                w.write('%s%s\n' % (id2, n))
                                w.write(file2[i])
                                n += 1
                            elif id1 != "none" and id2 == "none":
                                w.write('%s%s\n' % (id1, n))
                                w.write(file1[i])
                                w.write('%s' % file2[i-1].replace('@', '>'))
                                w.write(file2[i])
                                n += 1
                            else:
                                w.write('%s' % file1[i-1].replace('@', '>'))
                                w.write(file1[i])
                                w.write('%s%s\n' % (id2, n))
                                w.write(file2[i])
                                n += 1
        self.logger.info("运行fastq_to_fasta函数出错")