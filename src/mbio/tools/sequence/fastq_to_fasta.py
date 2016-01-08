# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class FastqToFastaAgent(Agent):
    """
    将fastq文件转换成fasta格式的文件
    version v1
    author：qiuping
    last_modify:2015.01.06
    """
    def __init__(self, parent):
        super(FastqToFastaAgent, self).__init__(parent)
        options = [
            {"name": "fastq_input", "type": "infile", "format": "sequence.fastq"},
            {"name": "fasta_id", "type": "string", "default": "none"}
        ]
        self.add_option(options)
        self.step.add_steps("fastq_to_fasta")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.fastq_to_fasta.start()
        self.step.update()

    def stepfinish(self):
        self.step.fastq_to_fasta.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数设置
        :return:
        """
        if not self.option('fastq_input'):
            raise OptionError("必须设置输入的fastq文件")

    def set_resource(self):
        """
        设置所需资源
        :return:
        """
        self._cpu = 10
        self._memory = ''


class FastqToFastaTool(Tool):
    def __init__(self, config):
        super(FastqToFastaTool, self).__init__(config)
        self._version = "v1"

    def run(self):
        """
        运行
        :return:
        """
        super(FastqToFastaTool, self).run()
        self.fastq_to_fasta(self.option('fastq_input').prop["path"], self.option('fasta_id'))
        self.end()

    def fastq_to_fasta(self, fastq, id1):
        """
        将fastq文件转换成fasta文件
        :param fastq:
        :return:
        """
        n = 0
        i = 1
        self.logger.info("开始运行fastq_to_fasta函数")
        with open(fastq, 'r') as r:
            with open(self.output_dir + '/fasta', 'w') as w:
                for line in r:
                    n += 1
                    if (n-1) % 4 == 0:
                        if id1 == 'none':
                            w.write('%s' % line.replace('@', '>'))
                        else:
                            w.write('%s%s\n' % (id1, i))
                            i += 1
                    if (n+2) % 4 == 0:
                        w.write('%s' % line)
        self.logger.info("运行fastq_to_fasta函数出错")