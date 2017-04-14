# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import subprocess


class FastqStatAgent(Agent):
    """
    fastx_quality_stats:用于统计fastq文件中各个碱基数目，质量等信息
    version 1.0
    author: qindanhua
    last_modify: 2016.01.06
    """

    def __init__(self, parent):
        super(FastqStatAgent, self).__init__(parent)
        options = [
            {"name": "fastq", "type": "infile", "format": "sequence.fastq"}
        ]
        self.add_option(options)
        self.step.add_steps('fastxtoolkit')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.fastxtoolkit.start()
        self.step.update()

    def step_end(self):
        self.step.fastxtoolkit.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("fastq").is_set:
            raise OptionError("请传入OTU代表序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = '5G'  # 待测试


class FastqStatTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(FastqStatTool, self).__init__(config)
        self.fastxtoolkit_path = '/bioinfo/seq/fastx_toolkit_0.0.14'

    def fastxtoolkit(self):
        """
        运行fastxtoolkit软件里的fastx_quality_stats工具统计fastq文件碱基质量等信息
        :return:
        """
        cmd = '%s/fastx_quality_stats -i %s  -o %s' % (self.fastxtoolkit_path,
                                                       self.option('fastq').prop['path'], 'fastq_stat.xls')
        print self.config.SOFTWARE_DIR + cmd
        self.logger.info('开始统计fastq文件信息')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR + cmd, shell=True)
            self.logger.info("统计完成")
            return True
        except subprocess.CalledProcessError:
            self.set_error("统计过程出现错误")
            raise Exception("统计过程出现错误")
            return False

    def set_output(self):
        """
        设置输出文件路径
        :return:
        """
        self.logger.info("set output")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir + '/fastq_stat.xls',
                self.output_dir + '/fastq_stat.xls')
        self.logger.info("done")

    def run(self):
        super(FastqStatTool, self).run()
        self.fastxtoolkit()
        self.set_output()
        self.end()
