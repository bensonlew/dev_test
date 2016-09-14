# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re


class CufflinksAgent(Agent):
    """
    有参转录组cufflinks拼接
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.09
    """
    def __init__(self, parent):
        super(CufflinksAgent, self).__init__(parent)
        options = [
            {"name": "sample_bam", "type": "infile", "format": "ref_rna.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default":10},  #cufflinks软件所分配的cpu数量
            {"name": "fr_stranded", "type": "string"},  # 是否链特异性
            {"name": "strand_direct", "type": "string"},# 链特异性时选择正负链
            {"name": "sample_gtf", "type": "outfile","format":"ref_rna.gtf"},  # 输出的文件夹
        ]
        self.add_option(options)
        self.step.add_steps("cufflinks")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.cufflinks.start()
        self.step.update()

    def stepfinish(self):
        self.step.cufflinks.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('sample_bam'):
            raise OptionError('必须输入样本文件为bam格式')
        if not self.option('ref_fa') :
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        if not self.option("fr_stranded")  and not self.option("strand_direct").is_set:
            raise OptionError("当链特异性时必须选择正负链")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "100G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            [".gtf", "gtf", "样本拼接之后的gtf文件"]
        ])
        super(CufflinksAgent, self).end()


class CufflinksTool(Tool):
    def __init__(self, config):
        super(CufflinksTool, self).__init__(config)
        self._version = "v1.0.1"
        self.cufflinks_path = '/bioinfo/rna/cufflinks-2.2.1/'

    def run(self):
        """
        运行
        :return:
        """
        super(CufflinksTool, self).run()
        # self.run_cufflinks()
        self.set_output()
        self.end()


    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        try:
            sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
            """
            for root, dirs, files in os.walk(self.work_dir + "/" + sample_name+ "/"):
                for names in files:
                    self.logger.info(os.path.join(root, names))
                    os.link(os.path.join(root, names),self.output_dir + sample_name )
            """
            os.link(self.work_dir + "/" + sample_name + "/", self.output_dir)
            self.logger.info(self.output_dir + "/"+sample_name+ "/")
            # self.option('sample_gtf').set_path(self.work_dir + "/" + sample_name +"/"+ "transcripts.gtf")
            self.logger.info("设置组装拼接分析结果目录成功")

        except Exception as e:
            self.logger.info("设置组装拼接分析结果目录失败{}".format(e))
            self.set_error("设置组装拼接分析结果目录失败{}".format(e))
