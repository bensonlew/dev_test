# -*- coding: utf-8 -*-
# __author__ = 'mengmeng.liu'
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import subprocess
from mbio.packages.align.blast.blastout_statistics import blastout_statistics

class BlaststatAgent(Agent):
    """
    statistics blastout 调用blastout_statistics.py 进行统计分析
    version v1.0
    author:mengmeng.liu
    last_modify:2016.8.17 by wangbixuan
    """

    def __init__(self,parent):
        super(BlaststatAgent,self).__init__(parent)
        options = [
        {"name":"in_stat","type":"infile","format":"align.blast.blast_table,align.blast.blast_xml"},
        ]
        self.add_option(options)
        self.step.add_steps("stat_blast")
        self.on('start',self.start_statblast)
        self.on('end',self.end_statblast)

    def start_statblast(self):
        self.step.stat_blast.start()
        self.step.update()

    def end_statblast(self):
        self.step.stat_blast.finish()
        self.step.update()

    def check_options(self):
        """
        参数检查
        """
        if not self.option('in_stat').is_set:
            raise OptionError("必须指定需统计文件")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            [r".*_evalue\.xls","xls","比对结果E-value分布图"],
            [r".*_similar\.xls","xls","比对结果相似度分布图"]
        ])
        super(BlaststatAgent,self).end()

    def set_resource(self):
        self._cpu = 1
        self._memory = ''


class BlaststatTool(Tool):
    def __init__(self,config):
        super(BlaststatTool,self).__init__(config)
        #self.packages_path = 'packages/align/blast/blastout_statistics.py'

    def run_stat(self,table_fp):
        cmd='{}/program/Python/bin/python {}/bioinfo/align/scripts/blastout_statistics.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd+=" %s %s"%(table_fp, self.output_dir)
        self.logger.info("开始进行统计分析")
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd,shell=True)
            self.logger.info("统计分析完成")
        except subprocess.CalledProcessError:
            self.set_error("运行统计出错")

    def convert_xml(self):
        inputfile = self.option('in_stat').prop['path']
        self.logger.info(self.option("in_stat").format)
        self.logger.info(self.option("in_stat").format.__class__)
        self.logger.info("align.blast.blast_xml".__class__)
        if self.option("in_stat").format == "align.blast.blast_xml":
            self.logger.info('程序输出结果为6(xml)，实际需要结果为5(xls)，开始调用程序xml2table转换')
            inputfile = inputfile + "tmp.xls"
            self.option('in_stat').convert2table(inputfile)
            self.logger.info("格式转变完成")
        self.run_stat(inputfile)

    def run(self):
        super(BlaststatTool,self).run()
        self.convert_xml()
        self.end()
