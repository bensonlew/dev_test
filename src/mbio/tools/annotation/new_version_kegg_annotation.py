# -*- coding: utf-8 -*-
# __author__ = 'wangbixuan'
# modified 2016.11.28
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import xml.etree.ElementTree as ET
import subprocess


class KeggAnnotationAgent(Agent):
    """
    to perform KEGG annotation
    author:wangbixuan
    last_modified:20160729
    modified at 20161128
    """

    def __init__(self, parent):
        super(KeggAnnotationAgent, self).__init__(parent)
        options = [
            {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml"}
        ]
        self.add_option(options)
        self.step.add_steps('kegg_annotation')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.kegg_annotation.start()
        self.step.update()

    def step_end(self):
        self.step.kegg_annotation.finish()
        self.step.update()

    def check_options(self):
        if not self.option("blastout").is_set:
            raise OptionError("必须提供BLAST结果文件")
        else:
            pass

    def set_resource(self):
        self._cpu = 10
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./kegg_table.xls", "xls", "KEGG annotation table"],
            ["./pathway_table.xls", "xls", "Sorted pathway table"],
            ["./kegg_taxonomy.xls", "xls", "KEGG taxonomy summary"]
        ])
        result_dir.add_regexp_rules([
            [r"pathways/ko\d+", 'pdf', '标红pathway图']
        ])
        super(KeggAnnotationAgent, self).end()


class KeggAnnotationTool(Tool):

    def __init__(self, config):
        super(KeggAnnotationTool, self).__init__(config)
        self._version = "2.0"

    def run(self):
        super(KeggAnnotationTool, self).run()
        self.kegg_annotation()

    def kegg_annotation(self):
        cmd = '{}/program/Python/bin/python /mnt/ilustre/users/sanger-dev/sg-users/chenyanyan/kegg_annotation/kegg_anno/kegg_annotation.py {}'.format(self.config.SOFTWARE_DIR, self.option("blastout").prop['path'])
        self.logger.info("运行kegg注释脚本")
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("运行成功完成！")
            self.end()
        except:
            import traceback
            self.logger.info('error:{}'.format(traceback.format_exc()))
            self.set_error("运行kegg脚本出错！")
