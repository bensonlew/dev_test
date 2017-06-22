# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modifiy: 2017.06.16
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.annotation.ref_annotation_query import AllAnnoStat
import os
import subprocess


class RefAnnoQueryAgent(Agent):
    """注释查询"""
    def __init__(self, parent):
        super(RefAnnoQueryAgent, self).__init__(parent)
        options = [
            {"name": "cog_list", "type": "string", "default": None},
            {"name": "gos_list", "type": "string", "default": None},
            {"name": "kegg_table", "type": "string", "default": None},
            {"name": "blast_nr_table", "type": "string", "default": None},
            {"name": "blast_swissprot_table", "type": "string", "default": None},
            {"name": "pfam_domain", "type": "string", "default": None},
            {"name": "length_path", "type": "string"},  # 注释转录本序列的长度
            {"name": "gtf_path", "type": "string"},  # 参考基因组gtf文件/新转录本gtf文件
        ]
        self.add_option(options)
        self.step.add_steps("ref_anno_query")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.ref_anno_query.start()
        self.step.update()

    def stepfinish(self):
        self.step.ref_anno_query.finish()
        self.step.update()

    def check_options(self):
        if not self.option("gos_list"):
            raise OptionError('缺少输入文件:gos_list')
        if not self.option("kegg_table"):
            raise OptionError('缺少输入文件:kegg_table')
        if not self.option("length_path"):
            raise OptionError('缺少输入文件:length_path')
        if not self.option("gtf_path"):
            raise OptionError('缺少输入文件:gtf_path')

    def set_resource(self):
        self._cpu = 5
        self._memory = '10G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [
            [".", "", "总注释结果输出目录"],
            ["/all_annotation.xls", "xls", "总注释结果表"]
        ]
        result_dir.add_relpath_rules(relpath)
        result_dir.add_regexp_rules([
        ])
        super(RefAnnoQueryAgent, self).end()


class RefAnnoQueryTool(Tool):
    def __init__(self, config):
        super(RefAnnoQueryTool, self).__init__(config)
        self.python = self.config.SOFTWARE_DIR + "/program/Python/bin/python"
        self.query_path = self.config.SOFTWARE_DIR + "/bioinfo/annotation/scripts/ref_annotation_query.py"

    def run_query(self):
        outpath = self.work_dir + "/all_annotation.xls"
        gtf_path = self.option("gtf_path")
        length_path = self.option("length_path")
        kegg_table = self.option("kegg_table")
        gos_list = self.option("gos_list")
        if self.option("blast_nr_table"):
            blast_nr_table = self.option("blast_nr_table")
        if self.option("blast_swissprot_table"):
            blast_swissprot_table = self.option("blast_swissprot_table")
        if self.option("pfam_domain"):
            pfam_domain = self.option("pfam_domain")
        if self.option("cog_list"):
            cog_list = self.option("cog_list")
        cmd = "{} {} {} {} {} {} {} {} {} {} {}".format(self.python, self.query_path, outpath, gtf_path, cog_list, kegg_table, gos_list, blast_nr_table, blast_swissprot_table, pfam_domain, length_path)
        self.logger.info("开始运行注释查询脚本")
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("运行注释查询脚本成功")
        except subprocess.CalledProcessError:
            self.set_error("运行注释查询脚本失败")
        self.set_output()

    def set_output(self):
        f1 = self.output_dir + "/all_annotation.xls"
        f2 = self.work_dir + "/all_annotation.xls"
        if os.path.exists(f1):
            os.remove(f1)
        os.link(f2, f1)
        self.end()

    def run(self):
        super(RefAnnoQueryTool, self).run()
        self.run_query()
