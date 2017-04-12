# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# modified 2017.04.12
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import xml.etree.ElementTree as ET
import subprocess


class MergeAnnotAgent(Agent):
    """
    将已知（参考基因组）序列和新序列的注释结果合一起
    """
    def __init__(self, parent):
        super(MergeAnnotAgent, self).__init__(parent)
        options = [
            {"name": "level2_dir", "type": "string"},
            {"name": "gos_dir", "type": "string"},
            {"name": "kegg_table_dir", "type": "string", "default": None},
            {"name": "cog_table_dir", "type": "string", "default": None},
            {"name": "database", "type": "string", "default": "go,cog,kegg"},
            {"name": "level2", "type": "outfile", "format": "annotation.go.level2"},
            {"name": "gos", "type": "outfile", "format": "annotation.go.go_list"},
            {"name": "kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
            {"name": "cog_table", "type": "outfile", "format": "annotation.cog.cog_table"},
        ]
        self.add_option(options)
        self.step.add_steps("merge_annot")
        self.on("start", self.step_start)
        self.on("end", self.step_end)

    def step_start(self):
        self.step.merge_annot.start()
        self.step.update()

    def step_end(self):
        self.step.merge_annot.finish()
        self.step.update()

    def check_options(self):
        self.database = set(self.option("database").split(","))
        if len(self.database) < 1:
            raise OptionError("至少选择一种注释库")
        for db in self.database:
            if db not in ["go", "cog", "kegg"]:
                raise OptionError("需要合并的注释文件不在支持范围内")
            if  db == "go" and not self.option("level2_dir") and not self.option("gos_dir"):
                raise OptionError("缺少go注释level2和gos的输入文件目录")
            if  db == "cog" and not self.option("cog_table_dir"):
                raise OptionError("缺少cog注释table的输入文件目录")
            if  db == "kegg" and not self.option("kegg_table_dir"):
                raise OptionError("缺少kegg注释table的输入文件目录")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = "5G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["./merged_go2level.xls", "xls", "go注释level2合并文件"],
            ["./merged_gos.list", "xls", "go注释gos合并文件"],
            ["./merged_cog_table.xls", "xls", "cog注释table合并文件"],
            ["./merged_kegg_table.xls", "xls", "kegg注释table合并文件"]
        ])
        super(MergeAnnotAgent, self).end()


class MergeAnnotTool(Tool):
    def __init__(self, config):
        super(MergeAnnotTool, self).__init__(config)
        self._version = '1.0'
        self.database = self.option("database").split(",")

    def run_merge(self):
        for db in self.database:
            if db == "go":
                level2 = self.option("level2_dir").split(";")
                gos = self.option("gos_dir").split(";")
                self.merge(dirs=level2, merge_file="merged_go2level.xls")
                self.merge(dirs=gos, merge_file="merged_gos.list")
                self.logger.info("合并go注释文件完成")
            if db == "cog":
                cog = self.option("cog_table_dir").split(";")
                self.merge(dirs=cog, merge_file="merged_cog_table.xls")
                self.logger.info("合并cog注释文件完成")
            if db == "kegg":
                kegg = self.option("kegg_table_dir").split(";")
                self.merge(dirs=kegg, merge_file="merged_kegg_table.xls")
                self.logger.info("合并kegg注释文件完成")
        files = ["merged_go2level.xls", "merged_gos.list", "merged_cog_table.xls", "merged_kegg_table.xls"]
        for f in files:
            if os.path.exists(f):
                linkfile = os.path.join(self.output_dir, f)
                if os.path.exists(linkfile):
                    os.remove(linkfile)
                os.link(f, linkfile)

    def merge(self, dirs, merge_file):
        filt = []
        for path in dirs:
            if os.path.exists(path):
                with open(path, "rb") as f:
                    lines = f.readlines()
                    for line in lines:
                        if line not in filt:
                            filt.append(line)
            else:
                self.set_error("{}文件不存在".format(path))
        with open(merge_file, "wb") as w:
            for line in filt:
                w.write(line)

    def run(self):
        super(MergeAnnotTool, self).run()
        self.run_merge()
        self.end()
