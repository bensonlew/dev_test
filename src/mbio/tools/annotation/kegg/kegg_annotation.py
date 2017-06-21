# -*- coding: utf-8 -*-
# __author__ = 'chenyanyan'
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
    author:chenyanyan
    modified at 20161128
    """

    def __init__(self, parent):
        super(KeggAnnotationAgent, self).__init__(parent)
        options = [
            {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
            {"name": "kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
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
        if self.option("taxonomy") not in ["Animals", "Plants", "Fungi", "Protists", "Archaea", "Bacteria", None]:
            raise OptionError("物种类别必须为Animals/Plants/Fungi/Protists/Archaea/Bacteria/None")

    def set_resource(self):
        self._cpu = 10
        self._memory = '50G'

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
        self.python = "program/Python/bin/python"
        self.taxonomy_path = self.config.SOFTWARE_DIR + "/database/KEGG/species/{}.ko.txt".format(self.option("taxonomy"))
        self.kegg_path = self.config.SOFTWARE_DIR + "/bioinfo/annotation/scripts/kegg_annotation.py"
        self.image_magick = self.config.SOFTWARE_DIR + "/program/ImageMagick/bin/convert"

    def run(self):
        super(KeggAnnotationTool, self).run()
        self.kegg_annotation()
        self.end()

    def kegg_annotation(self):
        self.logger.info("运行kegg注释脚本")
        if self.option("taxonomy"):
            taxonomy = self.taxonomy_path
        else:
            taxonomy = None
        blast_xml = self.option('blastout').prop['path']
        kegg_table = self.output_dir + '/kegg_table.xls'
        pidpath = self.output_dir + '/pid.txt'
        pathwaydir = self.output_dir + '/pathways'
        image_magick = self.image_magick
        pathway_table = self.output_dir + '/pathway_table.xls'
        layerfile = self.output_dir + '/kegg_layer.xls'
        taxonomyfile = self.output_dir + '/kegg_taxonomy.xls'
        cmd = "{} {} {} {} {} {} {} {} {} {} {} {}".format(self.python, self.kegg_path, blast_xml, None, kegg_table, pidpath, pathwaydir, pathway_table, layerfile, taxonomyfile, taxonomy, self.image_magick)
        command = self.add_command("kegg_anno", cmd).run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行kegg注释脚本完成")
        else:
            self.set_error("运行kegg注释脚本出错")
        self.option('kegg_table', self.output_dir + '/kegg_table.xls')
