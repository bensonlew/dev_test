# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# modified 2017.04.13
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import subprocess


class KeggUploadAgent(Agent):
    """
    """
    def __init__(self, parent):
        super(KeggUploadAgent, self).__init__(parent)
        options = [
            {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},
            {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
            {"name": "kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
        ]
        self.add_option(options)
        self.step.add_steps('kegg_update')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.kegg_update.start()
        self.step.update()

    def step_end(self):
        self.step.kegg_update.finish()
        self.step.update()

    def check_options(self):
        if not self.option("kos_list_upload").is_set:
            raise OptionError("必须提供kegg注释结果文件")
        if self.option("taxonomy") not in ["Animals", "Plants", "Fungi", "Protists", "Archaea", "Bacteria", None]:
            raise OptionError("物种类别必须为Animals/Plants/Fungi/Protists/Archaea/Bacteria/None")

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
        super(KeggUploadAgent, self).end()


class KeggUploadTool(Tool):

    def __init__(self, config):
        super(KeggUploadTool, self).__init__(config)
        self._version = "2.0"
        self.taxonomy_path = self.config.SOFTWARE_DIR + "/database/KEGG/species/{}.ko.txt".format(self.option("taxonomy"))

    def run(self):
        super(KeggUploadTool, self).run()
        self.kegg_annotation()
        self.end()

    def kegg_annotation(self):
        self.logger.info("运行kegg注释脚本")
        if self.option("taxonomy"):
            taxonomy = self.taxonomy_path
        else:
            taxonomy = None
        self.option("kos_list_upload").get_transcript_anno(outdir=self.work_dir + "/kegg.list")
        self.option("kos_list_upload").get_gene_anno(outdir=self.work_dir + "/gene_kegg.list")
        try:
            kegg_anno = self.load_package('annotation.kegg.kegg_annotation')()
            kegg_anno.pathSearch_upload(kegg_ids=self.work_dir + "/kegg.list", kegg_table=self.output_dir + '/kegg_table.xls', taxonomy=taxonomy)
            kegg_anno.pathTable(kegg_table=self.output_dir + '/kegg_table.xls', pathway_path=self.output_dir + '/pathway_table.xls', pidpath=self.work_dir + '/pid.txt')
            kegg_anno.getPic(pidpath=self.work_dir + '/pid.txt', pathwaydir=self.output_dir + '/pathways')
            kegg_anno.keggLayer(pathway_table=self.output_dir + '/pathway_table.xls', layerfile=self.output_dir + '/kegg_layer.xls', taxonomyfile=self.output_dir + '/kegg_taxonomy.xls')
            self.option("kegg_table", self.output_dir + '/kegg_table.xls')
            self.logger.info("运行成功完成！")
            self.option('kegg_table', self.output_dir + '/kegg_table.xls')
        except:
            import traceback
            self.logger.info('error:{}'.format(traceback.format_exc()))
            self.set_error("运行kegg脚本出错！")
