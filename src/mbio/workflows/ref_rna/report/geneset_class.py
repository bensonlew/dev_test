# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.workflow import Workflow
from collections import defaultdict
import os
import re
from itertools import chain
from mbio.packages.denovo_rna.express.kegg_regulate import KeggRegulate


class GenesetClassWorkflow(Workflow):
    """
    基因集功能分类分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(GenesetClassWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "geneset_go", "type": "string"},
            {"name": "geneset_cog", "type": "string"},
            {"name": "geneset_kegg", "type": "string"},
            {"name": "kegg_table", "type": "infile", "format": "annotation.kegg.kegg_table"},
            {"name": "anno_type", "type": "string"},
            {"name": "geneset_type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_table_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "geneset_id", "type": "string"},

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        # self.group_spname = dict()

    def run(self):
        self.start_listener()
        self.fire("start")
        if self.option("anno_type") == "kegg":
            self.get_kegg_table()
        self.set_db()
        # super(GenesetClassWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_geneset = self.api.ref_rna_geneset
        self.logger.info("wooooooooorkflowinfoooooooo")
        if self.option("anno_type") == "cog":
            output_file = self.option("geneset_cog")
            api_geneset.add_geneset_cog_detail(output_file, self.option("main_table_id"))
        elif self.option("anno_type") == "go":
            output_file = self.option("geneset_go")
            api_geneset.add_go_regulate_detail(output_file, self.option("main_table_id"))
        else:
            output_file = self.output_dir + '/kegg_stat.xls'
            pathway_file = self.output_dir + '/pathways'
            api_geneset.add_kegg_regulate_detail(self.option("main_table_id"), output_file)
            api_geneset.add_kegg_regulate_pathway(pathway_file, self.option("main_table_id"))
        # os.link(output_file, self.output_dir + "/" + os.path.basename(output_file))
        print(output_file)
        self.end()

    def get_kegg_table(self):
        kegg = KeggRegulate()
        ko_genes, path_ko = self.option('kegg_table').get_pathway_koid()
        # regulate_dict = defaultdict(set)
        regulate_gene = {}
        with open(self.option("geneset_kegg"), "r") as f:
            for line in f:
                line = line.strip().split("\t")
                regulate_gene[line[0]] = line[1].split(",")
        pathways = self.output_dir + '/pathways'
        if not os.path.exists(pathways):
            os.mkdir(pathways)
        self.logger.info(ko_genes)
        # kegg.get_pictrue(path_ko=path_ko, out_dir=pathways, regulate_dict=regulate_gene)  # 颜色
        kegg.get_pictrue(path_ko=path_ko, out_dir=pathways)
        kegg.get_regulate_table(ko_gene=ko_genes, path_ko=path_ko, regulate_gene=regulate_gene, output=self.output_dir + '/kegg_stat.xls')

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "基因集功能分类结果目录"],
            # ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        # print self.get_upload_files()
        self._upload_result()
        self._import_report_data()
        self.step.finish()
        self.step.update()
        self.logger.info("运行结束!")
        self._update("end")
        self.set_end()
        self.fire('end')
        # super(GenesetClassWorkflow, self).end()

