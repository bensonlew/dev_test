# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""无参转录组表达差异分析"""

from biocluster.workflow import Workflow
import os
import re


class DiffExpressWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(DiffExpressWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "express_file", "type": "string", 'default': "none"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},
            {"name": "ci", "type": "float"},
            {"name": "rate", "type": "float"},
            {"name": "express_id", "type": "string"},
            {"name": "params", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.diff_exp = self.add_tool("denovo_rna.express.diff_exp")
        self.output_dir = self.diff_exp.output_dir

    def run_diff_exp(self):
        options = {
            "count": self.option("express_file")[1],
            "fpkm": self.option("express_file")[0],
            "control_file": self.option("control_file"),
            "diff_ci": self.option("ci"),
            "diff_rate": self.option("rate"),
        }
        if self.option("group_id") != "all":
            options['edger_group'] = self.option("group_file")
            self.samples = self.option('count').prop['sample']
        else:
            self.samples = self.option('edger_group').prop['sample']
        self.diff_exp.set_options(options)
        self.diff_exp.on("end", self.set_db)
        self.diff_exp.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [[".", "", "结果输出目录"]]
        if self.diff_gene:
            relpath += [
                ["diff_fpkm", "xls", "差异基因表达量表"],
                ["diff_count", "xls", "差异基因计数表"]
            ]
        result_dir.add_regexp_rules([
            [r"_edgr_stat\.xls$", "xls", "edger统计结果文件"]
        ])
        result_dir.add_relpath_rules(relpath)
        super(DiffExpressWorkflow, self).end()

    def set_db(self):
        """
        保存结果表保存到mongo数据库中
        """
        api_diff_exp = self.api.denovo_express
        diff_files = os.listdir(self.output_dir)
        compare_column = list()
        for f in diff_files:
            if re.search(r'_edgr_stat.xls$', f):
                con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                compare_column.append('|'.join(con_exp))
        params = eval(self.option("params"))
        api_diff_exp.add_express_diff(params=params, samples=self.samples, compare_column=compare_column, express_id=self.option('express_id'), diff_exp_dir=self.output_dir)
        self.end()

    def run(self):
        self.run_diff_exp()
        super(DiffExpressWorkflow, self).run()
