# -*- coding: utf-8 -*-
# __author__ = "shenghe"
# last_modify:20160815

import os
import threading
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.annonation.go.go_graph import draw_GO


class GoEnrichAgent(Agent):
    """
    version v1.0
    author: hesheng
    last_modify: 2016.08.10
    """
    def __init__(self, parent):
        super(GoEnrichAgent, self).__init__(parent)
        options = [
            {"name": "diff_list", "type": "infile", "format": "denovo_rna.express.gene_list"},
            {"name": "all_list", "type": "infile", "format": "denovo_rna.express.gene_list"},
            {"name": "go_list", "type": "infile", "format": "annotation.go.go_list"},  # test
            ]
        self.add_option(options)
        self.step.add_steps("goenrich")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.goenrich.start()
        self.step.update()

    def stepfinish(self):
        self.step.goenrich.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        for item in self._options.values():
            if not item.is_set:
                raise OptionError('缺少输入文件')


    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 1
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
            ])
        result_dir.add_regexp_rules([
            [r"go_enrich_.*", "xls", "go富集结果文件"],
            [r"go_lineage.*", "png", "go富集有向无环图"],
            ])
        super(GoEnrichAgent, self).end()


class GoEnrichTool(Tool):
    """
    """
    def __init__(self, config):
        super(GoEnrichTool, self).__init__(config)
        self.goatools_path = '/bioinfo/annotation/goatools-0.6.5-shenghe'
        self.go_enrich_path = self.goatools_path + '/scripts/find_enrichment.py'
        self.obo = self.config.SOFTWARE_DIR + '/database/GO/go-basic.obo'
        self.set_environ(PYTHONPATH=self.config.SOFTWARE_DIR + self.goatools_path)
        self.python_path = 'program/Python/bin/python'
        self.out_enrich_fp = self.output_dir + '/go_enrich_' + os.path.splitext(os.path.basename(self.option('diff_list').path))[0] + '.xls'
        self.out_go_graph = self.output_dir + '/go_lineage'

    def run_enrich(self):
        cmd = self.python_path + ' ' + self.config.SOFTWARE_DIR + self.go_enrich_path + ' '
        cmd = cmd + self.option('diff_list').path + ' ' + self.option('all_list').path + ' ' + self.option('go_list').path
        cmd = cmd + ' --indent --method bonferroni,sidak,holm,fdr --outfile ' + self.out_enrich_fp
        cmd = cmd + ' --obo ' + self.obo
        command = self.add_command('go_enrich', cmd)
        command.run()
        command.wait()
        if command.return_code == 0:
            draw_thread = threading.Thread(target=self.run_draw_go_graph)
            draw_thread.start()
            draw_thread.join()
        else:
            self.set_error('goatools计算错误')

    def run_draw_go_graph(self):
        try:
            draw_GO(self.get_go_pvalue_dict(), out=self.out_go_graph)
            self.end()
        except Exception:
            self.set_error('绘图发生错误')


    def get_go_pvalue_dict(self):
        go2pvalue = {}
        with open(self.out_enrich_fp) as f:
            f.readline()
            for line in f:
                line_sp = line.split('\t')
                go2pvalue[line_sp[0]] = line_sp[9]
        return go2pvalue

    def run(self):
        super(GoEnrichTool, self).run()
        self.run_enrich()
