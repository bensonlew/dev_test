# -*- coding: utf-8 -*-
# __author__ = "shenghe"
# last_modify:20160815

import os
import traceback
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.annotation.go.go_graph import draw_GO


class GoEnrichAgent(Agent):
    """
    version v1.0
    author: hesheng
    last_modify: 2016.08.10
    """
    def __init__(self, parent):
        super(GoEnrichAgent, self).__init__(parent)
        options = [
            {"name": "diff_list", "type": "infile", "format": "rna.gene_list"},
            # {"name": "all_list", "type": "infile", "format": "rna.gene_list"},
            {"name": "go_list", "type": "infile", "format": "annotation.go.go_list"},  # test
            {"name": "pval", "type": "string", "default": "0.05"},
            {"name": "method", "type": "string", "default": "bonferroni,sidak,holm,fdr"}
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
        if not self.option("diff_list").is_set:
            raise OptionError("缺少输入文件:差异基因名称文件")
        # if not self.option("all_list").is_set:
        #     raise OptionError("缺少输入文件:全部基因名称文件")
        if not self.option("go_list").is_set:
            raise OptionError("缺少输入文件:差异基因对应的go_id")


    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 1
        self._memory = '5G'

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

    def check_list(self):
        """
        去除diff_list中没有注释信息的数据
        new_file_name为在work_dir中生成的新diff_list文件的绝对路径
        :return:
        """
        # file1 = "/mnt/ilustre/users/sanger-dev/workspace/20170613/Refrna_ore_test_for_api/Express/output/diff/trans_diff/diff_list_dir/A_vs_B"
        # file2 = "/mnt/ilustre/users/sanger-dev/workspace/20170613/Refrna_ore_test_for_api/Express/output/rsem/trans_list"
        file1 = self.option("diff_list").prop["path"]
        file2 = self.work_dir + "/all.list"
        f1 = open(file1, "r")
        f2 = open(file2, "r")
        lst_1 = f1.readlines()
        lst_2 = f2.readlines()
        f1.close()
        f2.close()
        new_file_name = self.work_dir + "/" + os.path.basename(file1)
        with open(new_file_name, "w") as fw:
            for item in lst_1:
                if item in lst_2:
                    fw.write(item)
        return new_file_name

    def run_enrich(self):
        cmd0 = "less {}| cut -f1 > {}/all.list".format(self.option('go_list').path, self.work_dir)
        os.system(cmd0)
        new_file_name = self.check_list()  # edited by shijin 除去背景中不存在的基因
        cmd = self.python_path + ' ' + self.config.SOFTWARE_DIR + self.go_enrich_path + ' '
        cmd = cmd + new_file_name + ' ' + self.work_dir + "/all.list" + ' ' + self.option('go_list').path
        cmd = cmd + ' --pval ' + self.option('pval') + ' --indent' + ' --method ' + self.option('method') + ' --outfile ' + self.out_enrich_fp
        cmd = cmd + ' --obo ' + self.obo
        command = self.add_command('go_enrich', cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.run_draw_go_graph()
        else:
            self.set_error('goatools计算错误')

    def run_draw_go_graph(self):
        try:
            self.logger.info('run_draw_go_graph')
            go_pvalue = self.get_go_pvalue_dict()
            self.logger.info('rrrrrrrrrrrrrrrrun_draw_go_graph')
            self.logger.info(go_pvalue)
            self.logger.info('run_draw_go_graphhhhhhhhhhhhhhhhh')
            if go_pvalue:
                draw_GO(go_pvalue, out=self.out_go_graph)
            self.end()
        except Exception:
            self.set_error('绘图发生错误:\n{}'.format(traceback.format_exc()))


    def get_go_pvalue_dict(self):
        go2pvalue = {}
        with open(self.out_enrich_fp) as f:
            f.readline()
            for line in f:
                line_sp = line.split('\t')
                p_bonferroni = float(line_sp[9])
                go2pvalue[line_sp[0]] = p_bonferroni
        tar = sorted(go2pvalue.items(), key=lambda e:e[1], reverse=True)
        new_go2pvalue = dict(tar[-10:])
        self.logger.info(new_go2pvalue)
        return new_go2pvalue

    def run(self):
        super(GoEnrichTool, self).run()
        self.run_enrich()
