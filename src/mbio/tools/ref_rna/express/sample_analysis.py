## !/mnt/ilustre/users/sanger/app/program/Anaconda2/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"
#last_modify:20160908

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.tools.ref_rna.express.sample_analysis_expr import *
import os
import re


class SampleAnalysisAgent(Agent):
    """
    调用cluster-PCA-heatmap-cor.r脚本，获得相关系数热图表格（基因同时给出），样本聚类树表格（基因同时给出），样本的PCA表格
    version v1.0
    author: zhangpeng
    last_modify: 2016.09.08
    """
    def __init__(self, parent):
        super(SampleAnalysisAgent, self).__init__(parent)
        options = [
            {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #输入文件，差异基因表达量矩阵
            {"name": "distance_method", "type": "string", "default": "euclidean"},  # 计算距离的算法
            
            {"name": "method", "type": "string", "default": "all"},  # 聚类方法选择
            {"name": "lognorm", "type": "int", "default": 10}  # 画热图时对原始表进行取对数处理，底数为10或2

        ]
        self.add_option(options)
        self.step.add_steps("cluster")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.cluster.start()
        self.step.update()

    def stepfinish(self):
        self.step.cluster.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("diff_fpkm").is_set:
            raise OptionError("必须设置输入文件:差异基因fpkm表")
        if self.option("distance_method") not in ("manhattan", "euclidean"):
            raise OptionError("所选距离算法不在提供的范围内")
        if self.option('lognorm') not in (10, 2):
            raise OptionError("所选log底数不在提供的范围内")
        if self.option("method") not in ("cor_heatmap","tree", "PCA", "all"):
            raise OptionError("所选方法不在范围内")

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '20G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        if self.option('method') in ('cor_heatmap', 'all'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./analysis_cor_heatmap/", "", "analysis分析结果目录"],
                ["analysis_cor_heatmap/cor_heatmap.xls", "xls", "相关系数热图"]
            ])
        if self.option('method') in ('all', 'tree'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./analysis_tree/", "", "树图的数据"],
                ["analysis_tree/all_heatmap.xls", "xls", "全基因的热图"],
				["analysis_tree/genes_tree.txt", "txt", "全基因的树图"],
				["analysis_tree/samples_tree.txt", "txt", "样本的树图"],
            ])
        if self.option('method') in ('all', 'PCA'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./analysis_PCA/", "", "样本PCA的数据"],
                ["analysis_PCA/pca_rotation.xls", "xls", "PCA的数据（可以两维度或者三维度）"]		
            ])
        super(SampleAnalysisAgent, self).end()


class SampleAnalysisTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(SampleAnalysisTool, self).__init__(config)
        self._version = '1.0.1'
        self.r_path = '/program/R-3.3.1/bin/Rscript'

    def run_cluster(self):
        clust(input_matrix=self.option('diff_fpkm').prop['path'],  method=self.option('method'), lognorm=self.option('lognorm'), distance_method=self.option('distance_method'),cltype="both")
        clust_cmd = self.r_path + " sample_analysis"
        self.logger.info("开始运行clust_cmd")
        cmd = self.add_command("clust_cmd", clust_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行clust_cmd成功")
        else:
            self.logger.info("运行clust_cmd出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        try:
            if self.option('method') in ('all', 'cor_heatmap'):
                os.system('cp -r %s/analysis_cor_heatmap/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置cor_heatmap结果目录成功")
            if self.option('method') in ('all', 'tree'):
                os.system('cp -r %s/analysis_tree/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置tree结果目录成功")
            if self.option('method') in ('all', 'PCA'):
                os.system('cp -r %s/analysis_PCA/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置PCA结果目录成功")
        except Exception as e:
            self.logger.info("设置结果目录失败{}".format(e))

    def run(self):
        super(SampleAnalysisTool, self).run()
        self.run_cluster()
        self.set_output()
        self.end()
