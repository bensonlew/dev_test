# -*- coding: utf-8 -*-
# __author__ = "qiuping"
# last_modify:2016.10.09

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.express.cluster import *
import os


class ClusterAgent(Agent):
    """
    调用align_and_estimate_abundance.pl脚本，运行rsem，进行表达量计算分析
    version v1.0
    author: qiuping
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(ClusterAgent, self).__init__(parent)
        options = [
            {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，差异基因表达量矩阵
            {"name": "distance_method", "type": "string", "default": "euclidean"},  # 计算距离的算法
            {"name": "log", "type": "int", "default": 10},  # 画热图时对原始表进行取对数处理，底数为10或2
            {"name": "method", "type": "string", "default": "hclust"},  # 聚类方法选择
            {"name": "sub_num", "type": "int", "default": 10}  # 子聚类的数目

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
        if self.option('log') not in (10, 2):
            raise OptionError("所选log底数不在提供的范围内")
        if self.option("method") not in ("hclust", "kmeans", "both"):
            raise OptionError("所选方法不在范围内")
        if not isinstance(self.option("sub_num"), int):
            raise OptionError("子聚类数目必须为整数")
        if not (self.option("sub_num") >= 3 and self.option("sub_num") <= 35):
            raise OptionError("子聚类数目范围必须在3-35之间！")

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        if self.option('method') in ('both', 'hclust'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./hclust/", "", "hclust分析结果目录"],
                ["hclust/hc_gene_order", "txt", "按基因聚类的基因排序列表"],
                ["hclust/hc_sample_order", "txt", "按样本聚类的样本排序列表"],
                ["hclust/hclust_heatmap.xls", "xls", "层级聚类热图数据"]
            ])
            result_dir.add_regexp_rules([
                [r"hclust/subcluster_", "xls", "子聚类热图数据"]
            ])
        if self.option('method') in ('both', 'kmeans'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./kmeans/", "", "kmeans分析结果目录"],
                ["kmeans/kmeans_heatmap.xls", "xls", "层级聚类热图数据"]
            ])
            result_dir.add_regexp_rules([
                [r"kmeans/subcluster_", "xls", "子聚类热图数据"]
            ])
        super(ClusterAgent, self).end()


class ClusterTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(ClusterTool, self).__init__(config)
        self._version = '1.0.1'
        self.r_path = '/program/R-3.3.1/bin/Rscript'

    def run_cluster(self):
        clust(input_matrix=self.option('diff_fpkm').prop['path'], sub_num=self.option('sub_num'), method=self.option('method'), lognorm=self.option('log'), distance_method=self.option('distance_method'), cltype="both")
        clust_cmd = self.r_path + " clust.r"
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
            if self.option('method') in ('both', 'hclust'):
                os.system('cp -r %s/hclust/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置hclust结果目录成功")
            if self.option('method') in ('both', 'kmeans'):
                os.system('cp -r %s/kmeans/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置kmeans结果目录成功")
            self.logger.info("设置聚类分析结果目录成功")
        except Exception as e:
            self.logger.info("设置聚类分析结果目录失败{}".format(e))

    def run(self):
        super(ClusterTool, self).run()
        if not self.option("sub_num"):
            if len(self.option("diff_fpkm").prop['gene']) > 200:
                self.option("sub_num", 10)
            else:
                self.option("sub_num", 5)
        self.run_cluster()
        self.set_output()
        self.end()
