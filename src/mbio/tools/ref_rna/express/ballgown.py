## !/mnt/ilustre/users/sanger/app/program/Anaconda2/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"
#last_modify:20160908

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.tools.ref_rna.express.ballgown_expr import *
import os
import re


class BallgownAgent(Agent):
    """
    调用cluster-PCA-heatmap-cor.r脚本，获得相关系数热图表格（基因同时给出），样本聚类树表格（基因同时给出），样本的PCA表格
    version v1.0
    author: zhangpeng
    last_modify: 2016.09.08
    """
    def __init__(self, parent):
        super(BallgownAgent, self).__init__(parent)
        options = [
            {"name": "diff_fpkm", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  #输入文件，差异基因表达量矩阵
            {"name": "feature", "type": "string", "default": "transcript"},  # 选择分析对象
            {"name": "group", "type":"infile", "format": "meta.otu.group_table"}, #分组信息
            #{"name": "gname", "type": "string"},  # 分组方案名称           
            {"name": "meas", "type": "string", "default": "FPKM"},  # 选择分析数据类型
            
        ]
        self.add_option(options)
        self.step.add_steps("Ballgown")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.Ballgown.start()
        self.step.update()

    def stepfinish(self):
        self.step.Ballgown.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("diff_fpkm").is_set:
            raise OptionError("必须设置输入文件:差异基因fpkm表")
        if self.option("feature") not in ("gene", "exon", "intron", "transcript"):
            raise OptionError("所选分析类型不在提供的范围内")
        if self.option("meas") not in ("cov", "FPKM"):
            raise OptionError("所选分析数据类型不在范围内")
        if not self.option("group").is_set:
            raise OptionError('输入分组')


    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '4G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        #dir_path =os.path.join(self.work_dir,"expr")
        if self.option('feature') in ('gene', 'exon', 'intron', 'transcript'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./expr/", "", "gene分析结果目录"],
                ["expr/exon.xls", "xls", "差异分析"],
				["expr/intron.xls",'xls',"差异外显子"],
				["expr/tran.xls","xls","差异转录"]
            ])
        if self.option('feature') in ('gene', 'exon', 'intron', 'transcript'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./diff/", "", "exon分析结果目录"],
                ["diff/diff_expr.xls", "xls", "差异分析"]
            ])
        super(BallgownAgent, self).end()


class BallgownTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(BallgownTool, self).__init__(config)
        self._version = '1.0.1'
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.r_path = '/program/R-3.3.1/bin/Rscript'

    def run_cluster(self):
        clust(input_matrix=self.option('diff_fpkm').prop['path'],  feature=self.option('feature'), meas=self.option('meas'), group=self.option('group').prop['path'])
        clust_cmd = self.r_path + " ballgown"
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
        #dir_path =os.path.join(self.work_dir,"expr")
        #os.mkdir(dir_path)
        #dir_path =os.path.join(self.work_dir,"diff")
        #os.mkdir(dir_path)
        try:
            if self.option('feature') in ('gene', 'exon', 'intron', 'transcript'):
                os.system('cp -r %s/expr/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置expr结果目录成功")
            if self.option('feature') in ('gene', 'exon', 'intron', 'transcript'):
                os.system('cp -r %s/diff/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置diff结果目录成功")
        except Exception as e:
            self.logger.info("设置结果目录失败{}".format(e))

    def run(self):
        super(BallgownTool, self).run()
        self.run_cluster()
        self.set_output()
        self.end()
