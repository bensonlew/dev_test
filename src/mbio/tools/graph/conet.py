# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""CoNet网络图分析工具"""

from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError
import os
# import subprocess


class ConetAgent(Agent):
    """
    Conet
    version v3
    """
    def __init__(self, parent=None):
        """
        """
        super(ConetAgent, self).__init__(parent)
        options = [
            {"name": "data_file", "type": "infile", "format": "meta.otu.otu_table"},  # 输入数据矩阵
            {"name": "feature_file", "type": "infile", "format": "meta.env_table"},  # 输入环境特征文件
            {"name": "method", "type": "string", "default": "correl_spearman"},  # Cooccurrence方法
            {"name": "lower_threshold", "type": "float", "default": 0.6},  # Cooccurrence 阈值，最小值
            {"name": "upper_threshold", "type": "float", "default": 1},  # Cooccurrence 阈值，最大值
            {"name": "network_file", "type": "outfile", "format": "meta.gml"},  # 输出网络图文件
            {"name": "randomization", "type": "bool", "default": True},  # 是否进行网络图随机化计算
            {"name": "iterations", "type": "int", "default": 100},  # 随机化迭代次数
            {"name": "resamplemethod", "type": "string", "default": "permute"},  # 重抽样方法
            {"name": "pval_threshold", "type": "float", "default": 0.05}  # 重抽样方法
            ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option("data_file").is_set:
            raise OptionError("必须设置参数data_file")
        if self.option("resamplemethod") not in ("permute", "bootstrap"):
            raise OptionError("参数resamplemethod只能选择'permute', 'bootstrap'")
        else:
            if self.option("resamplemethod") == "permute":
                self.option("resamplemethod", value="shuffle_rows")

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = 6000


class ConetTool(Tool):
    def __init__(self, config):
        super(ConetTool, self).__init__(config)
        self.script_path = "meta/CoNet3/lib/CoNet.jar"

    def run(self):
        super(ConetTool, self).run()
        self.run_conet()

    def run_conet(self):
        cmd = "java -Xmx"+self._memory+"m -cp "+self.script_path+" be.ac.vub.bsb.cooccurrence.cmd.CooccurrenceAnalyser --method ensemble --format GML --matrixtype abundance --outout network.gml --input "+self.option("data_file")+" --ensemblemethods "+self.option("method")+" --ensembleparams "+self.option("method")+"~upper_threshold="+self.option("upper_threshold")+"/"+self.option("method")+"~lower_threshold="+self.option("lower_threshold")
        if self.option("feature_file").is_set:
            cmd += " --features "+self.option("feature_file")
        if self.option("randomization"):
            cmd += " --resamplemethod"+self.option("resamplemethod")+" --iterations "+self.option("iterations")+" --pval_threshold "+self.option("pval_threshold")
        self.logger.info(u"生成命令: "+cmd)
        conet = self.add_command("conet", cmd)
        self.logger.info("开始运行conet")
        conet.run()
        self.wait(conet)
        if conet.return_code == 0:
            self.logger.info("conet运行完成")
            os.link(self.work_dir+'/network.gml', self.output_dir+'/network.gml')
            self.option('network_file').set_path(self.output_dir+'/network.gml')
        else:
            self.set_error("conet运行出错!")
