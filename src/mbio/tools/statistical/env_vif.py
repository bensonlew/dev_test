# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify: 2017.10.17

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import subprocess
from mbio.packages.statistical.env_vif import env_vif


class EnvVifAgent(Agent):
    """
    计算vif方差膨胀因子的工具
    """
    def __init__(self, parent):
        super(EnvVifAgent, self).__init__(parent)
        options = [
            {"name": "abundtable", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},
            # 物种/功能/基因丰度表格
            {"name": "envtable", "type": "infile", "format": "meta.otu.otu_table"},  # 环境因子表
            {"name": "viflim", "type": "int", "default": 10},  # 膨胀因子的筛选阈值[10-20],
            {"name": "method", "type": "string", "default": ""},  # rda|cca，默认根据 DCA result（DCA1>=3.5,CCA;DCA1<3.5,RDA）
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("abundtable").is_set:
            raise OptionError("请传入丰度文件！")
        if not self.option("envtable").is_set:
            raise OptionError("请传入环境因子文件！")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 2
        self._memory = '5G'

    def end(self):
        super(EnvVifAgent, self).end()


class EnvVifTool(Tool):
    def __init__(self, config):
        super(EnvVifTool, self).__init__(config)
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR+"/gcc/5.1.0/lib64:$LD_LIBRARY_PATH")
        self.r_path = "/program/R-3.3.1/bin/"

    def env_vif_r(self):
        self.logger.info(self.work_dir + '/correlation_matrix.xls')
        env_vif(self.option("abundtable").prop['path'], self.option("envtable").prop['path'], self.option("viflim"), self.option('method'), self.output_dir)
        cmd = self.r_path + "Rscript run_env_vif.r"
        self.logger.info("开始运行VIF方差膨胀因子分析")
        command = self.add_command("env_vif", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("vif方差膨胀因子运行完成")
        else:
            self.set_error("vif方差膨胀因子运行出错!")
        self.end()

    def run(self):
        super(EnvVifTool, self).run()
        self.env_vif_r()
