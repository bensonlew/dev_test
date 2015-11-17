# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class LefseAgent(Agent):
    """
    statistical lefse+ 调用analysis_lefse.py 脚本进行lefse分析
    version v1.0
    author: qiuping
    last_modify: 2015.11.13
    """
    def __init__(self,parent):
        super(LefseAgent,self).__init__(parent)
        options = [
            {"name": "lefse_input", "type": "infile", "format": "meta.otu.otu_table"},#输入文件，biom格式的otu表
            {"name": "lefse_group", "type": "infile", "format": "meta.otu.group_table"},  # 输入分组文件
            {"name": "LDA", "type": "outfile", "format": "pdf"},  # 输出的结果,包括lefse分析的lda图
            {"name": "clado", "type": "outfile", "format": "pdf"},  # 输出结果,结果为lefse分析的clado图
            {"name": "lefse_xls", "type": "outfile", "format": "statistical.lda_table"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("lefse_input").is_set:
            raise OptionError("必须设置输入的otutable文件.")
        if not self.option("lefse_group").is_set:
            raise OptionError("必须提供分组信息文件")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''


class LefseTool(Tool):
    """
    Lefse tool
    """
    def __init__(self,config):
        super(LefseTool,self).__init__(config)
        self._version = '1.0.1'
        self.cmd_path = 'meta/scripts'

    def run_lefse(self):
        """
        返回lefse分析的命令
        """
        biom_table = self.option('lefse_input').convert_to_biom('otu_table.biom')
        cmd = self.cmd_path + "analysis_lefse.py -i otu_table.biom -m %s -o lefse_result" % (self.option('lefse_group').prop["path"])
        self.logger.info("开始运行lefse分析脚本")
        lefse_obj = self.add_command("lefse", cmd).run()
        self.wait(lefse_obj)
        if lefse_obj.return_code == 0:
            self.logger.info("lefse分析运行完成")
            self.end()
        else:
            self.set_error("lefse分析运行出错!")

    def set_lefse_output(self):
        """
        将结果文件链接至output
        """
        os.link(self.work_dir + '/lefse_result/lefse/lefse_LDA.cladogram.pdf', self.output_dir + 'lefse_LDA.cladogram.pdf')
        self.option('clado', value=self.output_dir+'lefse_LDA.cladogram.pdf')
        os.link(self.work_dir + '/lefse_result/lefse/lefse_LDA.pdf', self.output_dir + 'lefse_LDA.pdf')
        self.option('LDA', value=self.output_dir+'lefse_LDA.pdf')
        os.link(self.work_dir + '/lefse_result/lefse/lefse_LDA.xls', self.output_dir + 'lefse_LDA.xls')
        self.option('lefse_xls', value=self.output_dir+'lefse_LDA.xls')

    def run(self):
        super(LefseTool,self).run()
        self.run_lefse()
        self.set_lefse_output()
