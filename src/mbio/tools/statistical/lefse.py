# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import subprocess


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
            {"name": "LDA", "type": "outfile", "format": "statistical.lefse_pdf"},  # 输出的结果,包括lefse分析的lda图
            {"name": "clado", "type": "outfile", "format": "statistical.lefse_pdf"},  # 输出结果,结果为lefse分析的clado图
            {"name": "lefse_xls", "type": "outfile", "format": "statistical.lda_table"},  # 输出结果
            {"name": "l", "type":"string", "default":"6"}
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
        self.biom_path = "Python/bin/"
        self.script_path = "meta/scripts/"
        self.plot_lefse_path = "meta/lefse/"

    def run_biom(self):
        biom_cmd = self.biom_path + "biom convert -i %s -o otu_taxa_table.biom --table-type \"otu table\" --process-obs-metadata taxonomy  --to-hdf5" % self.option('lefse_input').prop["path"]
        self.logger.info("开始运行biom_cmd")
        biom_command = self.add_command("biom_cmd", biom_cmd).run()
        self.wait(biom_command)
        if biom_command.return_code == 0:
            self.logger.info("biom_cmd运行完成")
            self.end()
        else:
            self.set_error("biom_cmd运行出错!")

    def run_script(self):
        script_cmd = self.script_path + "summarize_taxa.py -i otu_taxa_table.biom -o tax_summary_a -L 1,2,3,4,5,6,7 -a"
        self.logger.info("开始运行script_cmd")
        script_command = self.add_command("script_cmd", script_cmd).run()
        self.wait(script_command)
        if script_command.return_code == 0:
            self.logger.info("script_cmd运行完成")
            self.end()
        else:
            self.set_error("script_cmd运行出错!")

    def run_sum_tax(self):
        cmd = "for ((i=1;i<=7;i+=1)){\n\
            /mnt/ilustre/users/sanger/app/meta/scripts/sum_tax.fix.pl -i tax_summary_a/otu_taxa_table_L$i.txt -o tax_summary_a/otu_taxa_table_L$i.stat.xls\n\
            mv tax_summary_a/otu_taxa_table_L$i.txt.new tax_summary_a/otu_taxa_table_L$i.txt\n\
        }\n\\"
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("run_sum_tax出错")
            return False

    def run_plot_lefse(self):
        plot_cmd = self.plot_lefse_path + "plot-lefse.pl -o . -i tax_summary_a -l %s -m %s\n" % (self.option("l").prop['path'], self.option('lefse_group').prop['path'])
        self.logger.info("开始运行plot_cmd")
        plot_command = self.add_command("plot_cmd", plot_cmd).run()
        self.wait(plot_command)
        if plot_command.return_code == 0:
            self.logger.info("plot_cmd运行完成")
            self.end()
        else:
            self.set_error("plot_cmd运行出错!")

    def set_lefse_output(self):
        """
        将结果文件链接至output
        """
        os.link(self.work_dir + '/lefse/lefse_LDA.cladogram.pdf', self.output_dir + 'lefse_LDA.cladogram.pdf')
        self.option('clado', value=self.output_dir+'lefse_LDA.cladogram.pdf')
        os.link(self.work_dir + '/lefse/lefse_LDA.pdf', self.output_dir + 'lefse_LDA.pdf')
        self.option('LDA', value=self.output_dir+'lefse_LDA.pdf')
        os.link(self.work_dir + '/lefse/lefse_LDA.xls', self.output_dir + 'lefse_LDA.xls')
        self.option('lefse_xls', value=self.output_dir+'lefse_LDA.xls')

    def run(self):
        super(LefseTool,self).run()
        self.run_biom()
        self.run_script()
        self.run_sum_tax()
        self.run_plot_lefse()
        self.set_lefse_output()
        
