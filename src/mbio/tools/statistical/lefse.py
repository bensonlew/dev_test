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
    last_modify: 2015.12.03
    """
    def __init__(self, parent):
        super(LefseAgent, self).__init__(parent)
        options = [
            {"name": "lefse_input", "type": "infile", "format": "meta.otu.otu_table"},  # 输入文件，biom格式的otu表
            {"name": "lefse_group", "type": "infile", "format": "meta.otu.group_table"},  # 输入分组文件
            {"name": "lda_filter", "type": "float", "default": 2.0},
            {"name": "strict", "type": "int", "default": 0},
            {"name": "lefse_gname", "type": "string", "default": "None"}
        ]
        self.add_option(options)
        self.step.add_steps("run_biom", "tacxon_stat", "plot_lefse")

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("lefse_input").is_set:
            raise OptionError("必须设置输入的otutable文件.")
        if not self.option("lefse_group").is_set:
            raise OptionError("必须提供分组信息文件")
        if self.option("strict") not in [0, 1]:
            raise OptionError("所设严格性超出范围值")
        if self.option("lda_filter") > 4.0 or self.option("lda_filter") < -4.0:
            raise OptionError("所设阈值超出范围值")
        if self.option("lefse_gname") != 'None':
            for i in self.option('lefse_gname').split(','):
                gnum = self.option('lefse_group').group_num(i)
                if gnum < 2:
                    raise OptionError("lefse分析分组类别必须大于2")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''

    def biom_start_callback(self):
        self.step.run_biom.start()
        self.step.update()

    def biom_finish_callback(self):
        self.step.run_biom.finish()
        self.step.update()

    def sum_taxa_start_callback(self):
        self.step.tacxon_stat.start()
        self.step.update()

    def sum_taxa_finish_callback(self):
        self.step.tacxon_stat.finish()
        self.step.update()

    def lefse_start_callback(self):
        self.step.plot_lefse.start()
        self.step.update()
    
    def lefse_finish_callback(self):
        self.step.plot_lefse.finish()
        self.step.update()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "lefse分析结果输出目录"],
            ["./lefse_LDA.cladogram.png", "png", "lefse分析cladogram结果图片"],
            ["./lefse_LDA.png", "png", "lefse分析LDA图片"],
            ["./lefse_LDA.xls", "xls", "lefse分析lda数据表"]
        ])
        super(LefseAgent, self).end()


class LefseTool(Tool):
    """
    Lefse tool
    """
    def __init__(self, config):
        super(LefseTool, self).__init__(config)
        self._version = '1.0.1'
        self.biom_path = "Python/bin/"
        self.script_path = "meta/scripts/"
        self.plot_lefse_path = "meta/lefse/"
        self.set_environ(PATH="/mnt/ilustre/users/sanger/app/R-3.2.2/bin:$PATH")
        self.set_environ(R_HOME="/mnt/ilustre/users/sanger/app/R-3.2.2/lib64/R/")
        self.set_environ(LD_LIBRARY_PATH="/mnt/ilustre/users/sanger/app/R-3.2.2/lib64/R/lib:$LD_LIBRARY_PATH")

    def run_biom(self):
        self.add_state("biom_start", data="开始生成biom格式文件")
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR+"/gcc/5.1.0/lib64:$LD_LIBRARY_PATH")
        biom_cmd = self.biom_path + "biom convert -i %s -o otu_taxa_table.biom --table-type \"OTU table\" --process" \
                                    "-obs-metadata taxonomy  --to-hdf5" % self.option('lefse_input').prop["path"]
        self.logger.info("开始运行biom_cmd")
        biom_command = self.add_command("biom_cmd", biom_cmd).run()
        self.wait(biom_command)
        if biom_command.return_code == 0:
            self.logger.info("biom_cmd运行完成")
        else:
            self.set_error("biom_cmd运行出错!")
        self.add_state("biom_finish", data="生成biom格式文件")

    def run_script(self):
        self.add_state("sum_taxa_start", data="开始生成每一水平的物种统计文件")
        script_cmd = self.script_path + "summarize_taxa.py -i otu_taxa_table.biom -o tax_summary_a -L 1,2,3,4,5,6,7 -a"
        print script_cmd
        self.logger.info("开始运行script_cmd")
        script_command = self.add_command("script_cmd", script_cmd).run()
        self.wait(script_command)
        if script_command.return_code == 0:
            self.logger.info("script_cmd运行完成")
        else:
            self.set_error("script_cmd运行出错!")

    def run_sum_tax(self):
        cmd = "for ((i=1;i<=7;i+=1)){\n\
            /mnt/ilustre/users/sanger/app/meta/scripts/sum_tax.fix.pl -i tax_summary_a/otu_taxa_table_L$i.txt " \
              "-o tax_summary_a/otu_taxa_table_L$i.stat.xls\n\
            mv tax_summary_a/otu_taxa_table_L$i.txt.new tax_summary_a/otu_taxa_table_L$i.txt\n\
        }"
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("run_sum_tax运行完成")
            self.add_state("sum_taxa_finish", data="生成每一水平的物种统计文件完成")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("run_sum_tax运行出错")
            return False

    def format_input(self):
        self.add_state("lefse_start", data="开始进行lefse分析")
        
        if self.option('lefse_gname') == 'None':
            plot_cmd = 'Python/bin/python ' + self.config.SOFTWARE_DIR + '/' + self.plot_lefse_path + \
                       "lefse-input.py -i tax_summary_a -g %s -o lefse_input.txt" % \
                       self.option('lefse_group').prop['path']
        else:
            glist = self.option('lefse_gname').split(',')
            self.option('lefse_group').sub_group('./lefse_group', glist)
            plot_cmd = 'Python/bin/python ' + self.config.SOFTWARE_DIR + '/' + self.plot_lefse_path + \
                       "lefse-input.py -i tax_summary_a -g ./lefse_group -o lefse_input.txt" 
        self.logger.info("开始运行format_input_cmd")
        plot_command = self.add_command("format_input_cmd", plot_cmd).run()
        self.wait(plot_command)
        if plot_command.return_code == 0:
            self.logger.info("format_input_cmd运行完成")
        else:
            self.set_error("plot_cmd运行出错!")
        
    def run_lefse(self):
        cmd = 'Python/bin/python /mnt/ilustre/users/sanger/app/meta/lefse/run_lefse.py lefse_format.txt lefse_LDA.xls ' \
              '-l %s -y %s' % (self.option("lda_filter"), self.option("strict"))
        self.logger.info("开始运行run_lefse_cmd")
        self.logger.info(cmd)
        command = self.add_command("run_lefse_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("run_lefse_cmd运行完成")
        else:
            self.logger.info("run_lefse_cmd运行出错")

    def plot_res(self):
        cmd = 'Python/bin/python /mnt/ilustre/users/sanger/app/meta/lefse/plot_res.py lefse_LDA.xls lefse_LDA.png' \
              ' --dpi 300 --format png --width 15'
        self.logger.info("开始运行plot_res_cmd")
        command = self.add_command("plot_res_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("plot_res_cmd运行完成")
        else:
            self.logger.info("plot_res_cmd运行出错")

    def plot_cladogram(self):
        cmd = 'Python/bin/python /mnt/ilustre/users/sanger/app/meta/lefse/plot_cladogram.py lefse_LDA.xls ' \
              'lefse_LDA.cladogram.png' ' --format png'
        self.logger.info("开始运行plot_cladogram_cmd")
        command = self.add_command("plot_cladogram_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("plot_cladogram_cmd运行完成")
        else:
            self.logger.info("plot_cladogram_cmd运行出错")
        self.add_state("lefse_finish", data="lefse分析完成")
    
    def set_lefse_output(self):
        """
        将结果文件链接至output
        """
        os.system('sed -i "1i\\taxon\tmean\tgroup\tlda\tpvalue" %s' % (self.work_dir + '/lefse_LDA.xls'))
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        os.link(self.work_dir + '/lefse_LDA.cladogram.png', self.output_dir + '/lefse_LDA.cladogram.png')
        # self.option('clado').set_path(self.output_dir+'/lefse_LDA.cladogram.pdf')
        os.link(self.work_dir + '/lefse_LDA.png', self.output_dir + '/lefse_LDA.png')
        # self.option('LDA').set_path(self.output_dir+'/lefse_LDA.pdf')
        os.link(self.work_dir + '/lefse_LDA.xls', self.output_dir + '/lefse_LDA.xls')
        # self.option('lefse_xls').set_path(self.output_dir+'/lefse_LDA.xls')

    def run(self):
        super(LefseTool, self).run()
        self.run_biom()
        self.run_script()
        self.run_sum_tax()
        self.format_input()
        self.run_lefse()
        self.plot_res()
        self.plot_cladogram()
        self.set_lefse_output()
        self.end()
