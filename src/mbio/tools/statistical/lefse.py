# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import subprocess
import re
import pandas as pd
import pandas as pd


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
            {"name":"lefse_type","type":"string","default":"meta_taxon"},###meta_taxon,metagenome_taxon,其他
            {"name": "strict", "type": "int", "default": 0},
            {"name": "lefse_gname", "type": "string"},
            {"name": "start_level", "type": "int", "default": 3},
            {"name": "end_level", "type": "int", "default": 7},
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
        if not self.option("lefse_gname"):
            raise OptionError("必须设置lefse分组方案名称")
        if self.option("strict") not in [0, 1]:
            raise OptionError("所设严格性超出范围值")
        if self.option("lda_filter") > 4.0 or self.option("lda_filter") < -4.0:
            raise OptionError("所设阈值超出范围值")
        if len(self.option('lefse_gname').split(',')) >= 3:
            raise OptionError("lefse分析不支持大于2个的分组方案")
        for i in self.option('lefse_gname').split(','):
            gnum = self.option('lefse_group').group_num(i)
            if gnum < 2:
                raise OptionError("lefse分析分组类别必须大于2")
        if self.option('start_level') not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            raise OptionError('起始分类水平不在范围内')
        if self.option('end_level') not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            raise OptionError('结束分类水平不在范围内')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '10G'

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
        self.biom_path = "/program/Python/bin/"
        self.python_path = "/program/Python/bin/python"
        self.perl_path = self.config.SOFTWARE_DIR + "/program/perl/perls/perl-5.24.0/bin/perl"
        self.sum_taxa_path = "/program/Python/bin/"
        self.script_path = "/bioinfo/taxon/scripts/"
        self.plot_lefse_path = self.config.SOFTWARE_DIR + "/bioinfo/statistical/lefse/"
        self._path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin:$PATH"
        self._r_home = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/"
        self._LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/lib:$LD_LIBRARY_PATH"
        self.set_environ(PATH=self._path, R_HOME=self._r_home, LD_LIBRARY_PATH=self._LD_LIBRARY_PATH)
        self.end_level = 7
        self.start_level = 3

    def run_new_otufile(self):
        """
         对于非taxon的丰度表，我们需要进行特殊处理，给他添加人为分类。
         author：gaohao
         last_modify: 2017.10.18
        :return:
        """
        self.add_state("otufile_start", data="开始生成新otu格式文件")
        otufile_cmd = self.python_path + "%slefse_for_biom.py -i %s -o otufile_input.xls " % (self.script_path,self.option('lefse_input').prop["path"])
        self.logger.info("开始运行otufile_cmd")
        otufile_command = self.add_command("otufile_cmd", otufile_cmd).run()
        self.wait(otufile_command)
        if otufile_command.return_code == 0:
            self.logger.info("otufile_cmd运行完成")
        else:
            self.set_error("otufile_cmd运行出错!")
        self.add_state("otufile_finish", data="生成新的otu格式文件")

    def run_biom(self):
        self.add_state("biom_start", data="开始生成biom格式文件")
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR+"/gcc/5.1.0/lib64:$LD_LIBRARY_PATH")
        biom_cmd = self.biom_path + "biom convert -i otufile_input.xls -o otu_taxa_table.biom --table-type \"OTU table\" --process" \
                                    "-obs-metadata taxonomy  --to-hdf5"
        self.logger.info("开始运行biom_cmd")
        biom_command = self.add_command("biom_cmd", biom_cmd).run()
        self.wait(biom_command)
        if biom_command.return_code == 0:
            self.logger.info("biom_cmd运行完成")
        else:
            self.set_error("biom_cmd运行出错!")
        self.add_state("biom_finish", data="生成biom格式文件")

    def run_meta_biom(self):
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
        if self.option('end_level') >= self.option('start_level'):
            self.start_level = self.option('start_level')
            self.end_level = self.option('end_level')
        else:
            self.end_level = self.option('start_level')
            self.start_level = self.option('end_level')
        if self.end_level == 9:
            level = ','.join([str(i) for i in range(self.start_level, self.end_level)])
        else:
            level = ','.join([str(i) for i in range(self.start_level, self.end_level + 1)])
        self.logger.info(level)
        if self.start_level != 9:
            script_cmd = self.python_path + " %ssummarize_taxa.py -i otu_taxa_table.biom " \
                                            "-o tax_summary_a -L %s -a" % (self.config.SOFTWARE_DIR + self.script_path, level)
            self.logger.info("开始运行script_cmd")
            script_command = self.add_command("script_cmd", script_cmd).run()
            self.wait(script_command)
            if script_command.return_code == 0:
                self.logger.info("script_cmd运行完成")
            else:
                self.set_error("script_cmd运行出错!")
                raise Exception("script_cmd运行出错!")
        self.get_otu_taxon()
        self.remove_parent_otu(self.work_dir + '/tax_summary_a')

    def remove_parent_otu(self, tax_summary_a):
        files = os.listdir(tax_summary_a)
        for i in files:
            if re.search(r'txt$', i):
                _path = os.path.join(tax_summary_a, i)
                if i == 'otu_taxa_table_L9.txt':
                    df = pd.read_table(_path, index_col=0, skiprows=None)
                else:
                    df = pd.read_table(_path, index_col=0, skiprows=1)
                tmp = df.rename(index=lambda x: ';'.join(x.split(';')[self.start_level - 1:]))
                tmp.to_csv(_path, sep='\t')

    def get_otu_taxon(self):
        if self.end_level == 9:
            if not os.path.exists(self.work_dir + '/tax_summary_a'):
                os.mkdir(self.work_dir + '/tax_summary_a')
            otu_taxon_otu = os.path.join(self.work_dir + '/tax_summary_a', "otu_taxa_table_L9.txt")
            with open(self.option('lefse_input').prop['path'], 'r') as r:
                with open(otu_taxon_otu, 'w') as w:
                    line1 = r.next()
                    if re.search(r'Constructed from biom', line1):
                        line1 = r.next()
                    w.write(line1)
                    for line in r:
                        line = re.sub(r'\.0', '', line)
                        line = line.strip('\n').split('\t')
                        name = line[-1].split('; ')
                        name.append(line[0])
                        line[0] = ';'.join(name)
                        line = '\t'.join(line[0:-1]) + '\n'
                        w.write(line)
        else:
            pass

    # def run_sum_tax(self):
    #     cmd = "for ((i=%s;i<=%s;i+=1)){\n\
    #         %s %ssum_tax.fix.pl -i tax_summary_a/otu_taxa_table_L$i.txt " \
    #           "-o tax_summary_a/otu_taxa_table_L$i.stat.xls\n\
    #         mv tax_summary_a/otu_taxa_table_L$i.txt.new tax_summary_a/otu_taxa_table_L$i.txt\n\
    #     }" % (self.start_level, self.end_level, self.perl_path, self.config.SOFTWARE_DIR + self.script_path)
    #     try:
    #         subprocess.check_output(cmd, shell=True)
    #         self.logger.info("run_sum_tax运行完成")
    #         self.get_otu_taxon()
    #         self.remove_parent_otu(self.work_dir + '/tax_summary_a')
    #         self.add_state("sum_taxa_finish", data="生成每一水平的物种统计文件完成")
    #     except subprocess.CalledProcessError:
    #         self.logger.info("run_sum_tax运行出错")
    #         raise Exception("run_sum_tax运行出错")

    def format_input(self):
        self.add_state("lefse_start", data="开始进行lefse分析")
        glist = self.option('lefse_gname').split(',')
        self.option('lefse_group').sub_group('./lefse_group', glist)
        plot_cmd = self.python_path + ' ' + self.plot_lefse_path + \
                   "lefse-input.py -i tax_summary_a -g ./lefse_group -o lefse_input.txt"
        self.logger.info("开始运行format_input_cmd")
        plot_command = self.add_command("format_input_cmd", plot_cmd).run()
        self.wait(plot_command)
        if plot_command.return_code == 0:
            self.logger.info("format_input_cmd运行完成")
        else:
            self.set_error("format_input_cmd运行出错!")

    def run_format(self):
        if len(self.option('lefse_gname').split(',')) == 1:
            format_cmd = self.python_path + " " + self.plot_lefse_path + 'format_input.py  lefse_input.txt  lefse_format.txt  -f  r -c 1 -u 2 -o 1000000'
        elif len(self.option('lefse_gname').split(',')) == 2:
            format_cmd = self.python_path + " " + self.plot_lefse_path + 'format_input.py  lefse_input.txt  lefse_format.txt  -f  r -c 1 -s 2 -u 3 -o 1000000'
        self.logger.info("开始运行format_cmd")
        format_command = self.add_command("format_cmd", format_cmd).run()
        self.wait(format_command)
        if format_command.return_code == 0:
            self.logger.info("format_cmd运行完成")
        else:
            self.set_error("format_cmd运行出错!")

    def run_lefse(self):
        cmd = self.python_path + ' %srun_lefse.py lefse_format.txt lefse_LDA.xls ' \
              '-l %s -y %s' % (self.plot_lefse_path, self.option("lda_filter"), self.option("strict"))
        self.logger.info("开始运行run_lefse_cmd")
        self.logger.info(cmd)
        command = self.add_command("run_lefse_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("run_lefse_cmd运行完成")
        else:
            self.logger.info("run_lefse_cmd运行出错")

    def run_lefse_cmd_check(self, command, line):
        if re.search(r"Error\sin\slda\Wdefault", line):
            command.kill()
            self.set_error("该分组方案的分组类别所含样本量小于3，lda分析出错")

    def plot_res(self):
        cmd = self.python_path + ' %splot_res.py lefse_LDA.xls lefse_LDA.png' \
              ' --dpi 300 --format png --width 20' % (self.plot_lefse_path)
        self.logger.info("开始运行plot_res_cmd")
        command = self.add_command("plot_res_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("plot_res_cmd运行完成")
        else:
            self.logger.info("plot_res_cmd运行出错")

    def plot_cladogram(self):
        cmd = '%s %splot_cladogram.py lefse_LDA.xls ' \
              'lefse_LDA.cladogram.png' ' --format png' % (self.python_path ,self.plot_lefse_path)
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
        os.system('cp %s %s' % (self.work_dir + '/lefse_LDA.xls', self.work_dir + '/lefse_lda_head.xls'))
        os.system('sed -i "1i\\taxon\tmean\tgroup\tlda\tpvalue" %s' % (self.work_dir + '/lefse_lda_head.xls'))
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        # os.link(self.work_dir + '/lefse_LDA.cladogram.png', self.output_dir + '/lefse_LDA.cladogram.png')
        # os.link(self.work_dir + '/lefse_LDA.png', self.output_dir + '/lefse_LDA.png')
        os.link(self.work_dir + '/lefse_lda_head.xls', self.output_dir + '/lefse_LDA.xls')

    def set_lefse_function_output(self):
        """
        将结果文件链接至output
        """
        os.system('cp %s %s' % (self.work_dir + '/lefse_LDA.xls', self.work_dir + '/lefse_lda_head.xls'))
        os.system('grep "Other" %s -v |sed "s/p__//g" > %s ' %(self.work_dir + '/lefse_lda_head.xls', self.work_dir + '/lefse_lda_head2.xls'))
        os.system('sed -i "1i\\taxon\tmean\tgroup\tlda\tpvalue" %s' % (self.work_dir + '/lefse_lda_head2.xls'))
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        os.link(self.work_dir + '/lefse_lda_head2.xls', self.output_dir + '/lefse_LDA.xls')

    def run(self):
        super(LefseTool, self).run()
        if(self.option('lefse_type') == 'meta_taxon'):
            self.run_meta_biom()
            self.run_script()
            self.format_input()
            self.run_format()
            self.run_lefse()
            self.set_lefse_output()
            self.end()
        elif (self.option('lefse_type') == 'metagenome_taxon'):
            self.run_meta_biom()
            self.run_script()
            self.format_input()
            self.run_format()
            self.run_lefse()
            self.set_lefse_output()
            self.end()
        else:
            self.run_new_otufile()
            self.run_biom()
            self.run_script()
            self.format_input()
            self.run_format()
            self.run_lefse()
            self.set_lefse_function_output()
            self.end()


