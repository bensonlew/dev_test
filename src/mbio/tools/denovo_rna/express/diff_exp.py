# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.express.get_diff_matrix import *
from mbio.packages.denovo_rna.express.edger_stat import *
import os
import re
import itertools
import shutil

class DiffExpAgent(Agent):
    """
    调用align_and_estimate_abundance.pl脚本，运行rsem，进行表达量计算分析
    version v1.0
    author: qiuping
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(DiffExpAgent, self).__init__(parent)
        options = [
            {"name": "count", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 输入文件，基因技术矩阵
            {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #输入文件，基因表达量矩阵
            {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts",  "type": "int", "default": 20},  # 离散值估计检验的最小计数值
            {"name": "edger_group", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "sample_list", "type": "string", "default": ''},  # 选择计算表达量的样本名，多个样本用‘，’隔开,有重复时没有该参数
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  #对照组文件，格式同分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "diff_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  #差异基因计数表
            {"name": "diff_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  #差异基因表达量表
            {"name": "gname", "type": "string"},  #  分组方案名称
            {"name": "diff_rate", "type": "float", "default": 0.01}  #期望的差异基因比率
        ]
        self.add_option(options)
        self.step.add_steps("diff_exp")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.diff_exp.start()
        self.step.update()

    def stepfinish(self):
        self.step.diff_exp.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("count").is_set:
            raise OptionError("必须设置输入文件:基因计数表")
        if not self.option("fpkm").is_set:
            raise OptionError("必须设置输入文件:基因表达量表")
        if not self.option('control_file').is_set:
            raise OptionError("必须设置输入文件：上下调对照组参考文件")
        if self.option("diff_ci") >= 1 or self.option("diff_ci") <= 0:
            raise OptionError("显著性水平不在(0,1)范围内")
        if self.option("diff_rate") > 1 or self.option("diff_rate") <= 0:
            raise OptionError("期望的差异基因比率不在(0，1]范围内")
        if self.option("sample_list") != '' and self.option("edger_group").is_set:
            raise OptionError("有生物学重复时不可设sample_list参数")
        samples, genes = self.option('count').get_matrix_info()
        # num, control_vs = self.option('control_file').get_control_info()
        if self.option("sample_list") != '':
            sam = self.option("sample_list").split(',')
            for i in sam:
                if i not in samples:
                    raise OptionError("传入的样本列表里的样本%s不在fpkm表里" % i)
        if self.option("sample_list") != '':
            vs_list = list(itertools.permutations(sam, 2))
        elif self.option("edger_group").is_set:
            gnames = self.option('edger_group').get_group_name(self.option('gname'))
            vs_list = list(itertools.permutations(gnames, 2))
        else:
            vs_list = list(itertools.permutations(samples, 2))
        # print samples,vs_list,num,control_vs
        # if num != len(vs_list) / 2:
        #     raise OptionError("缺少对照样本名")
        for n in self.option('control_file').prop['vs_list']:
            if n not in vs_list:
                raise OptionError("对照样本名在fpkm表中不存在")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            # ["./edger_stat", "", "edger统计结果目录"],
            [r"diff_fpkm", "xls", "差异基因表达量表"],
            [r"diff_count", "xls", "差异基因计数表"]
        ])
        result_dir.add_regexp_rules([
            [r"_edgr_stat\.xls$", "xls", "edger统计结果文件"]
            ])
        super(DiffExpAgent, self).end()


class DiffExpTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(DiffExpTool, self).__init__(config)
        self._version = '1.0.1'
        self.edger = "/bioinfo/rna/trinityrnaseq-2.2.0/Analysis/DifferentialExpression/run_DE_analysis.pl"

    def run_edger(self):
        if self.option('edger_group').is_set:
            self.option('edger_group').get_edger_group([self.option('gname')], './edger_group')
            edger_cmd = self.edger + " --matrix %s --method edgeR --samples_file %s --output edger_result --min_rowSum_counts %s" % (self.option('count').prop['path'], './edger_group', self.option('min_rowsum_counts'))
        else:
            edger_cmd = self.edger + " --matrix %s --method edgeR --dispersion %s --output edger_result --min_rowSum_counts %s" % (self.option('count').prop['path'], self.option('dispersion'), self.option('min_rowsum_counts'))
        self.logger.info("开始运行edger_cmd")
        edger_com = self.add_command("edger_cmd", edger_cmd).run()
        self.wait(edger_com)
        if edger_com.return_code == 0:
            self.logger.info("运行edger_cmd成功")
            edger = os.listdir(self.work_dir + '/edger_result/')
            edger_files = ''
            for f in edger:
                if re.search(r'edgeR.DE_results$', f):
                    get_diff_list(self.work_dir + '/edger_result/' + f, self.work_dir + '/' + f.split('.')[3] + '_diff_list', self.option('diff_ci'))
                    edger_files += '%s ' % (self.work_dir + '/' + f.split('.')[3] + '_diff_list')
            os.system('cat %s> diff_list && sort diff_list | uniq' % edger_files)
        else:
            self.logger.info("运行edger_cmd出错")

    def re_run_edger(self):
        samples, genes = self.option('fpkm').get_matrix_info()
        gene_num = len(genes)
        self.logger.info(genes,gene_num)
        if not self.option('edger_group').is_set and gene_num > 10000:
            diff_num = len(open('diff_list','rb').readlines())
            dispersion = check_dispersion(genes, diff_num, self.option('diff_rate'))
            shutil.rmtree(self.work_dir + '/edger_result/')
            self.run_edger()
        else:
            pass

    def stat_egder(self):
        control_dict = self.option('control_file').get_control_dict()
        self.logger.info(str(control_dict))
        edger_results = os.listdir(self.work_dir + '/edger_result')
        for afile in edger_results:
            if re.search(r'edgeR.DE_results$', afile):
                for i in control_dict.keys():
                    con_keys = i.split('_vs_')
                    if con_keys[0] in afile and con_keys[1] in afile:
                        if self.option("edger_group").is_set:
                            stat_edger(self.work_dir + '/edger_result/' + afile, self.option('count').prop['path'], self.option('fpkm').prop['path'], control_dict[i][0], control_dict[i][1], self.output_dir + "/", './edger_group', self.option("diff_ci"), True)
                        else:
                            stat_edger(self.work_dir + '/edger_result/' + afile, self.option('count').prop['path'], self.option('fpkm').prop['path'], control_dict[i][0], control_dict[i][1], self.output_dir + "/", None, self.option("diff_ci"), True)
                    else:
                        control = afile.split('.')[3].split('_vs_')[0]
                        other = afile.split('.')[3].split('_vs_')[1]
                        if self.option("edger_group").is_set:
                            stat_edger(self.work_dir + '/edger_result/' + afile, self.option('count').prop['path'], self.option('fpkm').prop['path'], control, other, self.output_dir + "/", './edger_group', self.option("diff_ci"), False)
                        else:
                            stat_edger(self.work_dir + '/edger_result/' + afile, self.option('count').prop['path'], self.option('fpkm').prop['path'], control, other, self.output_dir + "/", None, self.option("diff_ci"), False)
            else:
                pass

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        try:
            self.logger.info("设置结果目录")
            get_diff_matrix(self.option('fpkm').prop['path'], self.output_dir + '/diff_list', self.output_dir + '/diff_fpkm')
            get_diff_matrix(self.option('count').prop['path'], self.output_dir + '/diff_list', self.output_dir + '/diff_count')
            self.option('diff_fpkm').set_path(self.output_dir + '/diff_fpkm')
            self.option('diff_count').set_path(self.output_dir + '/diff_count')
            self.logger.info("设置edger分析结果目录成功")
        except Exception as e:
            self.logger.info("设置edger分析结果目录失败{}".format(e))

    def run(self):
        super(DiffExpTool, self).run()
        self.run_edger()
        self.re_run_edger()
        self.stat_egder()
        self.set_output()
        self.end()
