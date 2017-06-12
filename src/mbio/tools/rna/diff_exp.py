# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.ref_rna.express.get_diff_matrix import *
from mbio.packages.ref_rna.express.diff_stat import * # 更改统计脚本位置
import os
import re
import itertools
import shutil
import copy
import numpy as np
import subprocess


class DiffExpAgent(Agent):
    """
    调用align_and_estimate_abundance.pl脚本，运行rsem，进行表达量计算分析
    version v1.0
    author: zhangpeng#主要在前辈基础上进行修改
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(DiffExpAgent, self).__init__(parent)
        options = [
            {"name": "count", "type": "infile", "format": "rna.express_matrix"},  # 输入文件，基因技术矩阵
            {"name": "fpkm", "type": "infile", "format": "rna.express_matrix"},  # 输入文件，基因表达量矩阵
            {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
            {"name": "edger_group", "type": "infile", "format": "sample.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "sample.control_table"},  # 对照组文件，格式同分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "method", "type": "string", "default": "edgeR"}, # 选择计算的软件
            {"name": "gname", "type": "string", "default": "none"},  # 分组方案名称
            {"name": "fc", "type": "float", "default": 2}, #log底数
            {"name": "diff_fdr_ci", "type": "float", "default": 0.05}, #fdr的选择
            {"name": "diff_rate", "type": "float", "default": 0.01},  # 期望的差异基因比率
            {"name": "diff_count", "type": "outfile", "format": "rna.express_matrix"},  # 差异基因计数表
            {"name": "diff_fpkm", "type": "outfile", "format": "rna.express_matrix"},  # 差异基因表达量表
            {"name": "diff_list", "type": "outfile", "format": "rna.gene_list"},  # 差异基因名称文件
            {"name": "diff_list_dir", "type": "outfile", "format": "rna.gene_list_dir"},
            {"name": "regulate_edgrstat_dir", "type": "outfile", "format": "rna.diff_stat_dir"},
            {"name":"pvalue_padjust","type":"string","default":"padjust"}  #按照pvalue(diff_ci)还是padjust(diff_fdr_ci)筛选
        ]
        self.add_option(options)
        self.step.add_steps("diff_exp")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)
        self.diff_gene = False

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
        if self.option("diff_fdr_ci") >= 1 or self.option("diff_fdr_ci") <= 0:
            raise OptionError("显著性水平不在(0,1)范围内")
        if self.option("fc") < 0:
            raise OptionError("显著性水平不能负数")
        if self.option("diff_rate") > 1 or self.option("diff_rate") <= 0:
            raise OptionError("期望的差异基因比率不在(0，1]范围内")
        if self.option("method") not in ("edgeR", "DESeq2", "DEGseq"):
            raise OptionError("差异分析软件不在许可范围内")
        samples, genes = self.option('count').get_matrix_info()
        if self.option("edger_group").is_set:
            if self.option('gname') == "none":
                self.option('gname', self.option('edger_group').prop['group_scheme'][0])
            gnames = self.option('edger_group').get_group_name(self.option('gname'))
            vs_list = list(itertools.permutations(gnames, 2))
        else:
            vs_list = list(itertools.permutations(samples, 2))
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
        self._memory = '5G'

    def diff_gene_callback(self):
        self.diff_gene = True

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [[".", "", "结果输出目录"]]
        if self.diff_gene:
            relpath += [
                ["diff_fpkm", "xls", "差异基因表达量表"],
                ["diff_count", "xls", "差异基因计数表"]
            ]
        result_dir.add_regexp_rules([
            [r"_edgr_stat\.xls$", "xls", "edger统计结果文件"]
        ])
        result_dir.add_relpath_rules(relpath)
        super(DiffExpAgent, self).end()


class DiffExpTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(DiffExpTool, self).__init__(config)
        self._version = '1.0.1'
        self.edger = "/bioinfo/rna/trinityrnaseq-2.2.0/Analysis/DifferentialExpression/run_DE_analysis8.pl"
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.r_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin:$PATH"
        self._r_home = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/"
        self._LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/lib:$LD_LIBRARY_PATH"
        self.set_environ(PATH=self.r_path, R_HOME=self._r_home, LD_LIBRARY_PATH=self._LD_LIBRARY_PATH)
        self.restart_edger = False
        self.diff_gene = False
        self.regulate_stat = self.work_dir + '/regulate_edgrstat_result'

    def run_edger(self, dispersion=None):
        if self.option('edger_group').is_set:
            self.option('edger_group').get_edger_group([self.option('gname')], './edger_group')
            edger_cmd = self.edger + " --matrix %s --method %s --dispersion %s --samples_file %s --output edger_result --min_rowSum_counts %s" % (self.option('count').prop['path'],self.option('method'), self.option('dispersion'), './edger_group', self.option('min_rowsum_counts'))
        else:
            edger_cmd = self.edger + " --matrix %s --method %s --dispersion %s --output edger_result --min_rowSum_counts %s" % (self.option('count').prop['path'], self.option('method'), self.option('dispersion'), self.option('min_rowsum_counts'))
            restart_edger_cmd = self.edger + " --matrix %s --method %s --dispersion %s --output edger_result --min_rowSum_counts %s" % (self.option('count').prop['path'],self.option('method'), dispersion, self.option('min_rowsum_counts'))
        self.logger.info("开始运行edger_cmd")
        if self.restart_edger:
            self.logger.info("开始运行重运行edger_cmd，校正dispersion")
            shutil.rmtree(self.work_dir + '/diff_list_dir/')
            edger_com = self.add_command("restart_edger_cmd", restart_edger_cmd).run()
        else:
            edger_com = self.add_command("edger_cmd", edger_cmd).run()
        self.wait(edger_com)
        if edger_com.return_code == 0:
            self.logger.info("运行edger_cmd成功")
            self.cat_diff_list(self.work_dir + '/edger_result/', self.work_dir + '/diff_list_dir/')
        else:
            self.set_error("运行edger_cmd出错")
            self.logger.info("运行edger_cmd出错")

    def cat_diff_list(self, edger_dir, output_dir):
        edger = os.listdir(edger_dir)
        edger_files = ''
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        else:
            shutil.rmtree(output_dir)
            os.mkdir(output_dir)
        for f in edger:
            if re.search(r'edgeR.DE_results$', f):
                get_diff_list(edger_dir + f, output_dir + f.split('.')[-3], self.option('diff_ci'))
                edger_files += '%s ' % (output_dir + f.split('.')[-3])
        os.system('cat %s> diff_lists && sort diff_lists | uniq > diff_list' % edger_files)
        os.remove('diff_lists')

    def re_run_edger(self):
        samples, genes = self.option('fpkm').get_matrix_info()
        gene_num = len(genes)
        if not self.option('edger_group').is_set and gene_num > 10000:
            diff_num = len(open('diff_list', 'rb').readlines())
            dispersion = check_dispersion(gene_num, diff_num, self.option('diff_rate'))
            shutil.rmtree(self.work_dir + '/edger_result/')
            self.restart_edger = True
            self.run_edger(dispersion)
        else:
            pass

    def run_stat_egder(self):
        edger_results = os.listdir(self.work_dir + '/edger_result')
        num, sams = self.option('control_file').get_control_info()  # sams为包含两两比较的样本（分组）元组的列表:[(对照，实验), (对照，实验)]
        group_info = None
        stat = DiffStat()
        stat.get_express_info(countfile=self.option('count').prop['path'], fpkmfile=self.option('fpkm').prop['path'])
        if self.option("edger_group").is_set:
            group_info = stat.get_group_info(self.option("edger_group").prop['path'])
        if not os.path.exists(self.regulate_stat):
            os.mkdir(self.regulate_stat)
        else:
            shutil.rmtree(self.regulate_stat)
            os.mkdir(self.regulate_stat)
        for i in sams:
            resluts = copy.copy(edger_results)
            for afile in resluts:
                if re.search(r'edgeR.DE_results$', afile):
                    if i[0] in afile and i[1] in afile:
                        # self.logger.info(afile)
                        stat.diff_stat(express_info=stat.express_info, edgr_result=self.work_dir + '/edger_result/' + afile, control=i[0], other=i[1], output=self.output_dir, group_info=group_info, regulate=True, diff_ci=self.option("diff_ci"),fc=self.option('fc'),diff_fdr_ci=self.option('diff_fdr_ci'),pvalue_padjust=self.option("pvalue_padjust"))  #确保按照fc过滤 ，按照diff_ci或diff_fdr_ci判断是否significant  #modify by khl
                        file_name = '/%s_vs_%s_edgr_stat.xls' % (i[0], i[1])
                        os.link(self.output_dir + file_name, self.regulate_stat + file_name)
                        edger_results.remove(afile)
                else:
                    pass
        # 统计不做上下调基因比较的差异信息
        for f in edger_results:
            if re.search(r'edgeR.DE_results$', f):
                self.logger.info(f)
                control = f.split('.')[-3].split('_vs_')[0]
                other = f.split('.')[-3].split('_vs_')[1]
                stat.diff_stat(express_info=stat.express_info, edgr_result=self.work_dir + '/edger_result/' + f, control=control, other=other, output=self.output_dir, group_info=group_info, regulate=False, diff_ci=self.option("diff_ci"), fc=self.option("fc"), diff_fdr_ci=self.option("diff_fdr_ci"),pvalue_padjust=self.option('pvalue_padjust')) #modify by khl

    def merge(self):
        """
        整合输出数据，张鹏增加
        """
        edger_results = os.listdir(self.work_dir + '/regulate_edgrstat_result')
        edger_results_path = self.work_dir + '/regulate_edgrstat_result'
        data = {}
        print edger_results
        sample_name = []
        _gene_length = {}
        for files in edger_results:
            #print files
            #print edger_results_path
            file_path = os.path.join(edger_results_path, files)
            print 'aaaaaaaaaaaaa'
            _sample_name = str(files.split("_edgr_stat.xls")[0])
            #print _sample_name
            sample_name.append(_sample_name)
            with open(file_path, "r+") as f:
                f.readline()
                for line in f:
                    line1 = line.strip().split("\t")
                    #print line1
                    if str(line1[0]) not in data.keys():
                        data[str(line1[0])] = {}
                    if str(_sample_name) not in data[str(line1[0])].keys():
                        data[str(line1[0])][_sample_name]={}
                        data[str(line1[0])][_sample_name]["count"] = line1[-3]
                    #print line1[-3]
        with open(self.work_dir + '/table_merge.xls','w+') as f1:
            f1.write("Gene_ID" + "\t"+"\t".join(sample_name) + "\n")
            for keys in data.keys():
                values = data[keys]
                count = []
                for single_name in sample_name:
                    if single_name not in values.keys():
                        count.append(str(0))
                    else:
                        count.append(str(values[single_name]["count"]))
                #print values[single_name]
                #m = values.values()
                #print m.values()
                #print values.values().count('{\'count\': \'no\'}')
                f1.write(keys+"\t"+"\t".join(count)+"\n")
        #cmd = '{}/program/perl/perls/perl-5.24.0/bin/perl {}/bioinfo/meta/scripts/plot-bubble.pl '.format(self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        #with open(self.work_dir + '/table_merge.xls', 'r+') as f2:
            #f2.readline()
            #for f21 in f2:
                #f21.replace('yes',1)
                #f21.replace('no',0)
            #print f2
            #tmp = np.loadtxt(self.work_dir + '/table_merge.xls', dtype=np.str, delimiter="\t")
            #tmp1 = tmp[1:,]
                #f2.readline()
                #i = 0
                #for line2 in f2:
                    #if i = 0:
                        #i = 1
                        #f3.write("Gene_ID" + "\t"+"\t".join(sample_name) + "\n")
                


    def R_merge(self):
        cmd = '{}/program/perl/perls/perl-5.24.0/bin/perl {}/bioinfo/statistical/scripts/merge.pl '.format(self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd += '-o %s ' %(self.work_dir)
        self.logger.info('运行程序')
        self.logger.info(cmd)

        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 失败')
            self.set_error('无法生成 cmd.r 文件')

        try:
            subprocess.check_output(self.config.SOFTWARE_DIR + '/program/R-3.3.1/bin/R --restore --no-save < %s/merge_cmd.r' % (self.work_dir), shell=True)
            self.logger.info('生成成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成失败')
            self.set_error('R运行生成error')
            raise "运行R脚本失败"

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        try:
            self.logger.info("设置结果目录")
            self.option('regulate_edgrstat_dir', self.regulate_stat)
            if os.path.getsize(self.work_dir + '/diff_list') != 0:
                get_diff_matrix(self.option('fpkm').prop['path'], self.work_dir + '/diff_list', self.output_dir + '/diff_fpkm')
                get_diff_matrix(self.option('count').prop['path'], self.work_dir + '/diff_list', self.output_dir + '/diff_count')
                self.diff_gene = True
                self.add_state('diff_gene')
                self.option('diff_fpkm', self.output_dir + '/diff_fpkm')
                self.option('diff_count', self.output_dir + '/diff_count')
                self.option('diff_list', self.work_dir + '/diff_list')
                files = os.listdir(self.work_dir + '/diff_list_dir/')
                for f in files:
                    if not os.path.getsize(self.work_dir + '/diff_list_dir/' + f):
                        os.remove(self.work_dir + '/diff_list_dir/' + f)
                self.option('diff_list_dir', self.work_dir + '/diff_list_dir/')
                self.logger.info("设置edger分析结果目录成功")
            else:
                self.logger.info('输入的fpkm表没有检测到差异基因')
        except Exception as e:
            self.set_error("设置edger分析结果目录失败{}".format(e))
            self.logger.info("设置edger分析结果目录失败{}".format(e))

    def run(self):
        super(DiffExpTool, self).run()
        self.run_edger()
        self.re_run_edger()
        self.run_stat_egder()
        self.merge()#张鹏增加
        self.R_merge()
        self.set_output()
        self.end()
