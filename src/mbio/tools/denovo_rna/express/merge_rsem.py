# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.express.express_distribution import *
import os
import re


class MergeRsemAgent(Agent):
    """
    调用abundance_estimates_to_matrix.pl脚本，将各个样本表达量结果合成表达量矩阵，其中有进行标准化等分析，并计算表达量分布图的作图数据
    version v1.0
    author: qiuping
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(MergeRsemAgent, self).__init__(parent)
        options = [
            {"name": "rsem_files", "type": "infile", "format": "denovo_rna.express.rsem_dir"},  # 包含所有样本的rsem结果文件的文件夹
            {"name": "tran_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 转录本计数矩阵
            {"name": "gene_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 基因计数矩阵
            {"name": "tran_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 转录本表达量矩阵
            {"name": "gene_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 基因表达量矩阵
            {"name": "exp_way", "type": "string", "default": "fpkm"}  # 表达量衡量指标
        ]
        self.add_option(options)
        self.step.add_steps("rsem")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.rsem.start()
        self.step.update()

    def stepfinish(self):
        self.step.rsem.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('rsem_files'):
            raise OptionError('必须设置输入文件：rsem结果文件')
        if self.option("exp_way") not in ['fpkm', 'tpm']:
            raise OptionError("所设表达量的代表指标不在范围内，请检查")
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
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"matrix$", "xls", "表达量矩阵"]
        ])
        super(MergeRsemAgent, self).end()


class MergeRsemTool(Tool):
    """
    Lefse tool
    """
    def __init__(self, config):
        super(MergeRsemTool, self).__init__(config)
        self._version = '1.0.1'
        self.fpkm = "/bioinfo/rna/scripts/abundance_estimates_to_matrix.pl"
        self.tpm = "/bioinfo/rna/trinityrnaseq-2.2.0/util/abundance_estimates_to_matrix.pl"
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.r_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin:$PATH"
        self._r_home = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/"
        self._LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/lib:$LD_LIBRARY_PATH"
        self.set_environ(PATH=self.r_path, R_HOME=self._r_home, LD_LIBRARY_PATH=self._LD_LIBRARY_PATH)
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.r_path1 = "/program/R-3.3.1/bin/Rscript"

    def merge_rsem(self):
        files = os.listdir(self.option('rsem_files').prop['path'])
        if self.option('exp_way') == 'fpkm':
            merge_gene_cmd = self.fpkm + ' --est_method RSEM --out_prefix genes '
            merge_tran_cmd = self.fpkm + ' --est_method RSEM --out_prefix transcripts '
        else:
            merge_gene_cmd = self.tpm + ' --est_method RSEM --out_prefix genes '
            merge_tran_cmd = self.tpm + ' --est_method RSEM --out_prefix transcripts '
        for f in files:
            if re.search(r'genes\Wresults$', f):
                merge_gene_cmd += '{} '.format(self.option('rsem_files').prop['path'] + '/' + f)
            elif re.search(r'isoforms\Wresults$', f):
                merge_tran_cmd += '{} '.format(self.option('rsem_files').prop['path'] + '/' + f)
        self.logger.info(merge_tran_cmd)
        self.logger.info(merge_gene_cmd)
        self.logger.info("开始运行merge_gene_cmd")
        self.logger.info("开始运行merge_tran_cmd")
        gene_com = self.add_command("merge_gene_cmd", merge_gene_cmd).run()
        self.wait(gene_com)
        if gene_com.return_code == 0:
            self.logger.info("运行merge_gene_cmd成功")
        else:
            self.logger.info("运行merge_gene_cmd出错")
            raise Exception("运行merge_gene_cmd出错")
        tran_com = self.add_command("merge_tran_cmd", merge_tran_cmd).run()
        self.wait(tran_com)
        if tran_com.return_code == 0:
            self.logger.info("运行merge_tran_cmd成功")
        else:
            self.logger.info("运行merge_tran_cmd出错")
            raise Exception("运行merge_tran_cmd出错")

    def get_distribution(self):
        """获取表达量分布图的作图数据"""
        # gene
        distribution(rfile='./gene_distribution.r', input_matrix=self.option('gene_fpkm').prop['path'], outputfile='./gene_distribution.xls')
        # transcript
        distribution(rfile='./tran_distribution.r', input_matrix=self.option('tran_fpkm').prop['path'], outputfile='./tran_distribution.xls')
        gcmd = self.r_path1 + " gene_distribution.r"
        tcmd = self.r_path1 + " tran_distribution.r"
        self.logger.info("开始运行表达量分布图的数据分析")
        cmd1 = self.add_command("gene_cmd", gcmd).run()
        cmd2 = self.add_command("tran_cmd", tcmd).run()
        self.wait()
        if cmd1.return_code == 0 and cmd2.return_code == 0:
            self.logger.info("表达量分布图的数据分析成功")
        else:
            self.set_error("表达量分布图的数据分析出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置merge_rsem结果目录")
        results = os.listdir(self.work_dir)
        try:
            for f in results:
                if re.search(r'^(transcripts\.TMM)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('tran_fpkm').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(genes\.TMM)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('gene_fpkm').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(transcripts\.count)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('tran_count').set_path(self.output_dir + '/' + f)
                elif re.search(r'^(genes\.count)(.+)(matrix)$', f):
                    os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
                    self.option('gene_count').set_path(self.output_dir + '/' + f)
            self.logger.info("设置merge_rsem分析结果目录成功")
        except Exception as e:
            self.logger.info("设置merge_rsem分析结果目录失败{}".format(e))

    def run(self):
        super(MergeRsemTool, self).run()
        self.merge_rsem()
        self.set_output()
        self.get_distribution()
        self.end()
