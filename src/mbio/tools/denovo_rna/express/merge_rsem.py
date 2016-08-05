# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re

class MergeRsemAgent(Agent):
    """
    调用align_and_estimate_abundance.pl脚本，运行rsem，进行表达量计算分析
    version v1.0
    author: qiuping
    last_modify: 2016.06.20
    """
    def __init__(self, parent):
        super(MergeRsemAgent, self).__init__(parent)
        options = [
            {"name": "rsem_files", "type": "infile", "format": "denovo_rna.express.rsem_dir"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "tran_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "tran_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "exp_way", "type": "string", "default": "fpkm"}
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
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)

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
        self.end()
