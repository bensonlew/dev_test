# -*- coding: utf-8 -*-
import os
import itertools
import shutil
import copy
import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class DiffExpNewAgent(Agent):
    """
    Differential Analysis
    """
    def __init__(self, parent):
        super(DiffExpNewAgent, self).__init__(parent)
        options = [
            # input counts table and express table
            dict(name="count", type="infile", format="rna.express_matrix"),
            dict(name="exp", type="infile", format="rna.express_matrix"),
            # cutoff values setting
            dict(name="dispersion", type="float", default=0.1),  # used for cmp between samples
            dict(name="pvalue_cutoff", type="float", default=0.05),
            dict(name="fdr_cutoff", type="float", default=0.05),
            dict(name="fc_cutoff", type="float", default=2.0),
            dict(name="stat_judge", type="string", default="padjust"),
            # compare detail
            dict(name="group_info", type="infile", format="sample.group_table"),
            dict(name="cmp_info", type="infile", format="sample.control_table"),
            dict(name="group_scheme_name", type="string", default="none"),
            # method selection
            dict(name="method", type="string", default="DESeq2"),
            # check the DEGs number resulted from of edgeR
            dict(name="diff_ratio", type="float", default=0.01),
            # set output setting
            dict(name="diff_count", type="outfile", format="rna.express_matrix"),
            dict(name="diff_fpkm", type="outfile", format="rna.express_matrix"),
            dict(name="diff_list", type="outfile", format="rna.gene_list"),
            dict(name="diff_list_dir", type="outfile", format="rna.gene_list_dir"),
            dict(name="diffexp_result", type="outfile", format="rna.diff_stat_dir"),
        ]
        self.add_option(options)

    def check_options(self):
        """
        check parameters
        """
        if not (0 <= self.option("pvalue_cutoff") <= 1):
            raise OptionError("pvalue cutoff is not in [0-1]")
        if not (0 <= self.option("fdr_cutoff") <= 1):
            raise OptionError("adjusted pvalue cutoff is not in [0-1]")
        if self.option("fc_cutoff") < 0:
            raise OptionError("fold change cutoff should not be negative")
        if not (0 <= self.option("diff_ratio") <= 1):
            raise OptionError("differentially expressed gene ratio is not in [0-1]")
        if self.option("method") not in ("edgeR", "DESeq2", "DEGseq"):
            raise OptionError("method not among ( edgeR DESeq2 DEGseq )")

        samples, genes = self.option('count').get_matrix_info()
        if self.option("group_info").is_set:
            if self.option('group_scheme_name') == "none":
                self.option('group_scheme_name', self.option('group_info').prop['group_scheme'][0])
            group_names = self.option('group_info').get_group_name(self.option('group_scheme_name'))
            cmp_list = list(itertools.permutations(group_names))
        else:
            cmp_list = list(itertools.permutations(samples, 2))

        for n in self.option('cmp_info').prop['vs_list']:
            if n not in cmp_list:
                raise OptionError("Comparison information not match Count-table")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '5G'

    def end(self):
        super(DiffExpNewAgent, self).end()


class DiffExpNewTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(DiffExpNewTool, self).__init__(config)
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.r_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin:$PATH"
        self._r_home = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/"
        self._LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/lib:$LD_LIBRARY_PATH"
        self.set_environ(PATH=self.r_path, R_HOME=self._r_home, LD_LIBRARY_PATH=self._LD_LIBRARY_PATH)
        self.result_dir = self.work_dir + '/diffexp_result'

    def DEGseq(self, count_table, cmp_info, stat_value=0.001, fold_change=1, sep='\t',
               method='MARS', thresholdKind=5):
        """
        Differential Analysis with DEGseq. Currently, Only MARS method are Supported.
        :param count_table:
        :param cmp_info: [(ctrl_group_name, test_group_name), ...}
        :param sep: separator of count_table
        :param method: "LRT", "CTR", "FET", "MARS", "MATR", "FC"
        :param stat_value: pvalue or qvalue cutoff
        :param fold_change: fold change cutoff
        :param thresholdKind: 1 or 5 are supported currently. Though possible kinds are:
            • ‘1’: pValue threshold,
            • ‘2’: zScore threshold,
            • ‘3’: qValue threshold (Benjamini et al. 1995),
            • ‘4’: qValue threshold (Storey et al. 2003),
            • ‘5’: qValue threshold (Storey et al. 2003) and
              Fold-Change threshold on MA-plot are both required (can
              be used only when ‘method="MARS"’).
        :return: Results will be in current directory
        """
        with open(count_table) as f:
            header = f.readline().strip('\n').split(sep)
        # load library
        f = open('DEGseq.script.r', 'w')
        f.write('library(DEGseq)\n')
        print(cmp_info)
        for ctrl, test in cmp_info:
            ctrl_ind = header.index(ctrl)+1
            test_ind = header.index(test)+1
            f.write('\n')
            f.write("## Calculation for {} vs {} \n".format(ctrl, test))
            f.write("ctrl <- readGeneExp(file='{}', geneCol=1, valCol=c({}))\n".format(count_table,
                                                                                       ctrl_ind))
            f.write('test <- readGeneExp(file="{}", geneCol=1, valCol=c({}))\n'.format(count_table,
                                                                                       test_ind))
            # f.write('layout(matrix(c(1,2,3,4,5,6), 3, 2, byrow=TRUE))\n')
            # f.write('par(mar=c(2, 2, 2, 2))\n')
            if int(thresholdKind) == 1:
                stat_type = "pValue"
            elif int(thresholdKind) == 5:
                stat_type = "qValue"
            else:
                stat_type = "qValue"
                print("! thresholdKind should be 1 or 5. Default value will be used.")
            f.write(
                'DEGexp(geneExpMatrix1=test, geneCol1=1, expCol1=c(2), groupLabel1="{}", '
                'geneExpMatrix2=ctrl, geneCol2=1, expCol2=c(2), groupLabel2="{}", '
                'method="{}", '
                '{}={}, '
                'thresholdKind={}, '
                'foldChange={}, '
                'outputDir="{}")'
                '\n'.format(test, ctrl, method, stat_type, stat_value,
                            int(thresholdKind), fold_change, self.result_dir)
            )
        f.close()
        subprocess.check_call("R < DEGseq.script.r --no-save", shell=True)

        # try:
        #     subprocess.check_output(self.config.SOFTWARE_DIR + '/program/R-3.3.1/bin/R --restore --no-save < %s/merge_cmd.r' % (self.work_dir), shell=True)
        #     self.logger.info('生成成功')
        # except subprocess.CalledProcessError:
        #     self.logger.info('生成失败')
        #     self.set_error('R运行生成error')
        #     raise Exception("运行R脚本失败")

    def run_deg(self):
        if self.option('stat_judge') == 'padjust':
            stat_value = self.option('fdr_cutoff')
            threshold_kind = 5
        else:
            stat_value = self.option('pvalue_cutoff')
            threshold_kind = 1

        self.DEGseq(self.option('count').prop['path'],
                    self.option('cmp_info').prop['vs_list'],
                    stat_value=stat_value,
                    fold_change=self.option('fc_cutoff'),
                    thresholdKind=threshold_kind)

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        self.option('diffexp_result', self.result_dir)

    def run(self):
        super(DiffExpNewTool, self).run()
        self.run_deg()
        self.set_output()
        self.end()
