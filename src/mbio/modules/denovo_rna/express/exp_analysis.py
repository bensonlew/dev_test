# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError


class ExpAnalysisModule(Module):
    def __init__(self, work_id):
        super(ExpAnalysisModule, self).__init__(work_id)
        self.step.add_steps('rsem', 'diff_exp')
        options = [
            {"name": "rsem_bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # 输入文件，bam格式的比对文件
            {"name": "rsem_fa", "type": "infile", "format": "sequence.fasta"},  #trinit.fasta文件
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "tran_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "tran_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "gene_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},
            {"name": "exp_way", "type": "string", "default": "fpkm"},
            {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts",  "type": "int", "default": 20},  # 离散值估计检验的最小计数值
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  #对照组文件，格式同分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "diff_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  #差异基因计数表
            {"name": "diff_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  #差异基因表达量表
            {"name": "gname", "type": "string"},  #  分组方案名称
            {"name": "diff_rate", "type": "float", "default": 0.01}  #期望的差异基因比率
        ]
        self.add_option(options)
        self.rsem = self.add_tool("denovo_rna.express.rsem")
        self.diff_exp = self.add_tool("denovo_rna.express.diff_exp")

    def check_options(self):
        if not self.option("fq_l") and not self.option("fq_r") and not self.option("fq_s"):
            raise OptionError("必须设置PE测序输入文件或者SE测序输入文件")
        if self.option("fq_l") and self.option("fq_r") and self.option("fq_s"):
            raise OptionError("不能同时设置PE测序输入文件和SE测序输入文件的参数")
        if self.option("fq_l") and not self.option("fq_r"):
            raise OptionError("要同时设置PE测序左端fq和右端fq，缺少右端fq")
        if not self.option("fq_l") and self.option("fq_r"):
            raise OptionError("要同时设置PE测序左端fq和右端fq，缺少左端fq")
        if self.option("exp_way") not in ['fpkm', 'tpm']:
            raise OptionError("所设表达量的代表指标不在范围内，请检查")
        if not self.option('control_file').is_set:
            raise OptionError("必须设置输入文件：上下调对照组参考文件")
        if self.option("diff_ci") >= 1 or self.option("diff_ci") <= 0:
            raise OptionError("显著性水平不在(0,1)范围内")
        if self.option("diff_rate") > 1 or self.option("diff_rate") <= 0:
            raise OptionError("期望的差异基因比率不在(0，1]范围内")
        return True

    def rsem_run(self):
        tool_opt = {}
        tool_opt['rsem_bam'] = self.option('rsem_bam')
        tool_opt['rsem_fa'] = self.option('rsem_fa')
        tool_opt['exp_way'] = self.option('exp_way')
        if self.option('fq_s'):
            tool_opt['fq_s'] = self.option('fq_s')
        else:
            tool_opt['fq_l'] = self.option('fq_l')
            tool_opt['fq_r'] = self.option('fq_r')
        self.rsem.set_options(tool_opt)
        self.rsem.on('end', self.set_output, 'rsem')
        self.rsem.on('end', self.diff_exp_run)
        self.step.rsem.start()
        self.rsem.run()
        self.step.rsem.finish()
        self.step.update()

    def diff_exp_run(self):
        tool_opt = {}
        tool_opt['count'] = self.option('gene_count')
        tool_opt['fpkm'] = self.option('gene_fpkm')
        tool_opt['dispersion'] = self.option('dispersion')
        tool_opt['min_rowsum_counts'] = self.option('min_rowsum_counts')
        tool_opt['control_file'] = self.option('control_file')
        tool_opt['diff_ci'] = self.option('diff_ci')
        tool_opt['diff_rate'] = self.option('diff_rate')
        if self.option('group').is_set:
            tool_opt['edger_group'] = self.option('group')
            tool_opt['gname'] = self.option('gname')
        self.diff_exp.set_options(tool_opt)
        self.diff_exp.on('end', self.set_output, 'diff_exp')
        self.diff_exp.on('end', self.end)
        self.step.diff_exp.start()
        self.diff_exp.run()
        self.step.diff_exp.finish()
        self.step.update()

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'rsem':
            self.linkdir(obj.output_dir, 'rsem')
            self.option('gene_count', obj.option('gene_count'))
            self.option('gene_fpkm', obj.option('gene_fpkm'))
            self.option('tran_count', obj.option('tran_count'))
            self.option('tran_fpkm', obj.option('tran_fpkm'))
        elif event['data'] == 'diff_exp':
            self.linkdir(obj.output_dir, 'diff_exp')
            self.option('diff_count', obj.option('diff_count'))
            self.option('diff_fpkm', obj.option('diff_fpkm'))
        else:
            pass

    def run(self):
        super(ExpAnalysisModule, self).run()
        self.rsem_run()

    def end(self):
        repaths = [
            [".", "", "表达量分析模块结果输出目录"],
            ["./rsem", "", "rsem分析结果输出目录"],
            ["./diff_exp", "", "edger分析结果输出目录"],
            [r"diff_exp/diff_fpkm", "xls", "差异基因表达量表"],
            [r"diff_exp/diff_count", "xls", "差异基因计数表"]

        ]
        regexps = [
            [r"rsem/results$", "xls", "rsem结果"],
            [r"rsem/matrix$", "xls", "表达量矩阵"],
            [r"diff_exp/.*_edgr_stat\.xls$", "xls", "edger统计结果文件"]
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(ExpAnalysisModule, self).end()
