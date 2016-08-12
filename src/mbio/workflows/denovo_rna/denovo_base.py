# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""无参转录组基础分析"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import shutil


class DenovoBaseWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        """
        self._sheet = wsheet_object
        super(DenovoBaseWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  #对照组文件，格式同分组文件
            {"name": "search_pfam", "type": "bool", "default": False},  # orf 是否比对Pfam数据库
            {"name": "primer", "type": "bool", "default": True},  # 是否设计SSR引物

            {"name": "min_contig_length", "type": "int", "default": 200},  # trinity报告出的最短的contig长度。默认为200
            {"name": "SS_lib_type", "type": "string", "default": 'none'},  # reads的方向，成对的reads: RF or FR; 不成对的reads: F or R，默认情况下，不设置此参数
            {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts",  "type": "int", "default": 2},  # 离散值估计检验的最小计数值
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "diff_rate", "type": "float", "default": 0.01}  # 期望的差异基因比率

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filecheck = self.add_tool("denovo_rna.filecheck.file_denovo")
        self.qc = self.add_module("denovo_rna.qc.quality_control")
        self.qc_stat_before = self.add_module("denovo_rna.qc.qc_stat")
        self.qc_stat_after = self.add_module("denovo_rna.qc.qc_stat")
        self.assemble = self.add_tool("denovo_rna.assemble.assemble")
        self.bwa = self.add_module("denovo_rna.mapping.bwa_samtools")
        self.orf = self.add_tool("denovo_rna.gene_structure.orf")
        self.ssr = self.add_tool("denovo_rna.gene_structure.ssr")
        self.snp = self.add_module("denovo_rna.gene_structure.snp")
        self.map_qc = self.add_module("denovo_rna.mapping.map_assessment")
        self.exp_stat = self.add_module("denovo_rna.express.exp_analysis")
        self.exp_diff = self.add_module("denovo_rna.express.diff_analysis")
        self.orf_len = self.add_tool("meta.qc.reads_len_info")
        self.step.add_steps("qcstat", "assemble", "annotation", "express", "gene_structure", "map_stat")
        self.logger.info('{}'.format(self.events))
        self.logger.info('{}'.format(self.children))

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('fastq_dir').is_set:
            raise OptionError('必须设置输入fastq文件夹')
        if not self.option('control_file').is_set:
            raise OptionError('必须设置输入对照方案文件')
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def run_filecheck(self):
        opts = {
                'fastq_dir': self.option('fastq_dir'),
                'fq_type': self.option('fq_type'),
                'control_file': self.option('control_file')
                }
        if self.option('group_table').is_set:
            opts.update({'group_table': self.option('group_table')})
        print opts
        self.filecheck.set_options(opts)
        self.filecheck.run()

    def run_qc(self):
        self.qc.set_options({
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type')
        })
        self.qc.on('end', self.set_output, 'qc')
        self.qc.on('start', self.set_step, {'start': self.step.qcstat})
        self.qc.on('end', self.set_step, {'end': self.step.qcstat})
        self.qc.run()

    def run_qc_stat(self, event):
        if event['data']:
            self.qc_stat_after.set_options({
                'fastq_dir': self.qc.option('sickle_dir'),
                'fq_type': self.option('fq_type'),
                'dup': True
            })
        else:
            self.qc_stat_before.set_options({
                'fastq_dir': self.option('fastq_dir'),
                'fq_type': self.option('fq_type')})
        if event['data']:
            self.qc_stat_after.on('end', self.set_output, 'qc_stat_after')
            self.qc_stat_after.run()
        else:
            self.qc_stat_before.on('end', self.set_output, 'qc_stat_before')
            self.qc_stat_before.run()

    def run_assemble(self):
        opts = {
            'fq_type': self.option('fq_type'),
            'min_contig_length': self.option('min_contig_length'),
            'SS_lib_type': self.option('SS_lib_type'),
        }
        if self.option('fq_type') == 'SE':
            opts.update({'fq_s': self.qc.option('fq_s')})
        else:
            opts.update({
                        'fq_l': self.qc.option('fq_l'),
                        'fq_r': self.qc.option('fq_r')
                        })
        self.assemble.set_options(opts)
        self.assemble.on('end', self.set_output, 'assemble')
        self.assemble.on('start', self.set_step, {'start': self.step.assemble})
        self.assemble.on('end', self.set_step, {'end': self.step.assemble})
        self.assemble.run()

    def run_map_orf(self):
        bwa_opts = {
            'fastq_dir': self.qc.option('sickle_dir'),
            'fq_type': self.option('fq_type'),
            'ref_fasta': self.assemble.option('trinity_fa')
        }
        orf_opts = {
            'fasta': self.assemble.option('trinity_fa'),
            'search_pfam': self.option('search_pfam')
        }
        self.bwa.set_options(bwa_opts)
        self.orf.set_options(orf_opts)
        self.bwa.on('end', self.set_output, 'mapping')
        self.bwa.on('start', self.set_step, {'start': self.step.map_stat})
        self.orf.on('end', self.set_output, 'orf')
        self.orf.on('start', self.set_step, {'start': self.step.gene_structure})
        self.bwa.run()
        self.orf.run()

    def run_orf_len(self):
        orf_fasta = self.orf.work_dir + '/ORF_fasta'
        self.orf_len.set_options({'fasta_path': orf_fasta})
        self.orf_len.on('end', self.set_output, 'orf_len')
        self.orf_len.run()

    def run_ssr_snp(self):
        ssr_opts = {
            'fasta': self.assemble.option('gene_fa'),
            'bed': self.orf.option('bed'),
            'primer': self.option('primer')
        }
        snp_opts = {
            'bed': self.orf.option('bed'),
            'bam': self.bwa.option('out_bam'),
            'ref_fasta': self.assemble.option('gene_fa')
        }
        self.ssr.set_options(ssr_opts)
        self.snp.set_options(snp_opts)
        self.ssr.on('end', self.set_output, 'ssr')
        self.snp.on('end', self.set_output, 'snp')
        self.on_rely([self.ssr, self.snp], self.set_step, {'end': self.step.gene_structure})
        self.ssr.run()
        self.snp.run()

    def run_map_qc(self):
        map_qc_opts = {
            'bed': self.orf.option('bed'),
            'bam': self.bwa.option('out_bam')
        }
        self.map_qc.set_options(map_qc_opts)
        self.map_qc.on('end', self.set_output, 'map_qc')
        self.map_qc.on('end', self.set_step, {'end': self.step.map_stat})
        self.map_qc.run()

    def run_exp_stat(self):
        exp_stat_opts = {
            'fq_type': self.option('fq_type'),
            'rsem_fa': self.assemble.option('trinity_fa'),
            'dispersion': self.option('dispersion'),
            'min_rowsum_counts': self.option('min_rowsum_counts'),
            'control_file': self.option('control_file'),
            'diff_ci': self.option('diff_ci'),
            'diff_rate': self.option('diff_rate')
        }
        if self.option('fq_type') == 'SE':
            exp_stat_opts.update({'fq_s': self.qc.option('sickle_dir')})
        else:
            exp_stat_opts.update({'fq_r': self.qc.option('sickle_r_dir'), 'fq_l': self.qc.option('sickle_l_dir')})
        if self.option('group_table').is_set:
            exp_stat_opts.update({
                'group_table': self.option('group_table'),
                'gname': self.option('group_table').prop['group_scheme'][0]
            })
        self.exp_stat.set_options(exp_stat_opts)
        self.exp_stat.on('end', self.set_output, 'exp_stat')
        self.exp_stat.on('start', self.set_step, {'start': self.step.express})
        self.exp_stat.run()

    def run_exp_diff(self):
        exp_diff_opts = {
            'diff_fpkm': self.exp_stat.option('diff_fpkm'),
            'gene_file': self.exp_stat.option('gene_file')
        }
        if self.option('group_table').is_set:
            exp_diff_opts.update({'group_table': self.option('group_table')})
        self.exp_diff.set_options(exp_diff_opts)
        self.exp_diff.on('end', self.set_output, 'exp_diff')
        # move when enrich
        self.exp_diff.on('end', self.set_step, {'end': self.step.express})
        self.exp_diff.run()

    def move2outputdir(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            if mode == 'link':
                shutil.copytree(olddir, newdir, symlinks=True)
            elif mode == 'copy':
                shutil.copytree(olddir, newdir)
            else:
                raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')
        else:
            allfiles = os.listdir(olddir)
            oldfiles = [os.path.join(olddir, i) for i in allfiles]
            newfiles = [os.path.join(newdir, i) for i in allfiles]
            self.logger.info(newfiles)
            for newfile in newfiles:
                if os.path.isfile(newfile) and os.path.exists(newfile):
                    os.remove(newfile)
                elif os.path.isdir(newfile) and os.path.exists(newfile):
                    shutil.rmtree(newfile)
            for i in range(len(allfiles)):
                if os.path.isfile(oldfiles[i]):
                    os.system('cp {} {}'.format(oldfiles[i], newfiles[i]))
                else:
                    os.system('cp -r {} {}'.format(oldfiles[i], newdir))

    def set_output(self, event):
        obj = event["bind_object"]
        # 设置qc报告文件
        if event['data'] == 'qc':
            self.move2outputdir(obj.output_dir, 'QC_stat')
        if event['data'] == 'qc_stat_before':
            self.move2outputdir(obj.output_dir, 'QC_stat/before_qc')
        if event['data'] == 'qc_stat_after':
            self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
        if event['data'] == 'assemble':
            self.move2outputdir(obj.output_dir, 'Assemble')
        if event['data'] == 'mapping':
            self.move2outputdir(obj.output_dir, 'Map_stat')
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, 'Map_stat')
        if event['data'] == 'orf':
            self.move2outputdir(obj.output_dir, 'Gene_structure/orf')
        if event['data'] == 'orf_len':
            self.move2outputdir(obj.output_dir, 'Gene_structure/orf')
        if event['data'] == 'ssr':
            self.move2outputdir(obj.output_dir, 'Gene_structure/ssr')
        if event['data'] == 'snp':
            self.move2outputdir(obj.output_dir, 'Gene_structure/snp')
        if event['data'] == 'exp_stat':
            self.move2outputdir(obj.output_dir, 'Express')
        if event['data'] == 'exp_diff':
            self.move2outputdir(obj.output_dir, 'Express')

    def run(self):
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_qc_stat, False)
        self.qc.on('end', self.run_qc_stat, True)
        self.qc.on('end', self.run_assemble)
        self.assemble.on('end', self.run_map_orf)
        self.on_rely([self.orf, self.bwa], self.run_ssr_snp)
        self.on_rely([self.orf, self.bwa], self.run_map_qc)
        self.orf.on('end', self.orf_len)
        self.assemble.on('end', self.run_exp_stat)
        self.exp_stat.on('end', self.run_exp_diff)
        self.on_rely([self.map_qc, self.exp_diff, self.ssr, self.snp, self.orf_len], self.end)
        self.run_filecheck()
        super(DenovoBaseWorkflow, self).run()

    def end(self):
        self.send_files()
        super(DenovoBaseWorkflow, self).end()

    def send_files(self):
        repaths = [
            ['.', "文件夹", "denovo rna 结果文件目录"],
            ["QC_stat", "文件夹", "样本数据统计文件目录"],
            ['QC_stat/before_qc', "文件夹", "质控前的样本数据统计文件目录"],
            ["QC_stat/before_qc/qualityStat/", "文件夹", "每个样本的fastq的质量统计文件的输出目录"],
            ["QC_stat/before_qc/fastq_stat.xls", "xls", "所有样本的fastq信息统计表"],
            ['QC_stat/after_qc', "文件夹", "质控后的样本数据统计文件目录"],
            ["QC_stat/after_qc/qualityStat/", "文件夹", "质控后的每个样本的fastq的质量统计文件的输出目录"],
            ["QC_stat/after_qc/fastq_stat.xls", "xls", "质控后的所有样本的fastq信息统计表"],
            ["QC_stat/after_qc/dup.xls", "xls", "所有样本的fastq序列重复统计表"],
            ["QC_stat/sickle_dir/", "文件夹", "质量剪切后的fastq文件输出目录"],
            ['Assemble', "文件夹", "Trinity拼接组装统计结果目录"],
            ['Assemble/transcript.iso.txt', "txt", "Trinity.fasta可变剪接体统计文件"],
            ['Assemble/trinity.fasta.stat.xls', "xls", "Trinity.fasta序列相关信息统计文件"],
            ['Gene_structure', "文件夹", "基因结构分析结果目录"],
            ['Gene_structure/snp', "文件夹", "snp分析结果目录"],
            ['Gene_structure/orf', "文件夹", "orf分析结果目录"],
            ["Gene_structure/orf/reads_len_info", "文件夹", "orf序列长度分布信息文件夹"],
            ['Gene_structure/ssr', "文件夹", "orf分析结果目录"],
            ["Gene_structure/ssr/misa_stat.xls", "xls", "ssr类型统计表"],
            ['Map_stat', "文件夹", "Mapping后质量统计结果目录"],
            ["Map_stat/coverage/", "文件夹", "基因覆盖度分析输出目录"],
            ["Map_stat/sorted_bam/", "文件夹", "每个样本排序后的bam文件输出目录"],
            ["Map_stat/dup/", "文件夹", "冗余序列分析输出目录"],
            ["Map_stat/satur/", "文件夹", "测序饱和度分析输出目录"],
            ["Map_stat/bam_stat.xls", "xls", "bam格式比对结果统计表"],
            ['Express/', "文件夹", "表达量分析结果目录"],
            ['Express/diff_exp', "文件夹", "表达量差异检测分析结果目录"],
            ["Express/diff_exp/diff_fpkm", "xls", "差异基因表达量表"],
            ["Express/diff_exp/diff_count", "xls", "差异基因计数表"],
            ['Express/rsem', "文件夹", "表达量计算分析结果目录"],
            ['Express/network', "文件夹", "差异基因网络共表达分析结果目录"],
            ["Express/network/all_edges.txt", "txt", "edges结果信息"],
            ["Express/network/all_nodes.txt ", "txt", "nodes结果信息"],
            ["Express/network/removeGene.xls ", "xls", "移除的基因信息"],
            ["Express/network/removeSample.xls ", "xls", "移除的样本信息"],
            ["Express/network/softPower.pdf", "pdf", "softpower相关信息"],
            ["Express/network/ModuleTree.pdf", "pdf", "ModuleTree图"],
            ["Express/network/eigengeneClustering.pdf", "pdf", "eigengeneClustering图"],
            ["Express/network/eigenGeneHeatmap.pdf", "pdf", "eigenGeneHeatmap图"],
            ["Express/network/networkHeatmap.pdf", "pdf", "networkHeatmap图"],
            ["Express/network/sampleClustering.pdf", "pdf", "sampleClustering图"],
            ['Express/correlation', "文件夹", "表达量样本相关性分析结果目录"],
            ["Express/correlation/correlation_matrix.xls", "xls", "相关系数矩阵表"],
            ["Express/correlation/hcluster_tree_correlation_matrix.xls_average.tre", "xls", "相关系数树文件"],
            ["Express/cluster", "文件夹", "差异基因聚类分析分析结果目录"],
            ["Express/cluster/hclust/", "", "层级聚类分析结果目录"],
            ["Express/cluster/hclust/hc_gene_order", "txt", "按基因聚类的基因排序列表"],
            ["Express/cluster/hclust/hc_sample_order", "txt", "按样本聚类的样本排序列表"],
            ["Express/cluster/hclust/hclust_heatmap.xls", "xls", "层级聚类热图数据"]
        ]
        regexps = [
            [r'Assemble/.*_length\.distribut\.txt$', 'txt', '长度分布信息统计文件'],
            [r"Gene_structure/snp/.*snp_position_stat\.xls", "xls", "样本snp编码位置信息统计表"],
            [r"Gene_structure/snp/.*snp_type_stat\.xls", "xls", "样本snp类型统计表"],
            [r"Gene_structure/snp/.*snp\.xls", "xls", "样本snp信息表"],
            [r"Gene_structure/ssr/.*misa$", "misa", "ssr结果"],
            [r"Gene_structure/orf/reads_len_info/.*reads_len_info\.txt$", "xls", "orf序列长度分布信息文件"],
            [r"Gene_structure/orf/transdecoder.pep$", "fasta", "蛋白质序列文件"],
            [r"Gene_structure/orf/transdecoder.cds$", "fasta", "cds序列文件"],
            [r"Gene_structure/orf/transdecoder.bed$", "bed", "orf位置信息bed格式文件"],
            [r"Map_stat/dup/.*pos\.DupRate\.xls", "xls", "比对到基因组的序列的冗余统计表"],
            [r"Map_stat/dup/.*seq\.DupRate\.xls", "xls", "所有序列的冗余统计表"],
            [r"Map_stat/satur/.*eRPKM\.xls", "xls", "RPKM表"],
            [r"Map_stat/coverage/.*cluster_percent\.xls", "xls", "饱和度作图数据"],
            [r"Express/cluster/hclust/subcluster_", "xls", "子聚类热图数据"],
            [r"Express/diff_exp/.*_edgr_stat\.xls$", "xls", "edger统计结果文件"],
            [r"Express/rsem/.*results$", "xls", "单样本rsem分析结果表"],
            [r"Express/rsem/.*matrix$", "xls", "表达量矩阵"],
            [r"Express/network/CytoscapeInput.*", "txt", "Cytoscape作图数据"]
        ]
        if self.option("search_pfam") is True:
            repaths += [["Gene_structure/orf/pfam_domain", "", "Pfam比对蛋白域结果信息"]]
        if self.option('fq_type') == 'SE':
            repaths += [
                ['QC_stat/clip_dir', "文件夹", "SE去接头后的fastq文件输出目录"]
            ]
        else:
            repaths += [
                ["QC_stat/seqprep_dir/", "文件夹", "PE去接头后fastq文件输出目录"]
            ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        for i in self.get_upload_files():
            self.logger.info('upload file:{}'.format(str(i)))
