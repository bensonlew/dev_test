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
            {"name": "fq_type", "type": "string"}, # PE OR SE
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  #对照组文件，格式同分组文件
            {"name": "search_pfam", "type": "bool", "default": True},  # orf 是否比对Pfam数据库
            {"name": "primer", "type": "bool", "default": True},  # 是否设计SSR引物

            {"name": "min_contig_length", "type": "int", "default": 200},  # trinity报告出的最短的contig长度。默认为200
            {"name": "SS_lib_type", "type": "string", "default": 'none'},  # reads的方向，成对的reads: RF or FR; 不成对的reads: F or R，默认情况下，不设置此参数
            {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts",  "type": "int", "default": 20},  # 离散值估计检验的最小计数值
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "diff_rate", "type": "float", "default": 0.01}  #期望的差异基因比率

            # {"name": "sickle_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切输出结果文件夹(包括左右段)
            # {"name": "sickle_r_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切右端输出结果文件夹
            # {"name": "sickle_l_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切左端输出结果文件夹
            # {"name": "fq_s", "type": "outfile", "format": "sequence.fastq"},  # SE所有样本集合
            # {"name": "fq_r", "type": "outfile", "format": "sequence.fastq"},  # PE所有右端序列样本集合
            # {"name": "fq_l", "type": "outfile", "format": "sequence.fastq"},  # PE所有左端序列样本集合
            # {"name": "gene_fa", "type": "outfile", "format": "sequence.fasta"},  #组装后的基因的fasta序列文件
            # {"name": "trinity_fa", "type": "outfile", "format": "sequence.fasta"},  #trinity拼接出来的fasta文件

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filecheck = self.add_tool("denovo_rna.filecheck.file_denovo")
        self.qc = self.add_module("denovo_rna.qc.quality_control")
        self.qc_stat = self.add_module("denovo_rna.qc.qc_stat")
        self.assemble = self.add_tool("denovo_rna.assemble.assemble")
        self.bwa = self.add_module("denovo_rna.maping.bwa_samtools")
        self.orf = self.add_tool("denovo_rna.gene_structure.orf")
        self.ssr = self.add_tool("denovo_rna.gene_structure.ssr")
        self.snp = self.add_module("denovo_rna.gene_structure.snp")
        self.map_qc = self.add_module("denovo_rna.maping.map_assessment")
        self.exp_stat = add_module("denovo_rna.express.exp_analysis")
        self.exp_diff = add_module("denovo_rna.express.diff_analysis")
        self.step.add_steps("qcstat", "assemble", "annotation", "express", "gene_structure","map_stat")

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

    def run_qc_stat(self, qc):
        if qc['data']:
            self.qc_stat.set_options({
                'fastq_dir': self.qc.option('sickle_dir'),
                'fq_type': self.option('fq_type'),
                'dup': True
            })
        else:
            self.qc_stat.set_options({
                'fastq_dir': self.option('fastq_dir'),
                'fq_type': self.option('fq_type')
        self.qc_stat.on('end', self.set_output, 'qc_stat')
        self.qc_stat.run()

    def run_assemble(self):
        opts = {
            'fq_type': self.option('fq_type'),
            'min_contig_length': self.option('min_contig_length'),
            'SS_lib_type': self.option('SS_lib_type'),
        }
        if self.option('fq_type') == 'SE':
            opts.update({'fq_s': self.option('fq_s')})
        else:
            opts.update({
                        'fq_l': self.option('fq_l')，
                        'fq_r': self.option('fq_r')
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
            'ref_fasta': self.assemble.option('gene_fa')
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

    def run_ssr_snp(self):
        ssr_opts = {
            'fasta': self.assemble.option('gene_fa'),
            'bed': self.orf.option('bed'),
            'primer', self.option('primer')
        }
        snp_opts = {
            'bed': self.orf.option('bed'),
            'bam': self.bwa.option('out_bam'),
            'ref_fasta': self.assemble.option('gene_fa')
        }
        self.ssr.set_options(bwa_opts)
        self.snp.set_options(orf_opts)
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
            'gene_table': self.exp_stat.option('gene_table')
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
            self.move2outputdir(obj.output_dir, self.output_dir + 'QC_stat')
        if event['data'] == 'qc_stat':
            self.move2outputdir(obj.output_dir, self.output_dir + 'QC_stat')
        if event['data'] == 'assemble':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Assemble')
        if event['data'] == 'mapping':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Map_stat')
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Map_stat')
        if event['data'] == 'orf':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Gene_structure')
        if event['data'] == 'ssr':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Gene_structure')
        if event['data'] == 'snp':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Gene_structure')
        if event['data'] == 'exp_stat':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Express')
        if event['data'] == 'exp_diff':
            self.move2outputdir(obj.output_dir, self.output_dir + 'Express')

    def run(self):
        self.filecheck.on('end', self.run_qc)
        self.file_check.on('end', self.run_qc_stat, False)
        self.qc.on('end', self.run_qc_stat, True)
        self.qc.on('end', self.run_assemble)
        self.assemble.on('end', self.run_map_orf)
        self.on_rely([self.orf, self.bwa], self.run_ssr_snp)
        self.on_rely([self.orf, self.bwa], self.run_map_qc)
        self.assemble.on('end', self.run_exp_stat)
        self.exp_stat.on('end', self.run_exp_diff)
        self.on_rely([self.map_qc, self.run_exp_diff, self.ssr, self.snp], self.end)
        super(DenovoBaseWorkflow, self).run()


    def send_files(self):
        repaths = [
            [".", "", "多样性结果文件目录"],
            ["QC_stat", "", "样本数据统计文件目录"],
            ["QC_stat/samples_info/samples_info.txt", "txt", "样本信息统计文件"],
            ["QC_stat/base_info", "", "单个样本碱基质量统计目录"],
            ["QC_stat/reads_len_info", "", "序列长度分布统计文件目录"],
            ["Otu", "", "OTU聚类结果文件目录"],
            ["Tax_assign", "", "OTU对应物种分类文件目录"],
            ["Tax_assign/seqs_tax_assignments.txt", "taxon.seq_taxon", "OTU序列物种分类文件"],
            ["OtuTaxon_summary", "", "OTU物种分类综合统计目录"],
            ["OtuTaxon_summary/otu_taxon.biom", "meta.otu.biom", "OTU的biom格式文件"],
            ["OtuTaxon_summary/otu_taxon.xls", "meta.otu.otu_table", "OTU物种分类统计表"],
            ["OtuTaxon_summary/tax_summary_a", "meta.otu.tax_summary_dir", "不同级别的otu表和biom表的目录"],
            ["Alpha_diversity", "", "Alpha diversity文件目录"],
            ["Alpha_diversity/estimators.xls", "xls", "Alpha多样性指数表"],
            ["Beta_diversity", "", "Beta diversity文件目录"],
            ["Beta_diversity/Anosim", "", "anosim&adonis结果输出目录"],
            ["Beta_diversity/Anosim/anosim_results.txt", "txt", "anosim分析结果"],
            ["Beta_diversity/Anosim/adonis_results.txt", "txt", "adonis分析结果"],
            ["Beta_diversity/Anosim/format_results.xls", "xls", "anosim&adonis综合统计表"],
            ["Beta_diversity/Dbrda", "", "db_rda分析结果目录"],
            ["Beta_diversity/Dbrda/db_rda_sites.xls", "xls", "db_rda样本坐标表"],
            ["Beta_diversity/Dbrda/db_rda_species.xls", "xls", "db_rda物种坐标表"],
            ["Beta_diversity/Dbrda/db_rda_centroids.xls", "xls", "db_rda哑变量环境因子坐标表"],
            ["Beta_diversity/Dbrda/db_rda_biplot.xls", "xls", "db_rda数量型环境因子坐标表"],
            ["Beta_diversity/Box", "", "距离统计和统计检验分析结果目录"],
            ["Beta_diversity/Box/Stats.xls", "xls", "分组统计检验结果"],
            ["Beta_diversity/Box/Distances.xls", "xls", "组内组间距离值统计结果"],
            ["Beta_diversity/Distance", "", "距离矩阵计算结果输出目录"],
            ["Beta_diversity/Hcluster", "", "层次聚类结果目录"],
            ["Beta_diversity/Hcluster/hcluster.tre", "graph.newick_tree", "层次聚类树"],
            ["Beta_diversity/Nmds", "", "NMDS分析结果输出目录"],
            ["Beta_diversity/Nmds/nmds_sites.xls", "xls", "样本各维度坐标"],
            ["Beta_diversity/Pca", "", "PCA分析结果输出目录"],
            ["Beta_diversity/Pca/pca_importance.xls", "xls", "主成分解释度表"],
            ["Beta_diversity/Pca/pca_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Beta_diversity/Pca/pca_sites.xls", "xls", "样本各成分轴坐标"],
            ["Beta_diversity/Pca/pca_envfit_factor_scores.xls", "xls", "哑变量环境因子表"],
            ["Beta_diversity/Pca/pca_envfit_factor.xls", "xls", "哑变量环境因子坐标表"],
            ["Beta_diversity/Pca/pca_envfit_vector_scores.xls", "xls", "数量型环境因子表"],
            ["Beta_diversity/Pca/pca_envfit_vector.xls", "xls", "数量型环境因子坐标表"],
            ["Beta_diversity/Pcoa", "", "pcoa分析结果目录"],
            ["Beta_diversity/Pcoa/pcoa_eigenvalues.xls", "xls", "矩阵特征值"],
            ["Beta_diversity/Pcoa/pcoa_sites.xls", "xls", "样本坐标表"],
            ['Beta_diversity/Rda/dca.xls', 'xls', 'DCA分析结果'],
            ["Beta_diversity/Plsda", "", "plsda分析结果目录"],
            ["Beta_diversity/Plsda/plsda_sites.xls", "xls", "样本坐标表"],
            ["Beta_diversity/Plsda/plsda_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Beta_diversity/Plsda/plsda_importance.xls", "xls", "主成分解释度表"],
            ["Beta_diversity/Rda", "", "rda_cca分析结果目录"]
        ]
        regexps = [
            [r"QC_stat/base_info/.*\.fastq\.fastxstat\.txt", "", "单个样本碱基质量统计文件"],
            [r"QC_stat/reads_len_info/step_\d+\.reads_len_info\.txt", "", "序列长度分布统计文件"],
            [r'Beta_diversity/Distance/%s.*\.xls$' % self.option('dis_method'), 'meta.beta_diversity.distance_matrix', '样本距离矩阵文件'],
            [r'Beta_diversity/Rda/.+_importance\.xls$', 'xls', '主成分变化解释度表'],
            [r'Beta_diversity/Rda/.+_sites\.xls$', 'xls', '样本坐标表'],
            [r'Beta_diversity/Rda/.+_species\.xls$', 'xls', '物种坐标表'],
            [r'Beta_diversity/Rda/.+_biplot\.xls$', 'xls', '数量型环境因子坐标表'],
            [r'Beta_diversity/Rda/.+_centroids\.xls$', 'xls', '哑变量环境因子坐标表'],
            ["Otu/otu_reps.fasta", "sequence.fasta", "OTU代表序列"],
            ["Otu/otu_seqids.txt", "txt", "OTU代表序列名称列表"],
            ["Otu/otu_table.biom", 'meta.otu.biom', "OTU表对应的Biom文件"],
            ["Otu/otu_table.xls", "meta.otu.otu_table", "OTU统计表"],
            ["Otu/otu_phylo.tre", "graph.newick_tree", "OTU代表序列进化树"],
            ["QC_stat/base_info/.*\.fastq\.fastxstat\.txt", "txt", "单个样本碱基质量统计文件"],
            ["QC_stat/reads_len_info/step_\d+\.reads_len_info\.txt", "txt", "序列长度分布统计文件"],
            ["OtuTaxon_summary/tax_summary_a/.+\.biom$", "meta.otu.biom", "OTU表的biom格式的文件"],
            ["OtuTaxon_summary/tax_summary_a/.+\.xls$", "meta.otu.biom", "单级物种分类统计表"],
            ["OtuTaxon_summary/tax_summary_a/.+\.full\.xls$", "meta.otu.biom", "多级物种分类统计表"]
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        for i in self.get_upload_files():
            self.logger.info('upload file:{}'.format(str(i)))

    def end(self):
        self.send_files()
        super(MetaBaseWorkflow, self).end()
