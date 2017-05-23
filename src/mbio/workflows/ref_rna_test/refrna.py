# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""有参转录一键化工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
import os
import json
import shutil
import re


class RefrnaWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(RefrnaWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "workflow_type", "type": "string", "default": "transcriptome"},  # 转录组
            {"name": "assemble_or_not", "type": "bool", "default": True},
            {"name": "blast_method", "type": "string", "default": "diamond"},
            {"name": "genome_structure_file", "type": "infile", "format": "gene_structure.gtf, gene_structure.gff3"},
            # 基因组结构注释文件，可上传gff3或gtf
            {"name": "strand_specific", "type": "bool", "default": False},
            # 当为PE测序时，是否有链特异性, 默认是False, 无特异性
            {"name": "strand_dir", "type": "string", "default": "None"},
            # 当链特异性时为True时，正义链为forward，反义链为reverse
            {"name": "is_duplicate", "type": "bool", "default": True},  # 是否有生物学重复
            {"name": "group_table", "type": "infile", "format": "sample.group_table"},  # 分组文件
            {"name": "control_file", "type": "infile", "format": "sample.control_table"},
            # 对照表

            {"name": "sample_base", "type": "bool", "default": False},  # 是否使用样本库
            {"name": "batch_id", "type": "string", "default": ""},  # 样本集编号

            {"name": "go_upload_file", "type": "infile", "format": "annotation.upload.anno_upload"},
            # 用户上传go文件
            {"name": "kegg_upload_file", "type": "infile", "format": "annotation.upload.anno_upload"},
            # 用户上传kegg文件

            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq_dir"},  # Fastq文件夹
            {"name": "qc_quality", "type": "int", "default": 30},  # 质量剪切中保留的最小质量值
            {"name": "qc_length", "type": "int", "default": 50},  # 质量剪切中保留的最短序列长度

            {"name": "ref_genome", "type": "string", "default": "customer_mode"},  # 参考基因组
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组

            {"name": "nr_blast_evalue", "type": "float", "default": 1e-5},  # NR比对e值
            {"name": "string_blast_evalue", "type": "float", "default": 1e-5},  # String比对使用的e值
            {"name": "kegg_blast_evalue", "type": "float", "default": 1e-5},  # KEGG注释使用的e值
            {"name": "swissprot_blast_evalue", "type": "float", "default": 1e-5},  # Swissprot比对使用的e值
            {"name": "database", "type": "string", "default": 'go,nr,cog,kegg,swissprot,pfam'},
            # 全部六个注释
            {"name": "nr_database", "type": "string", "default": "animal"},  # nr库类型

            {"name": "seq_method", "type": "string", "default": "Tophat"},  # 比对方法，Tophat or Hisat
            {"name": "map_assess_method", "type": "string", "default":
                "saturation,duplication,distribution,coverage,chr_stat"},
            # 比对质量评估分析
            {"name": "mate_std", "type": "int", "default": 50},  # 末端配对插入片段长度标准差
            {"name": "mid_dis", "type": "int", "default": 50},  # 两个成对引物间的距离中间值
            {"name": "result_reserved", "type": "int", "default": 1},  # 最多保留的比对结果数目

            {"name": "assemble_method", "type": "string", "default": "cufflinks"},
            # 拼接方法，Cufflinks or Stringtie or None

            {"name": "express_method", "type": "string", "default": "rsem"},
            # 表达量分析手段: Htseq, Featurecount, Kallisto, RSEM
            {"name": "exp_way", "type": "string", "default": "fpkm"}, #默认选择fpkm进行表达量的计算

            {"name": "diff_method", "type": "string", "default": "edgeR"},
            # 差异表达分析方法
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            # {"name": "sort_type", "type": "string", "default": "pos"},  # 排序方法
            {"name": "exp_analysis", "type": "string", "default": "cluster,kegg_rich,cog_class,kegg_regulate,go_rich,go_regulate"},
            # 差异表达富集方法,聚类分析, GO富集分析, KEGG富集分析, cog统计分析

            {"name": "human_or_not", "type": "bool", "default": True},  # 是否为人类基因组
            {"name": "gene_structure_analysis", "type": "string", "default": "alter-splicing,SNP"},
            # 基因结构分析，分为alter-splicing, SNP, RNA-editing, gene-fusion
            {"name": "alter_splicing_method", "type": "string", "default": "rMATS"},
            # 可变剪切分析软件: rMATS, ASprofile, MapSplice, SpliceGrapher, CLASS2

            {"name": "protein_analysis", "type": "string", "default": "network"},
            {"name": "combine_score", "type": "int", "default": 300},
            # 蛋白质分析

            {"name": "p_length", "type": "int", "default": 100},  # pfam参数
            {"name": "markov_length", "type": "int", "default": 3000},  # markov_length

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.json_path = self.config.SOFTWARE_DIR + "/database/refGenome/scripts/ref_genome.json"
        self.json_dict = self.get_json()
        self.filecheck = self.add_tool("rna.filecheck_ref")
        self.gs = self.add_tool("gene_structure.genome_structure")
        self.qc = self.add_module("sequence.hiseq_qc")
        self.qc_stat_before = self.add_module("sequence.hiseq_reads_stat")
        self.qc_stat_after = self.add_module("sequence.hiseq_reads_stat")
        self.mapping = self.add_module("rna.rnaseq_mapping")
        self.altersplicing = self.add_module("gene_structure.rmats")
        self.map_qc = self.add_module("denovo_rna.mapping.map_assessment")
        self.map_qc_gene = self.add_module("denovo_rna.mapping.map_assessment")
        self.map_gene = self.add_module("rna.rnaseq_mapping")
        self.assembly = self.add_module("assemble.refrna_assemble")
        self.exp = self.add_module("rna.express")
        self.exp_alter = self.add_module("rna.express")
        self.exp_fc = self.add_module("rna.express")
        self.exp_diff_trans = self.add_module("denovo_rna.express.diff_analysis")
        self.exp_diff_gene = self.add_module("denovo_rna.express.diff_analysis")
        self.snp_rna = self.add_module("gene_structure.snp_rna")
        self.seq_abs = self.add_tool("annotation.transcript_abstract")
        self.new_gene_abs = self.add_tool("annotation.transcript_abstract")
        self.new_trans_abs = self.add_tool("annotation.transcript_abstract")
        self.para_anno = self.add_module("rna.parallel_anno")

        self.annotation = self.add_module('annotation.ref_annotation')
        self.new_annotation = self.add_module('annotation.ref_annotation')
        self.network_trans = self.add_module("protein_regulation.ppinetwork_analysis")
        self.network_gene = self.add_module("protein_regulation.ppinetwork_analysis")
        self.tf = self.add_tool("protein_regulation.TF_predict")
        self.merge_trans_annot = self.add_tool("annotation.merge_annot")
        self.merge_gene_annot = self.add_tool("annotation.merge_annot")
        self.pfam = self.add_tool("denovo_rna.gene_structure.orf")
        if self.option("ref_genome") != "customer_mode":
            self.ref_genome = self.json_dict[self.option("ref_genome")]["ref_genome"]
            self.taxon_id = self.json_dict[self.option("ref_genome")]["taxon_id"]
        else:
            self.ref_genome = self.option("ref_genome_custom")
            self.taxon_id = ""
        self.gff = ""
        if self.option("ref_genome") == "customer_mode":
            if self.option('genome_structure_file').format == "gene_structure.gff3":
                self.gff = self.option('genome_structure_file').prop["path"]
        else:
            self.gff = self.json_dict[self.option("ref_genome")]["gff"]
        self.IMPORT_REPORT_AFTER_END = False
        self.final_tools = [self.snp_rna, self.altersplicing, self.exp_diff_gene, self.exp_diff_trans]
        self.genome_status = True
        self.step.add_steps("qcstat", "mapping", "assembly", "annotation", "exp", "map_stat",
                            "seq_abs","network_analysis", "sample_analysis", "altersplicing")

    def check_options(self):
        """
        检查选项
        """
        if not self.option("fq_type") in ["PE", "SE"]:
            raise OptionError("fq序列类型应为PE或SE")
        if not self.option("qc_quality") > 0 and not self.option("qc_quality") < 42:
            raise OptionError("qc中最小质量值超出范围，应在0~42之间")
        if not self.option("qc_length") > 0:
            raise OptionError("qc中最小长度超出范围，应大于0")
        if not self.option("nr_blast_evalue") > 0 and not self.option("nr_blast_evalue") < 1:
            raise OptionError("NR比对的E值超出范围")
        if not self.option("string_blast_evalue") > 0 and not self.option("string_blast_evalue") < 1:
            raise OptionError("String比对的E值超出范围")
        if not self.option("kegg_blast_evalue") > 0 and not self.option("kegg_blast_evalue") < 1:
            raise OptionError("Kegg比对的E值超出范围")
        if not self.option("swissprot_blast_evalue") > 0 and not self.option("swissprot_blast_evalue") < 1:
            raise OptionError("Swissprot比对的E值超出范围")
        if not self.option("seq_method") in ["Tophat", "Hisat"]:
            raise OptionError("比对软件应在Tophat与Hisat中选择")
        for i in self.option('map_assess_method').split(','):
            if i not in ["saturation", "duplication", "distribution", "coverage", "chr_stat"]:
                raise OptionError("比对质量评估分析没有{}，请检查".format(i))
        if self.option("assemble_or_not"):
            if self.option("assemble_method") not in ["cufflinks", "stringtie"]:
                raise OptionError("拼接软件应在cufflinks和stringtie中选择")
        return True

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def get_json(self):
        f = open(self.json_path, "r")
        json_dict = json.loads(f.read())
        return json_dict

    def run_filecheck(self):
        opts = {
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type'),
            'control_file': self.option('control_file'),
            "ref_genome_custom": self.ref_genome
        }
        if self.gff != "":
            opts.update({
                "gff": self.gff
            })
        else:
            opts.update({
                "in_gtf": self.option('genome_structure_file').prop["path"]
            })
        if self.option('group_table').is_set:
            opts.update({'group_table': self.option('group_table')})
        self.filecheck.set_options(opts)
        self.filecheck.run()

    def run_gs(self):
        opts = {
            "in_fasta": self.ref_genome,
            # "in_gtf": self.filecheck.option("gtf")
        }
        if self.gff != "":
            opts.update({
                "in_gff": self.gff
            })
        else:
            opts.update({
                "in_gtf": self.filecheck.option("gtf")
            })
        self.gs.set_options(opts)
        self.gs.run()

    def run_qc(self):
        self.qc.set_options({
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type')
        })
        self.qc.on('end', self.set_output, 'qc')
        self.genome_status = self.filecheck.option("genome_status")
        self.qc.on('start', self.set_step, {'start': self.step.qcstat})
        self.qc.on('end', self.set_step, {'end': self.step.qcstat})
        self.qc.run()

    def run_seq_abs(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        self.seq_abs.set_options(opts)
        self.seq_abs.on('start', self.set_step, {'start': self.step.seq_abs})
        self.seq_abs.on('end', self.set_step, {'end': self.step.seq_abs})
        self.seq_abs.run()

    def run_align(self, event):
        method = event["data"]
        self.blast_modules = []
        self.gene_list = self.seq_abs.option('gene_file')
        blast_lines = int(self.seq_abs.option('query').prop['seq_number']) / 10
        self.logger.info('.......blast_lines:%s' % blast_lines)
        blast_opts = {
            'query': self.seq_abs.option('query'),
            'query_type': 'nucl',
            'database': None,
            'blast': 'blastx',
            'evalue': None,
            'outfmt': 5,
            'lines': blast_lines,
        }
        # go注释参数设置
        self.blast_nr = self.add_module('align.' + method)
        blast_opts.update(
            {
                'database': self.option("nr_database"),
                'evalue': self.option('nr_blast_evalue')
            }
        )
        self.blast_nr.set_options(blast_opts)
        self.blast_modules.append(self.blast_nr)
        self.blast_nr.on('end', self.set_output, 'nrblast')
        # cog注释参数设置
        self.blast_string = self.add_module('align.' + method)
        blast_opts.update(
            {'database': 'string', 'evalue': self.option('string_blast_evalue')}
        )
        self.blast_string.set_options(blast_opts)
        self.blast_modules.append(self.blast_string)
        self.blast_string.on('end', self.set_output, 'stringblast')
        # kegg注释参数设置
        self.blast_kegg = self.add_module('align.' + method)
        blast_opts.update(
            {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
        )
        self.blast_kegg.set_options(blast_opts)
        self.blast_modules.append(self.blast_kegg)
        self.blast_kegg.on('end', self.set_output, 'keggblast')
        # 运行run方法
        self.on_rely(self.blast_modules, self.run_para_anno, True)
        self.blast_string.run()
        self.blast_kegg.run()
        self.blast_nr.run()

    def run_para_anno(self):
        opts = {
            "string_align_dir": self.blast_string.catblast.option("blastout"),
            "nr_align_dir": self.blast_nr.catblast.option("blastout"),
            "kegg_align_dir": self.blast_kegg.catblast.option("blastout"),
            "gene_file": self.seq_abs.option("gene_file"),
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        self.para_anno.set_options(opts)
        self.para_anno.on("end", self.run_annotation)
        self.para_anno.run()

    def run_annotation(self):
        # 读入上传表格文件进行注释
        opts = {
            "gos_list_upload": self.para_anno.option("out_go"),
            "kos_list_upload": self.para_anno.option("out_kegg"),
            "blast_string_table": self.para_anno.option("out_cog"),
            "gene_file": self.seq_abs.option("gene_file"),
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        if self.option("go_upload_file").is_set:
            opts.update({
                "gos_list_upload": self.option("go_upload_file")
            })
        if self.option("kegg_upload_file").is_set:
            opts.update({
                "kos_list_upload": self.option("kegg_upload_file")
            })
        self.annotation.set_options(opts)
        self.annotation.on("end", self.set_output, "annotation")
        self.annotation.run()

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

    def run_map_assess_gene(self):
        opts = {
            "bam": self.map_gene.option("bam_output"),
            "bed": self.filecheck.option("bed")
        }
        self.map_qc_gene.set_options(opts)
        self.map_qc_gene.on("end", self.set_output, "map_qc_gene")
        self.map_qc_gene.run()

    def run_map_gene(self):
        opts = {
            "ref_genome_custom": self.seq_abs.option("query"),
            "ref_genome": "customer_mode",
            "mapping_method": self.option("seq_method").lower(),  # 比对软件
            "seq_method": self.option("fq_type"),   # PE or SE
            "fastq_dir": self.qc.option("sickle_dir"),
            "assemble_method": self.option("assemble_method"),
            "mate_std": self.option("mate_std"),
            "mid_dis": self.option("mid_dis"),
            "result_reserved": self.option("result_reserved")
        }
        self.map_gene.set_options(opts)
        self.map_gene.on("end", self.set_output, "map_gene")
        self.map_gene.run()

    def run_mapping(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome": "customer_mode",
            "mapping_method": self.option("seq_method").lower(),  # 比对软件
            "seq_method": self.option("fq_type"),   # PE or SE
            "fastq_dir": self.qc.option("sickle_dir"),
            "assemble_method": self.option("assemble_method"),
            "mate_std": self.option("mate_std"),
            "mid_dis": self.option("mid_dis"),
            "result_reserved": self.option("result_reserved")
        }
        self.mapping.set_options(opts)
        self.mapping.on("end", self.set_output, "mapping")
        self.mapping.run()

    def run_assembly(self):
        self.logger.info("开始运行拼接步骤")
        opts = {
            "sample_bam_dir": self.mapping.option("bam_output"),
            "assemble_method": self.option("assemble_method"),
            "ref_gtf": self.filecheck.option("gtf"),
            "ref_fa": self.ref_genome
        }
        # 如果具有链特异性
        if self.option("strand_specific"):
            if self.option("strand_dir") == "forward":
                strand_dir = "firststrand"
            else:
                strand_dir = "secondstrand"
            opts.update = ({
                "strand_direct": strand_dir,
                "fr_stranded": "fr-stranded"
                })
        else:
            opts.update({
                "fr_stranded": "fr-unstranded"
                })
        self.assembly.set_options(opts)
        self.assembly.on("end", self.set_output, "assembly")
        self.assembly.on('start', self.set_step, {'start': self.step.assembly})
        self.assembly.on('end', self.set_step, {'end': self.step.assembly})
        self.assembly.run()

    def run_new_transcripts_abs(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome_gtf": self.assembly.option("new_transcripts_gtf")
        }
        self.new_trans_abs.set_options(opts)
        self.new_trans_abs.run()

    def run_new_gene_abs(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome_gtf": self.assembly.option("new_gene_gtf")
        }
        self.new_gene_abs.set_options(opts)
        self.new_gene_abs.run()

    def run_new_align(self, event):
        method = event["data"]
        self.new_blast_modules = []
        self.gene_list = self.new_gene_abs.option('gene_file')
        blast_lines = int(self.new_trans_abs.option('query').prop['seq_number']) / 10
        self.logger.info('.......blast_lines:%s' % blast_lines)
        blast_opts = {
            'query': self.new_trans_abs.option('query'),
            'query_type': 'nucl',
            'database': None,
            'blast': 'blastx',
            'evalue': None,
            'outfmt': 5,
            'lines': blast_lines,
        }
        if 'go' in self.option('database') or 'nr' in self.option('database'):
            self.new_blast_nr = self.add_module('align.' + method)
            blast_opts.update(
                {
                    'database': self.option("nr_database"),
                    'evalue': self.option('nr_blast_evalue')
                }
            )
            self.new_blast_nr.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_nr)
            self.new_blast_nr.on('end', self.set_output, 'new_nrblast')
            self.new_blast_nr.run()
        if 'cog' in self.option('database'):
            self.new_blast_string = self.add_module('align.' + method)
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.new_blast_string.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_string)
            self.new_blast_string.on('end', self.set_output, 'new_stringblast')
            self.new_blast_string.run()
        if 'kegg' in self.option('database'):
            self.new_blast_kegg = self.add_module('align.' + method)
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.new_blast_kegg.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_kegg)
            self.new_blast_kegg.on('end', self.set_output, 'new_keggblast')
            self.new_blast_kegg.run()
        if 'swissprot' in self.option('database'):
            self.new_blast_swissprot = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'swissprot', 'evalue': self.option('swissprot_blast_evalue')}
            )
            self.new_blast_swissprot.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_swissprot)
            self.new_blast_swissprot.on('end', self.set_output, 'new_swissprotblast')
            self.new_blast_swissprot.run()
        if 'pfam' in self.option("database"):
            opts = {
                "fasta": self.new_trans_abs.option('query'),
                "search_pfam": True,
                "p_length": self.option("p_length"),
                "Markov_length": self.option("markov_length")
            }
            self.pfam.set_options(opts)
            self.pfam.on("end", self.set_output, "pfam")
            self.new_blast_modules.append(self.pfam)
            self.pfam.run()
        self.on_rely(self.new_blast_modules, self.run_new_annotation)

    def run_new_annotation(self):
        anno_opts = {
            'gene_file': self.new_gene_abs.option('gene_file'),
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        if 'go' in self.option('database'):
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.new_blast_nr.option('outxml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.option('database'):
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.new_blast_nr.option('outxml'),
            })
        else:
            anno_opts.update({'nr_annot': False})
        if 'kegg' in self.option('database'):
            anno_opts.update({
                'blast_kegg_xml': self.new_blast_kegg.option('outxml'),
            })
        if 'cog' in self.option('database'):
            anno_opts.update({
                'blast_string_xml': self.new_blast_string.option('outxml'),
            })
        if 'swissprot' in self.option("database"):
            anno_opts.update({
                'blast_swissprot_xml': self.new_blast_swissprot.option('outxml'),
            })
        if 'pfam' in self.option("database"):
            anno_opts.update({
                'pfam_domain': self.pfam.output_dir + "/pfam_domain"
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.new_annotation.set_options(anno_opts)
        self.new_annotation.on('end', self.set_output, 'new_annotation')
        self.new_annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.new_annotation.run()

    def run_snp(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome":  "customer_mode",
            "ref_gtf": self.filecheck.option("gtf"),
            "seq_method": self.option("fq_type"),
            "fastq_dir": self.qc.option("sickle_dir")
        }
        self.snp_rna.set_options(opts)
        self.final_tools.append(self.snp_rna)
        self.snp_rna.on("end", self.set_output, "snp_rna")
        self.snp_rna.run()

    def run_map_assess(self):
        opts = {
            "bam": self.mapping.option("bam_output"),
            "bed": self.filecheck.option("bed")
        }
        self.map_qc.set_options(opts)
        self.map_qc.on("end", self.set_output, "map_qc")
        self.map_qc.run()

    def run_exp_rsem_default(self):  # 表达量与表达差异模块
        self.logger.info("开始运行表达量模块")
        opts = {
            "express_method": "rsem",
            "fastq_dir": self.qc.option("sickle_dir"),
            "fq_type": self.option("fq_type"),
            "ref_gtf": self.filecheck.option("gtf"),
            "merged_gtf": self.assembly.option("merged_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.assembly.option("merged_fa"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_ci": self.option("diff_ci"),
            "is_duplicate": self.option("is_duplicate"),
            "exp_way": self.option("exp_way"),
            "strand_dir": self.option("strand_dir")
        }
        mod = self.exp
        mod.set_options(opts)
        mod.on("end", self.set_output, "exp")
        mod.on('start', self.set_step, {'start': self.step.exp})
        mod.on('end', self.set_step, {'end': self.step.exp})
        mod.run()

    def run_exp_rsem_alter(self):
        if self.option("exp_way") == "fpkm":
            exp_way = "tpm"
        else:
            exp_way = "fpkm"
        self.logger.info("开始运行表达量模块,alter")
        opts = {
            "express_method": "rsem",
            "fastq_dir": self.qc.option("sickle_dir"),
            "fq_type": self.option("fq_type"),
            "ref_gtf": self.filecheck.option("gtf"),
            "merged_gtf": self.assembly.option("merged_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.assembly.option("merged_fa"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_ci": self.option("diff_ci"),
            "is_duplicate": self.option("is_duplicate"),
            "exp_way": exp_way,
            "strand_dir": self.option("strand_dir")
        }
        mod = self.exp_alter
        mod.set_options(opts)
        mod.on("end", self.set_output, "exp_rsem_alter")
        mod.on('start', self.set_step, {'start': self.step.exp})
        mod.on('end', self.set_step, {'end': self.step.exp})
        mod.run()

    def run_exp_fc(self):
        self.logger.info("开始运行表达量模块,fc_fpkm")
        opts = {
            "express_method": "featurecounts",
            "fastq_dir": self.qc.option("sickle_dir"),
            "fq_type": self.option("fq_type"),
            "ref_gtf": self.filecheck.option("gtf"),
            "merged_gtf": self.assembly.option("merged_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.assembly.option("merged_fa"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method":  self.option("diff_method"),
            "diff_ci": self.option("diff_ci"),
            "is_duplicate": self.option("is_duplicate"),
            "exp_way": "all",
            "strand_dir": self.option("strand_dir")
        }
        mod = self.exp_fc
        mod.set_options(opts)
        mod.on("end", self.set_output, "exp_fc_all")
        mod.on('start', self.set_step, {'start': self.step.exp})
        mod.on('end', self.set_step, {'end': self.step.exp})
        mod.run()

    def run_network_trans(self):
        opts = {
            "diff_exp_gene": self.exp.output_dir + "/diff/trans_diff/diff_list",
            "species": int(self.taxon_id),
            "combine_score": self.option("combine_score")
        }
        self.network_trans.set_options(opts)
        self.network_trans.on("end", self.set_output, "network_analysis")
        self.network_trans.on('start', self.set_step, {'start': self.step.network_analysis})
        self.network_trans.on('end', self.set_step, {'end': self.step.network_analysis})
        self.network_trans.run()

    def run_network_gene(self):
        opts = {
            "diff_exp_gene": self.exp.output_dir + "/diff/genes_diff/diff_list",
            "species": int(self.taxon_id),
            "combine_score": self.option("combine_score")
        }
        self.network_gene.set_options(opts)
        self.network_gene.on("end", self.set_output, "network_analysis")
        self.network_gene.on('start', self.set_step, {'start': self.step.network_analysis})
        self.network_gene.on('end', self.set_step, {'end': self.step.network_analysis})
        self.network_gene.run()

    def run_altersplicing(self):
        if self.option("strand_specific"):
            lib_type = "fr-firststrand"
        else:
            lib_type = "fr-unstranded"
        opts = {
            "sample_bam_dir": self.mapping.option("bam_output"),
            "lib_type": lib_type,
            "ref_gtf": self.filecheck.option("gtf"),
            "group_table": self.option("group_table"),
            "rmats_control": self.option("control_file")
        }
        if self.option("fq_type") == "PE":
            opts.update({"seq_type": "paired"})
        else:
            opts.update({"seq_type": "single"})
        self.altersplicing.set_options(opts)
        self.altersplicing.on("end", self.set_output, "altersplicing")
        self.altersplicing.on('start', self.set_step, {'start': self.step.altersplicing})
        self.altersplicing.on('end', self.set_step, {'end': self.step.altersplicing})
        self.final_tools.append(self.altersplicing)
        self.altersplicing.run()

    def run_merge_annot(self):
        """
        根据新加入模块操作，修改self.annotation
        :return:
        """
        gos_dir_trans = self.annotation.output_dir + "/go/query_gos.list" + \
            ";" + self.new_annotation.output_dir + "/go/query_gos.list"
        kegg_table_dir_trans = self.annotation.output_dir + "/kegg/kegg_table.xls" + \
            ";" + self.new_annotation.output_dir + "/kegg/kegg_table.xls"
        cog_table_dir_trans = self.annotation.output_dir + "/cog/cog_table.xls" + \
            ";" + self.new_annotation.output_dir + "/cog/cog_table.xls"
        gos_dir_gene = self.annotation.output_dir + "/anno_stat/go_stat/gene_gos.list" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/go_stat/gene_gos.list"
        kegg_table_dir_gene = self.annotation.output_dir + "/anno_stat/kegg_stat/gene_kegg_table.xls" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/kegg_stat/gene_kegg_table.xls"
        cog_table_dir_gene = self.annotation.output_dir + "/anno_stat/cog_stat/gene_cog_table.xls" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/cog_stat/gene_cog_table.xls"
        trans_opts = {
            "gos_dir": gos_dir_trans,
            "kegg_table_dir": kegg_table_dir_trans,
            "cog_table_dir": cog_table_dir_trans
        }
        gene_opts = {
            "gos_dir": gos_dir_gene,
            "kegg_table_dir": kegg_table_dir_gene,
            "cog_table_dir": cog_table_dir_gene
        }
        self.merge_trans_annot.set_options(trans_opts)
        self.merge_gene_annot.set_options(gene_opts)
        self.merge_trans_annot.run()
        self.merge_gene_annot.run()

    def run_exp_trans_diff(self):
        with open(self.exp.output_dir + "/diff/trans_diff/diff_list", "r") as f:
            content = f.read()
        if not content:
            self.exp_diff_trans.start_listener()
            self.exp_diff_trans.fire("end")
        else:
            exp_diff_opts = {
                'diff_fpkm': self.exp.output_dir + "/diff/trans_diff/diff_fpkm",
                'analysis': self.option('exp_analysis'),
                'diff_list': self.exp.output_dir + "/diff/trans_diff/diff_list"
            }
            if 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'gene_kegg_table': self.merge_trans_annot.option('kegg_table'),
                    'diff_list_dir': self.exp.output_dir + "/diff/trans_diff/diff_list_dir",
                     'all_list': self.exp.output_dir + "/rsem/trans_list",
                })
            if 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'gene_go_list': self.merge_trans_annot.option('golist_out'),
                    'diff_list_dir': self.exp.output_dir + "/diff/trans_diff/diff_list_dir",
                    'all_list': self.exp.output_dir + "/rsem/trans_list",
                    'gene_go_level_2': self.merge_trans_annot.option('go2level_out')
                })
            if 'cog_class' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'cog_table': self.merge_trans_annot.option('cog_table'),
                    'diff_list_dir': self.exp.output_dir + "/diff/trans_diff/diff_list_dir",
                })
            if 'kegg_regulate' in self.option('exp_analysis') or 'go_regulate' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'diff_stat_dir': self.exp.output_dir + "/diff/trans_diff/diff_stat_dir"
                })
            self.exp_diff_trans.set_options(exp_diff_opts)
            self.exp_diff_trans.on('end', self.set_output, 'exp_diff_trans')
            self.exp_diff_trans.run()

    def run_exp_gene_diff(self):
        with open(self.exp.output_dir + "/diff/genes_diff/diff_list", "r") as f:
            content = f.read()
        if not content:
            self.exp_diff_gene.start_listener()
            self.exp_diff_gene.fire("end")
        else:
            exp_diff_opts = {
                'diff_fpkm': self.exp.output_dir + "/diff/genes_diff/diff_fpkm",
                'analysis': self.option('exp_analysis'),
                'diff_list': self.exp.output_dir + "/diff/genes_diff/diff_list"
            }
            if 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'gene_kegg_table': self.merge_gene_annot.option('kegg_table'),
                    'diff_list_dir': self.exp.output_dir + "/diff/genes_diff/diff_list_dir",
                     'all_list': self.exp.output_dir + "/rsem/gene_list",
                })
            if 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'gene_go_list': self.merge_gene_annot.option('golist_out'),
                    'diff_list_dir': self.exp.output_dir + "/diff/genes_diff/diff_list_dir",
                    'all_list': self.exp.output_dir + "/rsem/gene_list",
                    'gene_go_level_2': self.merge_gene_annot.option('go2level_out')
                })
            if 'cog_class' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'cog_table': self.merge_gene_annot.option('cog_table'),
                    'diff_list_dir': self.exp.output_dir + "/diff/genes_diff/diff_list_dir",
                })
            if 'kegg_regulate' in self.option('exp_analysis') or 'go_regulate' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'diff_stat_dir': self.exp.output_dir + "/diff/genes_diff/diff_stat_dir"
                })
            self.exp_diff_gene.set_options(exp_diff_opts)
            self.exp_diff_gene.on('end', self.set_output, 'exp_diff_gene')
            self.exp_diff_gene.run()

    def get_group_from_edger_group(self):  # 用来判断是否进行可变剪切分析
        group_spname = self.option("group_table").get_group_spname()
        if self.option("fq_type") == "PE":
            for key in group_spname.keys():
                if len(group_spname[key]) <= 3:
                    self.logger.info("某分组中样本数小于等于3，将不进行可变剪切分析")
                    return False
        else:
            return True

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
            self.logger.info('{}'.format(self.qc_stat_before._upload_dir_obj))
        if event['data'] == 'qc_stat_after':
            self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
            self.logger.info('{}'.format(self.qc_stat_after._upload_dir_obj))
        if event['data'] == 'mapping':
            self.move2outputdir(obj.output_dir, 'mapping')
            self.logger.info('mapping results are put into output dir')
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, 'map_qc')
            self.logger.info('mapping assessments are done')
        if event['data'] == 'assembly':
            self.move2outputdir(obj.output_dir, 'assembly')
            self.logger.info('assembly are done')
        if event['data'] == 'exp':
            self.move2outputdir(obj.output_dir, 'express')
            self.logger.info('express文件移动完成')
        if event["data"] == "exp_alter":
            self.move2outputdir(obj.output_dir, 'exp_alter')
            self.logger.info('express_alter文件移动完成')
        if event['data'] == 'exp_fc_all':
            self.move2outputdir(obj.output_dir, 'express_fc_all')
            self.logger.info('express_fc_all文件移动完成')
        if event['data'] == 'exp_diff_gene':
            self.move2outputdir(obj.output_dir, 'express_diff_gene')
            self.logger.info("express diff")
        if event['data'] == 'exp_diff_trans':
            self.move2outputdir(obj.output_dir, 'express_diff_trans')
            self.logger.info("express diff")
        if event['data'] == 'snp_rna':
            self.move2outputdir(obj.output_dir, 'snp_rna')
            self.logger.info("snp_rna文件移动完成")
        if event['data'] == 'network_analysis':
            self.move2outputdir(obj.output_dir, 'network_analysis')
            self.logger.info("network_analysis文件移动完成")
        if event['data'] == 'annotation':
            self.move2outputdir(obj.output_dir, 'annotation')
            self.logger.info("annotation文件移动完成")
        if event['data'] == 'new_annotation':
            self.move2outputdir(obj.output_dir, 'new_annotation')
            self.logger.info("新转录本与新基因annotation文件移动完成")
        if event['data'] == 'altersplicing':
            self.move2outputdir(obj.output_dir, 'altersplicing')
            self.logger.info("altersplicing文件移动完成")
        if event["data"] == "new_keggblast":
            self.move2outputdir(obj.output_dir, 'new_keggblast')
            self.logger.info("new_keggblast文件移动完成")
        if event["data"] == "new_stringblast":
            self.move2outputdir(obj.output_dir, 'new_stringblast')
            self.logger.info("new_stringblast文件移动完成")
        if event["data"] == "new_nrblast":
            self.move2outputdir(obj.output_dir, 'new_nrblast')
            self.logger.info("new_nrblast文件移动完成")
        if event["data"] == "keggblast":
            self.move2outputdir(obj.output_dir, 'keggblast')
            self.logger.info("keggblast文件移动完成")
        if event["data"] == "stringblast":
            self.move2outputdir(obj.output_dir, 'stringblast')
            self.logger.info("stringblast文件移动完成")
        if event["data"] == "nrblast":
            self.move2outputdir(obj.output_dir, 'nrblast')
            self.logger.info("nrblast文件移动完成")
        if event["data"] == "map_gene":
            self.move2outputdir(obj.output_dir, 'map_gene')
            self.logger.info("map_gene文件移动完成")
        if event["data"] == "swsissprot":
            self.move2outputdir(obj.output_dir, 'new_swissprotblast')
            self.logger.info("swissprot文件移动完成")
        if event["data"] == "pfam":
            self.move2outputdir(obj.output_dir, 'pfam')
            self.logger.info("pfam文件移动完成")

    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_seq_abs)
        self.filecheck.on("end", self.run_gs)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        self.on_rely([self.qc, self.seq_abs], self.run_map_gene)
        self.map_gene.on("end", self.run_map_assess_gene)
        # self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.on_rely([self.map_qc, self.map_qc_gene, self.gs, self.qc_stat_after], self.run_map_api)
        self.assembly.on("end", self.run_exp_rsem_default)
        self.assembly.on("end", self.run_exp_rsem_alter)
        self.assembly.on("end", self.run_exp_fc)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        if self.option("blast_method") == "diamond":
            self.assembly.on('end', self.run_align, "diamond")
            self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "diamond")
        else:
            self.assembly.on('end', self.run_align, "blast")
            self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "blast")
        self.on_rely([self.new_annotation, self.annotation], self.run_merge_annot)
        self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        if self.taxon_id != "":
            self.exp.on("end", self.run_network_trans)
            self.exp.on("end", self.run_network_gene)
            self.final_tools.append(self.network_gene)
            self.final_tools.append(self.network_trans)
        self.on_rely(self.final_tools, self.end)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def end(self):
        super(RefrnaWorkflow, self).end()

    def run_map_api(self):
        self.export_qc()
        self.export_map_assess()
        # self.IMPORT_REPORT_AFTER_END = True
        self.run_assembly()
        if self.filecheck.option("genome_status"):
            if self.get_group_from_edger_group():
                self.run_altersplicing()
            self.run_snp()

    def export_qc(self):
        self.api_qc = self.api.ref_rna_qc
        qc_stat = self.qc_stat_after.output_dir
        fq_type = self.option("fq_type").lower()
        samples_list = self.api_qc.add_samples_info(qc_stat, fq_type=fq_type)
        quality_stat_after = self.qc_stat_after.output_dir + "/qualityStat"
        self.api_qc.add_gragh_info(quality_stat_after, "after")
        quality_stat_before = self.qc_stat_before.output_dir + "/qualityStat"  # 将qc前导表加于该处
        self.api_qc.add_gragh_info(quality_stat_before, "before")
        self.group_id, self.group_detail, self.group_category = self.api_qc.add_specimen_group(self.option("group_table").prop["path"])
        self.logger.info(self.group_detail)
        self.control_id, self.compare_detail = self.api_qc.add_control_group(self.option("control_file").prop["path"], self.group_id)

    def test_run(self):
        self.export_qc()
        # self.export_assembly()
        # self.export_map_assess()
        self.export_exp_rsem_default()
        # self.exp_alter.mergersem = self.exp_alter.add_tool("rna.merge_rsem")
        # self.exp.mergersem = self.exp.add_tool("rna.merge_rsem")
        # self.export_exp_rsem_alter()
        # self.exp_fc.featurecounts = self.exp_fc.add_tool("rna.featureCounts")
        # self.exp_fc.mergersem = self.exp_fc.add_tool("rna.merge_rsem")
        # self.exp.mergersem = self.exp.add_tool("rna.merge_rsem")
        # self.export_exp_fc()
        # self.export_gene_set()
        self.export_diff_gene()
        self.export_diff_trans()
        # self.export_cor()
        # self.export_pca()
        # self.export_annotation()

    def export_assembly(self):
        self.api_assembly = self.api.api("ref_rna.ref_assembly")
        if self.option("assemble_method") == "cufflinks":
            all_gtf_path = self.assembly.output_dir + "/Cufflinks"
            merged_path = self.assembly.output_dir + "/Cuffmerge"
        else:
            all_gtf_path = self.assembly.output_dir + "/Stringtie"
            merged_path = self.assembly.output_dir + "/StringtieMerge"
        self.api_assembly.add_assembly_result(all_gtf_path=all_gtf_path, merged_path=merged_path)

    def export_map_assess(self):
        self.api_map = self.api.ref_rna_qc
        stat_file = self.map_qc.output_dir + "/bam_stat.xls"
        self.api_map.add_mapping_stat(stat_file, "genome")
        stat_file = self.map_qc_gene.output_dir + "/bam_stat.xls"
        self.api_map.add_mapping_stat(stat_file, "gene")
        file_path = self.map_qc.output_dir + "/satur"
        self.api_map.add_rpkm_table(file_path)
        coverage = self.map_qc.output_dir + "/coverage"
        self.api_map.add_coverage_table(coverage)
        distribution = self.map_qc.output_dir + "/distribution"
        self.api_map.add_distribution_table(distribution)
        chrom_distribution = self.map_qc.output_dir + "chr_stat"
        self.api_map.add_chorm_distribution_table(chrom_distribution)

    def export_exp_rsem_default(self):
        self.exp.mergersem = self.exp.add_tool("rna.merge_rsem")
        self.api_exp = self.api.refrna_express
        rsem_dir = self.exp.output_dir + "/rsem"
        if self.option("is_duplicate"):
            group_fpkm_path = self.exp.mergersem.work_dir + "/group"
            is_duplicate = True
        else:
            group_fpkm_path = None
            is_duplicate = False
        with open(rsem_dir + "/genes.counts.matrix") as f:
            samples = f.readline().strip().split("\t")
        params={}
        params["express_method"] = "rsem"
        params["type"] = self.option("exp_way")
        params["group_id"] = str(self.group_id)
        # params["group_detail"] = self.group_detail
        params['group_detail'] = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            params['group_detail'][key] = value
        self.logger.info(params['group_detail'])
        distri_path = self.exp.mergersem.work_dir
        class_code = self.exp.mergersem.work_dir + "/class_code"
        self.express_id = self.api_exp.add_express(rsem_dir=rsem_dir, group_fpkm_path=group_fpkm_path, is_duplicate=is_duplicate,
                                                   class_code=class_code, samples=samples, params=params, major=True, distri_path=distri_path)

    def export_exp_rsem_alter(self):
        self.api_exp = self.api.refrna_express
        rsem_dir = self.exp_alter.output_dir + "/rsem"
        if self.option("is_duplicate"):
            group_fpkm_path = self.exp_alter.mergersem.work_dir + "/group"
            is_duplicate = True
        else:
            group_fpkm_path = None
            is_duplicate = False
        with open(rsem_dir + "/genes.counts.matrix") as f:
            samples = f.readline().strip().split("\t")
        params={}
        params["express_method"] = "rsem"
        if self.option("exp_way") == "fpkm":
            params["type"] = "tpm"
        else:
            params["type"] = "fpkm"
        params["group_id"] = str(self.group_id)
        params['group_detail'] = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            params['group_detail'][key] = value
        self.logger.info(params['group_detail'])
        distri_path = self.exp_alter.mergersem.work_dir
        class_code = self.exp.mergersem.work_dir + "/class_code"
        self.api_exp.add_express(rsem_dir=rsem_dir, group_fpkm_path=group_fpkm_path, is_duplicate=is_duplicate,
                             class_code=class_code, samples=samples, params=params, major=True, distri_path=distri_path)

    def export_exp_fc(self):
        self.api_exp = self.api.refrna_express
        feature_dir = self.exp_fc.output_dir + "/featurecounts"
        if self.option("is_duplicate"):
            group_fpkm_path = self.exp_fc.featurecounts.work_dir + "/group"
            is_duplicate = True
        else:
            group_fpkm_path = None
            is_duplicate = False
        with open(feature_dir+"/count.xls", 'r+') as f1:
            samples = f1.readline().strip().split("\t")
        params = dict()
        params["express_method"] = "featurecounts"
        params["type"] = "fpkm"
        params["group_id"] = str(self.group_id)
        params['group_detail'] = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            params['group_detail'][key] = value
        self.logger.info(params['group_detail'])
        distri_path = self.exp_fc.featurecounts.work_dir
        class_code = self.exp.mergersem.work_dir + "/class_code"
        self.api_exp.add_express_feature(feature_dir=feature_dir, group_fpkm_path=group_fpkm_path, is_duplicate=is_duplicate, samples=samples,
                            class_code=class_code, params=params, major=True, distri_path=distri_path)
        params2 = dict()
        params2["express_method"] = "featurecounts"
        params2["type"] = "tpm"
        params2["group_id"] = str(self.group_id)
        params2['group_detail'] = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            params2['group_detail'][key] = value
        self.logger.info(params2['group_detail'])
        self.api_exp.add_express_feature(feature_dir=feature_dir, group_fpkm_path=group_fpkm_path, is_duplicate=is_duplicate, samples=samples,
                            class_code=class_code, params=params2, major=True, distri_path=distri_path)

    def export_gene_set(self):
        self.api_geneset = self.api.refrna_express
        group_id = self.group_id
        path = self.exp.output_dir + "/diff/trans_diff/diff_stat_dir"
        self.transet_id = list()
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down, self.gene_list_up_down_trans = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name, compare_name=compare_name,
                                                           express_method="rsem", type="transcript", up_down='up_down', major=True)
                    down_id, gene_list = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name,
                                                           compare_name=compare_name, express_method="rsem",
                                                           type="transcript", up_down='down')
                    up_id, gene_list = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                         group_id=group_id, name=name, compare_name=compare_name,
                                                         express_method="rsem", type="transcript", up_down='up')
                    self.transet_id.append(up_down)
                else:
                    self.logger.info("转录本name和compare_name匹配错误")
        path = self.exp.output_dir + "/diff/genes_diff/diff_stat_dir"
        self.geneset_id = list()
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down, self.gene_list_up_down_gene = self.api_geneset.add_geneset(diff_stat_path = path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene",up_down='up_down')
                    down_id, gene_list = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene", up_down='down')
                    up_id, gene_list = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id, name=name,
                                                         compare_name=compare_name, express_method="rsem", type="gene",
                                                         up_down='up')
                    self.geneset_id.append(up_down)
                else:
                    self.logger.info("基因name和compare_name匹配错误")

    def export_diff_trans(self):
        path = self.exp.output_dir + "/diff/trans_diff"
        exp_path = self.exp.output_dir + "/rsem"
        with open(exp_path + "/transcripts.counts.matrix", 'r+') as f1:
            sample = f1.readline().strip().split("\t")
        compare_column = self.compare_detail
        params = {}
        merge_path = path + "/merge.xls"
        params['group_id'] = self.group_id
        params['control_id'] = self.control_id
        params['group_detail'] = dict()
        compare_column_specimen = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            value2 = self.group_detail[i].values()
            params['group_detail'][key] = value
            compare_column_specimen[key] = value2
        params['express_id'] = str(self.express_id)
        params['fc'] = 0  # 可能会改动
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_ci")
        params['diff_method'] = self.option("diff_method")
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,
                                                        class_code=class_code, diff_exp_dir=path,
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        query_type="transcript", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, merge_path)

    def export_diff_gene(self):
        path = self.exp.output_dir + "/diff/genes_diff"
        exp_path = self.exp.output_dir + "/rsem"
        with open(exp_path + "/genes.counts.matrix", 'r+') as f1:
            sample = f1.readline().strip().split("\t")
        compare_column = self.compare_detail
        params = {}
        merge_path = path + "/merge.xls"
        params['group_id'] = str(self.group_id)
        params['control_id'] = str(self.control_id)
        params['group_detail'] = dict()
        compare_column_specimen = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            value2 = self.group_detail[i].values()
            params['group_detail'][key] = value
            compare_column_specimen[key] = value2
        params['express_id'] = str(self.express_id)
        params['fc'] = 0  # 可能会改动
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_ci")
        params['diff_method'] = self.option("diff_method")
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,
                                                        class_code=class_code, diff_exp_dir=path,
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        query_type="gene", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, merge_path)

    def export_cor(self):
        self.api_cor = self.api.refrna_corr_express
        correlation = self.exp.output_dir + "/correlation/genes_correlation"
        group_id = str(self.group_id)
        group_detail = self.group_detail
        self.api_cor.add_correlation_table(correlation=correlation, group_id=group_id, group_detail=group_detail,
                                           express_id=self.express_id, detail=True, seq_type="gene")

    def export_pca(self):
        self.api_pca = self.api.refrna_corr_express
        pca_path = self.exp.output_dir + "/pca/genes_pca"
        self.api_pca.add_pca_table(pca_path, group_id=str(self.group_id), group_detail=self.group_detail,
                                   express_id=self.express_id, detail=True, seq_type="gene")

    def export_annotation(self):
        self.api_anno = self.api.api("ref_rna.ref_annotation")
        ref_anno_path = self.annotation.output_dir
        params = {
            "nr_evalue": self.option("nr_blast_evalue"),
            "swissprot_evalue": self.option("swissprot_blast_evalue")
        }
        params = json.dumps(params)
        new_anno_path = self.new_annotation.output_dir
        pfam_path = self.pfam.output_dir + "/pfam_domain"
        self.api_anno.add_annotation(name=None, params=params, ref_anno_path=ref_anno_path, new_anno_path=new_anno_path, pfam_path=pfam_path)

    def export_as(self):
        self.api_as = self.api.refrna_splicing_rmats
        if self.option("strand_specific"):
            lib_type = "fr-firststrand"
        else:
            lib_type = "fr-unstranded"
        if self.option("fq_type") == "PE":
            seq_type = "paired"
        else:
            seq_type = "single"
        params = {
            "ana_mode": "P",
            "as_diff": 0.001,
            "group_id": str(self.group_id),
            "lib_type": lib_type,
            "read_len": 150,
            "ref_gtf": self.filecheck.option("gtf").prop["path"],
            "seq_type": seq_type,
        }
        outpath = self.altersplicing.output_dir
        self.api_as.add_sg_splicing_rmats(self, params=params, major=True, group={}, ref_gtf=self.filecheck.option("gtf").prop["path"], name=None, outpath=outpath)

    def export_ppi(self):
        api_ppinetwork = self.api.ppinetwork
        self.ppi_id = api_ppinetwork.add_ppi_main_id(str(self.transet_id[0]), self.option("combine_score"), "trans", self.taxon_id)
        self.ppi_id = str(self.ppi_id)
        all_nodes_path = self.network_trans.output_dir + '/ppinetwork_predict/all_nodes.txt'   # 画图节点属性文件
        interaction_path = self.network_trans.output_dir + '/ppinetwork_predict/interaction.txt'  # 画图的边文件
        network_stats_path = self.network_trans.output_dir + '/ppinetwork_predict/network_stats.txt'  # 网络全局属性统计
        network_centrality_path = self.network_trans.output_dir + '/ppinetwork_topology/protein_interaction_network_centrality.txt'
        network_clustering_path = self.network_trans.output_dir + '/ppinetwork_topology/protein_interaction_network_clustering.txt'
        network_transitivity_path = self.network_trans.output_dir + '/ppinetwork_topology/protein_interaction_network_transitivity.txt'
        degree_distribution_path = self.network_trans.output_dir + '/ppinetwork_topology/protein_interaction_network_degree_distribution.txt'
        network_node_degree_path = self.network_trans.output_dir + '/ppinetwork_topology/protein_interaction_network_node_degree.txt'
        api_ppinetwork.add_node_table(file_path=all_nodes_path, table_id=self.ppi_id)   # 节点的属性文件（画网络图用）
        api_ppinetwork.add_edge_table(file_path=interaction_path, table_id=self.ppi_id)  # 边信息
        api_ppinetwork.add_network_attributes(file1_path=network_transitivity_path, file2_path=network_stats_path, table_id=self.ppi_id)  # 网络全局属性
        api_ppinetwork.add_network_cluster_degree(file1_path=network_node_degree_path,file2_path=network_clustering_path, table_id=self.ppi_id)  # 节点的聚类与degree，画折线图
        api_ppinetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.ppi_id)  # 中心信息
        api_ppinetwork.add_degree_distribution(file_path=degree_distribution_path, table_id=self.ppi_id)  # 度分布
        self.ppi_id = api_ppinetwork.add_ppi_main_id(str(self.geneset_id[0]), self.option("combine_score"), "gene", self.taxon_id)
        self.ppi_id = str(self.ppi_id)
        all_nodes_path = self.network_gene.output_dir + '/ppinetwork_predict/all_nodes.txt'   # 画图节点属性文件
        interaction_path = self.network_gene.output_dir + '/ppinetwork_predict/interaction.txt'  # 画图的边文件
        network_stats_path = self.network_gene.output_dir + '/ppinetwork_predict/network_stats.txt'  # 网络全局属性统计
        network_centrality_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_centrality.txt'
        network_clustering_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_clustering.txt'
        network_transitivity_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_transitivity.txt'
        degree_distribution_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_degree_distribution.txt'
        network_node_degree_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_node_degree.txt'
        api_ppinetwork.add_node_table(file_path=all_nodes_path, table_id=self.ppi_id)   # 节点的属性文件（画网络图用）
        api_ppinetwork.add_edge_table(file_path=interaction_path, table_id=self.ppi_id)  # 边信息
        api_ppinetwork.add_network_attributes(file1_path=network_transitivity_path, file2_path=network_stats_path, table_id=self.ppi_id)  # 网络全局属性
        api_ppinetwork.add_network_cluster_degree(file1_path=network_node_degree_path,file2_path=network_clustering_path, table_id=self.ppi_id)  # 节点的聚类与degree，画折线图
        api_ppinetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.ppi_id)  # 中心信息
        api_ppinetwork.add_degree_distribution(file_path=degree_distribution_path, table_id=self.ppi_id)  # 度分布

    def export_snp(self):
        self.api_snp = self.api.api("ref_rna.ref_snp")
        snp_anno = self.snp_rna.output_dir
        self.api_snp.add_snp_main(snp_anno)
