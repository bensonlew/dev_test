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
from collections import OrderedDict

class RefrnaWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(RefrnaWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "workflow_type", "type": "string", "default": "transcriptome"},  # 转录组
            {"name": "taxonmy", "type":"string", "default": "animal"},
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

            # 增加evalue参数，再转换为float传给module使用
            {"name": "nr_evalue", "type": "string", "default": "1e-5"},
            {"name": "string_evalue", "type": "string", "default": "1e-5"},
            {"name": "kegg_evalue", "type": "string", "default": "1e-5"},
            {"name": "swissprot_evalue", "type": "string", "default": "1e-5"},

            {"name": "nr_blast_evalue", "type": "float", "default": 1e-5},  # NR比对e值
            {"name": "string_blast_evalue", "type": "float", "default": 1e-5},  # String比对使用的e值
            {"name": "kegg_blast_evalue", "type": "float", "default": 1e-5},  # KEGG注释使用的e值
            {"name": "swissprot_blast_evalue", "type": "float", "default": 1e-5},  # Swissprot比对使用的e值
            {"name": "database", "type": "string", "default": 'go,nr,cog,kegg,swissprot,pfam'},
            # 全部六个注释
            {"name": "nr_database", "type": "string", "default": "animal"},  # nr库类型
            {"name": "kegg_database", "type": "string", "default": None},  # kegg注释库类型

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

            {"name": "diff_method", "type": "string", "default": "DESeq2"},
            # 差异表达分析方法
            {"name": "diff_fdr_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "fc", "type": "float", "default": 2},
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
        self.star_mapping = self.add_module("rna.rnaseq_mapping")
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
        self.anno_path = ""
        if self.option("ref_genome") != "customer_mode":
            self.ref_genome = self.json_dict[self.option("ref_genome")]["ref_genome"]
            self.option("ref_genome_custom", self.ref_genome)
            self.taxon_id = self.json_dict[self.option("ref_genome")]["taxon_id"]
            self.anno_path = self.json_dict[self.option("ref_genome")]["anno_path"]
            self.logger.info(self.anno_path)
        else:
            self.ref_genome = self.option("ref_genome_custom")
            self.taxon_id = ""
        self.gff = ""
        if self.option("ref_genome") == "customer_mode":
            if self.option('genome_structure_file').format == "gene_structure.gff3":
                self.gff = self.option('genome_structure_file').prop["path"]
        else:
            self.gff = self.json_dict[self.option("ref_genome")]["gff"]
        self.final_tools = [self.snp_rna, self.altersplicing, self.exp_diff_gene, self.exp_diff_trans, self.exp_fc]
        self.genome_status = True
        self.as_on = False  # 是否进行可变剪切
        self.step.add_steps("filecheck", "rna_qc", "mapping", "assembly", "new_annotation", "express", "snp_rna")


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
        try:
            nr_evalue = float(self.option("nr_evalue"))
            string_evalue = float(self.option("string_evalue"))
            kegg_evalue = float(self.option("string_evalue"))
            swissprot_evalue = float(self.option("swissprot_evalue"))
        except:
            raise OptionError("传入的evalue值不符合规范")
        else:
            self.option("nr_blast_evalue", nr_evalue)
            self.option("string_blast_evalue", string_evalue)
            self.option("kegg_blast_evalue", kegg_evalue)
            self.option("swissprot_blast_evalue", swissprot_evalue)
        if not self.option("nr_blast_evalue") > 0 and not self.option("nr_blast_evalue") < 1:
            raise OptionError("NR比对的E值超出范围")
        if not self.option("string_blast_evalue") > 0 and not self.option("string_blast_evalue") < 1:
            raise OptionError("String比对的E值超出范围")
        if not self.option("kegg_blast_evalue") > 0 and not self.option("kegg_blast_evalue") < 1:
            raise OptionError("Kegg比对的E值超出范围")
        if not self.option("swissprot_blast_evalue") > 0 and not self.option("swissprot_blast_evalue") < 1:
            raise OptionError("Swissprot比对的E值超出范围")
        if not self.option("seq_method") in ["Tophat", "Hisat", "Star"]:
            raise OptionError("比对软件应在Tophat,Star与Hisat中选择")
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
            "ref_genome_custom": self.option("ref_genome_custom")
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
        self.filecheck.on('start', self.set_step, {'start': self.step.filecheck})
        self.filecheck.on('end', self.set_step, {'end': self.step.filecheck})
        self.filecheck.run()

    def run_gs(self):
        opts = {
            "in_fasta": self.option("ref_genome_custom"),
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
        self.qc.on('start', self.set_step, {'start': self.step.rna_qc})
        self.qc.on('end', self.set_step, {'end': self.step.rna_qc})
        self.qc.run()

    def run_seq_abs(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        self.seq_abs.set_options(opts)
        # self.seq_abs.on('start', self.set_step, {'start': self.step.seq_abs})
        # self.seq_abs.on('end', self.set_step, {'end': self.step.seq_abs})
        self.seq_abs.run()

    def run_align(self, event):
        method = event["data"]
        self.blast_modules = []
        self.gene_list = self.seq_abs.option('gene_file')
        if int(self.seq_abs.option('query').prop['seq_number']) == 0:
            self.logger.info('.......blast_lines:0')
            self.new_annotation.start_listener()
            self.new_annotation.fire("end")
            return
        blast_lines = int(self.seq_abs.option('query').prop['seq_number']) / 10 + 1
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
            "length_file": self.seq_abs.option("length_file"),
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
            "ref_genome_gtf": self.filecheck.option("gtf"),
            "taxonomy": self.option("kegg_database"),
            "nr_annot": False,
            "length_file": self.seq_abs.option("length_file")
        }
        if self.anno_path != "":  # 本地参考基因组注释文件
            opts.update({
                "gos_list_upload": self.anno_path + "/go.list",
                "kos_list_upload": self.anno_path + "/kegg.list",
                "blast_string_table": self.anno_path + "/cog.list",
            })
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
            "ref_genome_custom": self.option("ref_genome_custom"),
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
        self.mapping.on("start", self.set_step, {"start": self.step.mapping})
        self.mapping.on("end", self.set_step, {"end": self.step.mapping})
        self.mapping.run()

    def run_star_mapping(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome": "customer_mode",
            "mapping_method": "star",
            "seq_method": self.option("fq_type"),   # PE or SE
            "fastq_dir": self.qc.option("sickle_dir"),
            "assemble_method": self.option("assemble_method")
        }
        self.star_mapping.set_options(opts)
        self.genome_status = self.filecheck.option("genome_status")
        if self.genome_status:  # 进行可变剪切分析
            if self.get_group_from_edger_group():
                self.star_mapping.on("end", self.run_altersplicing)
            else:
                self.logger.info("不进行可变剪切分析")
                self.altersplicing.start_listener()
                self.altersplicing.fire("end")
            self.star_mapping.on("end", self.run_snp)
            self.star_mapping.on("end", self.set_output, "mapping")
            self.star_mapping.run()
        else:
            self.logger.info("不进行snp分析与可变剪切分析")
            self.snp_rna.start_listener()
            self.snp_rna.fire("end")
            self.altersplicing.start_listener()
            self.altersplicing.fire("end")

    def run_assembly(self):
        self.logger.info("开始运行拼接步骤")
        opts = {
            "sample_bam_dir": self.mapping.option("bam_output"),
            "assemble_method": self.option("assemble_method"),
            "ref_gtf": self.filecheck.option("gtf"),
            "ref_fa": self.option("ref_genome_custom")
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
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome_gtf": self.assembly.option("new_transcripts_gtf")
        }
        self.new_trans_abs.set_options(opts)
        self.new_trans_abs.run()

    def run_new_gene_abs(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome_gtf": self.assembly.option("new_gene_gtf")
        }
        self.new_gene_abs.set_options(opts)
        self.new_gene_abs.run()

    def run_new_align(self, event):
        method = event["data"]
        self.new_blast_modules = []
        self.gene_list = self.new_gene_abs.option('gene_file')
        blast_lines = int(self.new_trans_abs.option('query').prop['seq_number']) / 10 + 1
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
            "ref_genome_gtf": self.assembly.option("new_transcripts_gtf"),
            'length_file': self.new_trans_abs.option('length_file')
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
                'taxonomy': self.option("kegg_database")
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
        self.new_annotation.on('start', self.set_step, {'start': self.step.new_annotation})
        self.new_annotation.on('end', self.set_step, {'end': self.step.new_annotation})
        self.new_annotation.run()

    def run_snp(self):
        self.logger.info("开始运行snp步骤")
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome":  "customer_mode",
            "ref_gtf": self.filecheck.option("gtf"),
            "seq_method": self.option("fq_type"),
            "in_sam": self.star_mapping.output_dir + "/sam"
        }
        self.snp_rna.set_options(opts)
        self.snp_rna.on("start", self.set_step, {"start": self.step.snp_rna})
        self.snp_rna.on("end", self.set_step, {"end": self.step.snp_rna})
        # self.final_tools.append(self.snp_rna)
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
            "new_gtf": self.assembly.option("new_transcripts_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.option("ref_genome_custom"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "fc": self.option("fc"),
            "is_duplicate": self.option("is_duplicate"),
            "exp_way": self.option("exp_way"),
            "strand_dir": self.option("strand_dir")
        }
        mod = self.exp
        mod.set_options(opts)
        mod.on("end", self.set_output, "exp")
        mod.on('start', self.set_step, {'start': self.step.express})
        mod.on('end', self.set_step, {'end': self.step.express})
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
            "merged_gtf": self.assembly.option("change_id_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.assembly.option("change_id_fa"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "fc": self.option("fc"),
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
            "new_gtf": self.assembly.option("new_transcripts_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.option("ref_genome_custom"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method":  self.option("diff_method"),
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "fc": self.option("fc"),
            "is_duplicate": self.option("is_duplicate"),
            "exp_way": "all",
            "strand_dir": self.option("strand_dir")
        }
        mod = self.exp_fc
        mod.set_options(opts)
        mod.on("end", self.set_output, "exp_fc_all")
        mod.on('start', self.set_step, {'start': self.step.express})
        mod.on('end', self.set_step, {'end': self.step.express})
        mod.run()

    def run_network_trans(self):
        with open(self.exp.option("network_diff_list").prop["path"], "r") as ft:
            ft.readline()
            content = ft.read()
        if not content:
            self.logger.info("无差异转录本，不进行网络分析")
            self.network_trans.start_listener()
            self.network_trans.fire("end")
        else:
            opts = {
                "diff_exp_gene": self.exp.option("network_diff_list"),
                "species": int(self.taxon_id),
                "combine_score": self.option("combine_score")
            }
            self.network_trans.set_options(opts)
            self.network_trans.on("end", self.set_output, "network_analysis")
            self.network_trans.run()

    def run_network_gene(self):
        with open(self.exp.output_dir + "/diff/genes_diff/network_diff_list", "r") as fg:
            fg.readline()
            content = fg.read()
        if not content:
            self.logger.info("无差异基因，不进行网络分析")
            self.network_gene.start_listener()
            self.network_gene.fire("end")
        else:
            opts = {
                "diff_exp_gene": self.exp.output_dir + "/diff/genes_diff/network_diff_list",
                "species": int(self.taxon_id),
                "combine_score": self.option("combine_score")
            }
            self.network_gene.set_options(opts)
            self.network_gene.on("end", self.set_output, "network_analysis")
            self.network_gene.run()

    def run_altersplicing(self):
        if self.option("strand_specific"):
            lib_type = "fr-firststrand"
        else:
            lib_type = "fr-unstranded"
        opts = {
            "sample_bam_dir": self.star_mapping.option("bam_output"),
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
        # self.altersplicing.on('start', self.set_step, {'start': self.step.altersplicing})
        # self.altersplicing.on('end', self.set_step, {'end': self.step.altersplicing})
        # self.final_tools.append(self.altersplicing)
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
            self.logger.info("无差异转录本，不进行差异分析")
            self.exp_diff_trans.start_listener()
            self.exp_diff_trans.fire("end")
        else:
            exp_diff_opts = {
                'diff_fpkm': self.exp.output_dir + "/diff/trans_diff/diff_fpkm",
                'analysis': self.option('exp_analysis'),
                'diff_list': self.exp.output_dir + "/diff/trans_diff/diff_list",
                "is_genelist": True,
                "diff_list_dir": self.exp.output_dir + "/diff/trans_diff/diff_list_dir"
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
            self.logger.info("无差异基因，不进行差异分析")
            self.exp_diff_gene.start_listener()
            self.exp_diff_gene.fire("end")
        else:
            exp_diff_opts = {
                'diff_fpkm': self.exp.output_dir + "/diff/genes_diff/diff_fpkm",
                'analysis': self.option('exp_analysis'),
                'diff_list': self.exp.output_dir + "/diff/genes_diff/diff_list",
                "is_genelist": True,
                "diff_list_dir": self.exp.output_dir + "/diff/genes_diff/diff_list_dir",
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
            lst = []
            for key in group_spname.keys():
                if len(group_spname[key]) <= 3:
                    self.logger.info("某分组中样本数小于等于3，将不进行可变剪切分析")
                    self.as_on = False  # 不进行可变剪切分析
                    return False
                else:
                    lst.append(len(group_spname[key]))
            if len(set(lst)) != 1:
                self.as_on = False  # 不进行可变剪切分析
                return False  # 各分组，样本数不相同
            self.as_on = True  # 不进行可变剪切分析
            return True
        else:
            self.as_on = True
            return True

    def move2outputdir(self, olddir, newname, mode='link'):  # 阻塞
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
                    os.link(oldfiles[i], newfiles[i])
                else:
                    os.system('cp -r {} {}'.format(oldfiles[i], newdir))

    def set_output(self, event):
        pass
        # obj = event["bind_object"]
        # # 设置qc报告文件
        # if event['data'] == 'qc':
        #     self.move2outputdir(obj.output_dir, 'QC_stat')
        # if event['data'] == 'qc_stat_before':
        #     self.move2outputdir(obj.output_dir, 'QC_stat/before_qc')
        #     self.logger.info('{}'.format(self.qc_stat_before._upload_dir_obj))
        # if event['data'] == 'qc_stat_after':
        #     self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
        #     self.logger.info('{}'.format(self.qc_stat_after._upload_dir_obj))
        # if event['data'] == 'mapping':
        #     self.move2outputdir(obj.output_dir, 'mapping')
        #     self.logger.info('mapping results are put into output dir')
        # if event['data'] == 'map_qc':
        #     self.move2outputdir(obj.output_dir, 'map_qc')
        #     self.logger.info('mapping assessments are done')
        # if event['data'] == 'assembly':
        #     self.move2outputdir(obj.output_dir, 'assembly')
        #     self.logger.info('assembly are done')
        # if event['data'] == 'exp':
        #     self.move2outputdir(obj.output_dir, 'express')
        #     self.logger.info('express文件移动完成')
        # if event["data"] == "exp_alter":
        #     self.move2outputdir(obj.output_dir, 'exp_alter')
        #     self.logger.info('express_alter文件移动完成')
        # if event['data'] == 'exp_fc_all':
        #     self.move2outputdir(obj.output_dir, 'express_fc_all')
        #     self.logger.info('express_fc_all文件移动完成')
        # if event['data'] == 'exp_diff_gene':
        #     self.move2outputdir(obj.output_dir, 'express_diff_gene')
        #     self.logger.info("express diff")
        # if event['data'] == 'exp_diff_trans':
        #     self.move2outputdir(obj.output_dir, 'express_diff_trans')
        #     self.logger.info("express diff")
        # if event['data'] == 'snp_rna':
        #     self.move2outputdir(obj.output_dir, 'snp_rna')
        #     self.logger.info("snp_rna文件移动完成")
        # if event['data'] == 'network_analysis':
        #     self.move2outputdir(obj.output_dir, 'network_analysis')
        #     self.logger.info("network_analysis文件移动完成")
        # if event['data'] == 'annotation':
        #     self.move2outputdir(obj.output_dir, 'annotation')
        #     self.logger.info("annotation文件移动完成")
        # if event['data'] == 'new_annotation':
        #     self.move2outputdir(obj.output_dir, 'new_annotation')
        #     self.logger.info("新转录本与新基因annotation文件移动完成")
        # if event['data'] == 'altersplicing':
        #     self.move2outputdir(obj.output_dir, 'altersplicing')
        #     self.logger.info("altersplicing文件移动完成")
        # if event["data"] == "new_keggblast":
        #     self.move2outputdir(obj.output_dir, 'new_keggblast')
        #     self.logger.info("new_keggblast文件移动完成")
        # if event["data"] == "new_stringblast":
        #     self.move2outputdir(obj.output_dir, 'new_stringblast')
        #     self.logger.info("new_stringblast文件移动完成")
        # if event["data"] == "new_nrblast":
        #     self.move2outputdir(obj.output_dir, 'new_nrblast')
        #     self.logger.info("new_nrblast文件移动完成")
        # if event["data"] == "keggblast":
        #     self.move2outputdir(obj.output_dir, 'keggblast')
        #     self.logger.info("keggblast文件移动完成")
        # if event["data"] == "stringblast":
        #     self.move2outputdir(obj.output_dir, 'stringblast')
        #     self.logger.info("stringblast文件移动完成")
        # if event["data"] == "nrblast":
        #     self.move2outputdir(obj.output_dir, 'nrblast')
        #     self.logger.info("nrblast文件移动完成")
        # if event["data"] == "map_gene":
        #     self.move2outputdir(obj.output_dir, 'map_gene')
        #     self.logger.info("map_gene文件移动完成")
        # if event["data"] == "swsissprot":
        #     self.move2outputdir(obj.output_dir, 'new_swissprotblast')
        #     self.logger.info("swissprot文件移动完成")
        # if event["data"] == "pfam":
        #     self.move2outputdir(obj.output_dir, 'pfam')
        #     self.logger.info("pfam文件移动完成")

    def set_output_all(self):
        self.logger.info("开始导入结果文件！")
        self.move2outputdir(self.qc.output_dir, 'QC_stat')
        self.move2outputdir(self.qc_stat_before.output_dir, 'QC_stat/before_qc')
        self.move2outputdir(self.qc_stat_after.output_dir, 'QC_stat/after_qc')
        self.move2outputdir(self.mapping.output_dir, 'mapping')
        self.move2outputdir(self.map_qc.output_dir, 'map_qc')
        self.move2outputdir(self.assembly.output_dir, 'assembly')
        self.move2outputdir(self.exp.output_dir, 'express')
        self.move2outputdir(self.exp_fc.output_dir, 'express_fc_all')
        self.move2outputdir(self.exp_diff_gene.output_dir, 'express_diff_gene')
        self.move2outputdir(self.exp_diff_trans.output_dir, 'express_diff_trans')
        self.move2outputdir(self.snp_rna.output_dir, 'snp_rna')
        self.move2outputdir(self.network_trans.output_dir, 'network_analysis')
        self.move2outputdir(self.annotation.output_dir, 'annotation')
        self.move2outputdir(self.new_annotation.output_dir, 'new_annotation')
        self.move2outputdir(self.new_blast_kegg.output_dir, 'new_keggblast')
        self.move2outputdir(self.new_blast_string.output_dir, 'new_stringblast')
        self.move2outputdir(self.new_blast_nr.output_dir, 'new_nrblast')
        # self.move2outputdir(self.blast_kegg.output_dir, 'keggblast')
        # self.move2outputdir(self.blast_string.output_dir, 'stringblast')
        # self.move2outputdir(self.blast_nr.output_dir, 'nrblast')
        self.move2outputdir(self.new_blast_swissprot.output_dir, 'new_swissprotblast')
        self.move2outputdir(self.pfam.output_dir, 'pfam')
        if self.as_on:
            self.move2outputdir(self.altersplicing.output_dir, 'altersplicing')
        self.logger.info("结果文件导入完成！")

    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_seq_abs)
        if self.option("blast_method") == "diamond":
            if self.anno_path == "":
                self.seq_abs.on('end', self.run_align, "diamond")
            else:
                self.seq_abs.on('end', self.run_annotation)
            self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "diamond")
        else:
            if self.anno_path == "":
                self.seq_abs.on('end', self.run_align, "blast")
            else:
                self.seq_abs.on('end', self.run_annotation)
            self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "blast")
        self.on_rely([self.new_annotation, self.annotation], self.run_merge_annot)
        self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        self.filecheck.on("end", self.run_gs)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        self.qc.on("end", self.run_star_mapping)
        self.map_gene.on("end", self.run_map_assess_gene)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp_rsem_default)
        self.assembly.on("end", self.run_exp_fc)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        if self.taxon_id != "":
            self.exp.on("end", self.run_network_trans)
            self.final_tools.append(self.network_trans)
        self.on_rely(self.final_tools, self.run_api_and_set_output)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def end(self):
        super(RefrnaWorkflow, self).end()

    def test_mus(self):
        self.logger.info("{}".format(self.option("ref_genome_custom").prop["path"]))
        self.filecheck.option("gtf", "/mnt/ilustre/users/sanger-test/workspace/20170622/Refrna_tsanger_8326/FilecheckRef/Mus_musculus.GRCm38.87.gff3.gtf")
        self.qc.option("sickle_dir", "/mnt/ilustre/users/sanger-test/workspace/20170622/Refrna_tsanger_8326/HiseqQc/output/sickle_dir")
        self.filecheck.option("bed", "/mnt/ilustre/users/sanger-test/workspace/20170622/Refrna_tsanger_8326/FilecheckRef/Mus_musculus.GRCm38.87.gff3.gtf.bed")
        self.mapping.option("bam_output", "/mnt/ilustre/users/sanger-test/workspace/20170626/Refrna_mouse_4/RnaseqMapping/output/bam")
        self.exp.option("network_diff_list", "/mnt/ilustre/users/sanger-test/workspace/20170629/Refrna_demo1/Express/output/diff/trans_diff/network_X1_vs_Z1")
        self.assembly.option("new_transcripts_gtf", "/mnt/ilustre/users/sanger-test/workspace/20170627/Refrna_mouse_6/RefrnaAssemble/output/NewTranscripts/new_transcripts.gtf")
        self.assembly.option("new_gene_gtf", "/mnt/ilustre/users/sanger-test/workspace/20170627/Refrna_mouse_6/RefrnaAssemble/output/NewTranscripts/new_genes.gtf")
        # self.qc.on("end", self.run_qc_stat, "after")
        # self.qc.on('end', self.run_mapping)
        # self.qc.on("end", self.run_star_mapping)
        # self.qc.on("end", self.run_seq_abs)
        # self.seq_abs.on("end", self.run_test_annotation)
        # self.mapping.on('end', self.run_assembly)
        # self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        # if self.taxon_id != "":
        #     self.exp.on("end", self.run_network_trans)
        #     self.final_tools.append(self.network_trans)
        # self.on_rely(self.final_tools, self.run_api_and_set_output)
        # self.assembly.on("end", self.run_exp_rsem_default)
        self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_merge_annot)
        self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        self.start_listener()
        self.fire("start")
        self.qc.start_listener()
        self.qc.fire("end")
        self.mapping.start_listener()
        self.mapping.fire("end")
        self.assembly.start_listener()
        self.assembly.fire("end")
        self.exp.start_listener()
        self.exp.fire("end")
        self.rpc_server.run()
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_AFTER_END = False
        # task_info = self.api.api('task_info.ref')
        # task_info.add_task_info()
        # self.group_id = "5955f5e1edcb253a204f8988"
        # self.control_id = "5955f821f2e3f7fddea08f6e"
        # self.group_category = ["X1", "B1", "Z1"]
        # self.group_detail = [
        #     {'5955f5deedcb253a204f7ef5': 'X1_3',
        #      '5955f5deedcb253a204f7ef4': 'X1_2',
        #      '5955f5deedcb253a204f7ef3': 'X1_1'},
        #     {'5955f5deedcb253a204f7efb': 'B1_1',
        #      '5955f5deedcb253a204f7ef9': 'B1_2',
        #      '5955f5deedcb253a204f7efa': 'B1_3'},
        #     {'5955f5deedcb253a204f7ef7': 'Z1_3',
        #      '5955f5deedcb253a204f7ef6': 'Z1_2',
        #      '5955f5deedcb253a204f7ef8': 'Z1_1'}
        # ]
        # self.export_qc()
        # self.export_genome_info()
        # self.export_annotation()
        # self.export_assembly()
        # self.export_snp()
        # self.export_map_assess()
        # self.export_exp_rsem_default()
        # self.exp_alter.mergersem = self.exp_alter.add_tool("rna.merge_rsem")
        # self.exp.mergersem = self.exp.add_tool("rna.merge_rsem")
        # self.export_gene_set()
        # self.export_diff_gene()
        # self.export_diff_trans()
        # self.export_ref_diff_gene()
        # self.export_ref_diff_trans()
        # # self.export_gene_detail()
        # self.export_ref_gene_set()
        # self.export_cor()
        # self.export_pca()
        # self.export_cluster_gene()
        # self.export_cluster_trans()
        # self.export_go_regulate()
        # self.export_kegg_regulate()
        # self.export_go_enrich()
        # self.export_kegg_enrich()
        # self.export_cog_class()
        # if self.taxon_id != "":
        #     with open(self.exp.option("network_diff_list").prop["path"], "r") as ft:
        #         ft.readline()
        #         content = ft.read()
        #         if content:
        #             self.export_ppi()
        # self.export_as()

    def run_test_annotation(self):
        pass

    def test_ore(self):
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_AFTER_END = False
        self.filecheck.option("gtf", "/mnt/ilustre/users/sanger-dev/workspace/20170606/Refrna_tsg_7901/FilecheckRef/Oreochromis_niloticus.Orenil1.0.87.gff3.gtf")
        self.exp_diff_trans.option("all_list", "/mnt/ilustre/users/sanger-dev/workspace/20170615/Refrna_ore_test_for_api/Express/output/diff/trans_diff/diff_list")
        self.exp_diff_gene.option("all_list", "/mnt/ilustre/users/sanger-dev/workspace/20170615/Refrna_ore_test_for_api/Express/output/diff/genes_diff/diff_list")
        # self.run_api_and_set_output()
        self.group_id = "59473300a4e1af65bfaf3816"
        self.control_id = "59473300a4e1af65bfaf3817"
        self.group_category = ["A", "B"]
        self.group_detail = [
        {
            "59473300a4e1af65bfaf2d76" : "CL1",
            "59473300a4e1af65bfaf2d77" : "CL2",
            "59473300a4e1af65bfaf2d74" : "HFL3",
            "59473300a4e1af65bfaf2d75" : "CL5"
        },
        {
            "59473300a4e1af65bfaf2d6f" : "HGL4",
            "59473300a4e1af65bfaf2d72" : "HFL6",
            "59473300a4e1af65bfaf2d73" : "HFL4",
            "59473300a4e1af65bfaf2d70" : "HGL3",
            "59473300a4e1af65bfaf2d71" : "HGL1"
        }
        ]
        self.export_genome_info()
        #self.export_as()
        self.end()

    def run_api_and_set_output(self):
        self.set_output_all()
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_AFTER_END = False
        task_info = self.api.api('task_info.ref')
        task_info.add_task_info()
        self.export_qc()
        self.export_genome_info()
        self.export_annotation()
        self.export_assembly()
        self.export_snp()
        self.export_map_assess()
        self.export_exp_rsem_default()
        self.exp_alter.mergersem = self.exp_alter.add_tool("rna.merge_rsem")
        self.exp.mergersem = self.exp.add_tool("rna.merge_rsem")
        self.export_gene_set()
        self.export_diff_gene()
        self.export_diff_trans()
        self.export_ref_diff_gene()
        self.export_ref_diff_trans()
        # self.export_gene_detail()
        self.export_ref_gene_set()
        self.export_cor()
        self.export_pca()
        self.export_cluster_gene()
        self.export_cluster_trans()
        self.export_go_regulate()
        self.export_kegg_regulate()
        self.export_go_enrich()
        self.export_kegg_enrich()
        self.export_cog_class()
        if self.taxon_id != "":
            with open(self.exp.option("network_diff_list").prop["path"], "r") as ft:
                ft.readline()
                content = ft.read()
                if content:
                    self.export_ppi()
        self.export_as()
        self.end()

    def export_genome_info(self):
        self.api_gi = self.api.genome_info
        species_name = self.option("ref_genome")
        self.api_gi.add_genome_info(species_name, output_dir=self.gs.output_dir, major=True)

    def export_qc(self):
        self.api_qc = self.api.ref_rna_qc
        qc_stat = self.qc_stat_before.output_dir
        fq_type = self.option("fq_type").lower()
        self.api_qc.add_samples_info(qc_stat, fq_type=fq_type, about_qc="before")
        quality_stat_after = self.qc_stat_after.output_dir + "/qualityStat"
        quality_stat_before = self.qc_stat_before.output_dir + "/qualityStat"  # 将qc前导表加于该处
        self.api_qc.add_gragh_info(quality_stat_before, "before")
        qc_stat = self.qc_stat_after.output_dir
        self.api_qc.add_samples_info(qc_stat, fq_type=fq_type, about_qc="after")
        self.api_qc.add_gragh_info(quality_stat_after, "after")
        quality_stat_before = self.qc_stat_before.output_dir + "/qualityStat"  # 将qc前导表加于该处
        # self.api_qc.add_gragh_info(quality_stat_before, "before")
        self.group_id, self.group_detail, self.group_category = self.api_qc.add_specimen_group(self.option("group_table").prop["path"])
        self.logger.info(self.group_detail)
        self.control_id, self.compare_detail = self.api_qc.add_control_group(self.option("control_file").prop["path"], self.group_id)
        self.api_qc.add_bam_path(self.mapping.output_dir)

    def export_assembly(self):
        self.api_assembly = self.api.api("ref_rna.ref_assembly")
        if self.option("assemble_method") == "cufflinks":
            all_gtf_path = self.assembly.output_dir + "/Cufflinks"
            merged_path = self.assembly.output_dir + "/Cuffmerge"
        else:
            all_gtf_path = self.assembly.output_dir + "/Stringtie"
            merged_path = self.assembly.output_dir + "/StringtieMerge"
        self.api_assembly.add_assembly_result(all_gtf_path=all_gtf_path, merged_path=merged_path, Statistics_path=self.assembly.output_dir + "/Statistics")


    def export_map_assess(self):
        self.api_map = self.api.ref_rna_qc
        # stat_file = self.map_qc.output_dir + "/bam_stat.xls"
        # self.api_map.add_mapping_stat(stat_file, "genome")
        # stat_file = self.map_qc_gene.output_dir + "/bam_stat.xls"
        # self.api_map.add_mapping_stat(stat_file, "gene")
        stat_dir = self.mapping.output_dir + "/stat"
        if self.option("seq_method") == "Topaht":
            self.api_map.add_tophat_mapping_stat(stat_dir)
        else:
            self.api_map.add_hisat_mapping_stat(stat_dir)
        file_path = self.map_qc.output_dir + "/satur"
        self.api_map.add_rpkm_table(file_path)
        coverage = self.map_qc.output_dir + "/coverage"
        self.api_map.add_coverage_table(coverage)
        distribution = self.map_qc.output_dir + "/distribution"
        self.api_map.add_distribution_table(distribution)
        chrom_distribution = self.map_qc.output_dir + "/chr_stat"
        self.api_map.add_chorm_distribution_table(chrom_distribution)

    def test_export_map_assess(self):
        self.api_map = self.api.ref_rna_qc
        chrom_distribution = self.map_qc.output_dir + "/chr_stat"
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
        if self.option("is_duplicate") == False:
            with open(rsem_dir + "/genes.counts.matrix") as f:
                samples = f.readline().strip().split("\t")
        else:
            group_spname = self.option("group_table").get_group_spname()
            lst = []
            for key in group_spname.keys():
                lst.extend(group_spname[key])
            samples = lst
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
        self.trans_gs_id_name = dict()
        self.gene_gs_id_name = dict()
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name, compare_name=compare_name,
                                                           express_method="rsem", type="transcript", up_down='up_down', major=True)
                    down_id= self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name,
                                                           compare_name=compare_name, express_method="rsem",
                                                           type="transcript", up_down='down', major=True)
                    up_id= self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                         group_id=group_id, name=name, compare_name=compare_name,
                                                         express_method="rsem", type="transcript", up_down='up', major=True)
                    if up_down:
                        self.transet_id.append(up_down)
                        self.trans_gs_id_name[str(up_down)] = name + "_vs_" + compare_name
                        self.up_down_trans_id = str(down_id) + "," + str(up_id)
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
                    up_down = self.api_geneset.add_geneset(diff_stat_path = path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene",up_down='up_down', major=True)
                    down_id = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene", up_down='down', major=True)
                    up_id = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id, name=name,
                                                         compare_name=compare_name, express_method="rsem", type="gene",
                                                         up_down='up', major=True)
                    self.up_down_gene_id = str(down_id) + "," + str(up_id)
                    self.geneset_id.append(up_down)
                    self.gene_gs_id_name[str(up_down)] = name + "_vs_" + compare_name
                else:
                    self.logger.info("基因name和compare_name匹配错误")

    def export_ref_gene_set(self):
        self.api_geneset = self.api.refrna_express
        group_id = self.group_id
        path = self.exp.output_dir + "/ref_diff/trans_ref_diff/diff_stat_dir"
        self.transet_id = list()
        self.trans_gs_id_name = dict()
        self.gene_gs_id_name = dict()
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name, compare_name=compare_name,
                                                           express_method="rsem", type="transcript", up_down='up_down', major=True)
                    down_id= self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name,
                                                           compare_name=compare_name, express_method="rsem",
                                                           type="transcript", up_down='down', major=True)
                    up_id= self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                         group_id=group_id, name=name, compare_name=compare_name,
                                                         express_method="rsem", type="transcript", up_down='up', major=True)
                    if up_down:
                        self.transet_id.append(up_down)
                        self.trans_gs_id_name[str(up_down)] = name + "_vs_" + compare_name
                        self.up_down_trans_id = str(down_id) + "," + str(up_id)
                else:
                    self.logger.info("转录本name和compare_name匹配错误")
        path = self.exp.output_dir + "/ref_diff/genes_ref_diff/diff_stat_dir"
        self.geneset_id = list()
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down = self.api_geneset.add_geneset(diff_stat_path = path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene",up_down='up_down', major=True)
                    down_id = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id,
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene", up_down='down', major=True)
                    up_id = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files, group_id=group_id, name=name,
                                                         compare_name=compare_name, express_method="rsem", type="gene",
                                                         up_down='up', major=True)
                    self.up_down_gene_id = str(down_id) + "," + str(up_id)
                    self.geneset_id.append(up_down)
                    self.gene_gs_id_name[str(up_down)] = name + "_vs_" + compare_name
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
        self.logger.info(params['group_detail'])  # 打印group_detail
        params['express_id'] = str(self.express_id)
        params['fc'] = 2
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_fdr_ci")
        params['diff_method'] = self.option("diff_method")
        params["type"] = "trans"
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,ref_all='all',value_type=self.option("exp_way"),
                                                        class_code=class_code, diff_exp_dir=path + "/diff_stat_dir",
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        is_duplicate=self.option("is_duplicate"),
                                                        query_type="transcript", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, count_path = merge_path,ref_all='all',query_type='transcript',
                                            class_code=class_code,workflow=True)

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
        params["type"] = "gene"
        compare_column_specimen = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            value2 = self.group_detail[i].values()
            params['group_detail'][key] = value
            compare_column_specimen[key] = value2
        params['express_id'] = str(self.express_id)
        params['fc'] = 2
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_fdr_ci")
        params['diff_method'] = self.option("diff_method")
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,ref_all='all',value_type=self.option("exp_way"),
                                                        class_code=class_code, diff_exp_dir=path + "/diff_stat_dir",
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        is_duplicate=self.option("is_duplicate"),
                                                        query_type="gene", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, count_path = merge_path, ref_all='all',query_type='gene',
                                            class_code=class_code,workflow=True)

    def export_ref_diff_trans(self):
        path = self.exp.output_dir + "/ref_diff/trans_ref_diff"
        exp_path = self.exp.output_dir + "/rsem"
        with open(exp_path + "/transcripts.counts.matrix", 'r+') as f1:
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
        self.logger.info(params['group_detail'])  # 打印group_detail
        params['express_id'] = str(self.express_id)
        params['fc'] = 2
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_fdr_ci")
        params['diff_method'] = self.option("diff_method")
        params["type"] = "trans"
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,ref_all='ref',value_type=self.option("exp_way"),
                                                        class_code=class_code, diff_exp_dir=path + "/diff_stat_dir",
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        is_duplicate=self.option("is_duplicate"),
                                                        query_type="transcript", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, count_path = merge_path,ref_all='ref',query_type='transcript',
                                            class_code=class_code,workflow=True)

    def export_ref_diff_gene(self):
        path = self.exp.output_dir + "/ref_diff/genes_ref_diff"
        exp_path = self.exp.output_dir + "/rsem"
        with open(exp_path + "/genes.counts.matrix", 'r+') as f1:
            sample = f1.readline().strip().split("\t")
        compare_column = self.compare_detail
        params = {}
        merge_path = path + "/merge.xls"
        params['group_id'] = str(self.group_id)
        params['control_id'] = str(self.control_id)
        params['group_detail'] = dict()
        params["type"] = "gene"
        compare_column_specimen = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            value2 = self.group_detail[i].values()
            params['group_detail'][key] = value
            compare_column_specimen[key] = value2
        params['express_id'] = str(self.express_id)
        params['fc'] = 2
        params['pvalue_padjust'] = 'padjust'  # 默认为padjust
        params['pvalue'] = self.option("diff_fdr_ci")
        params['diff_method'] = self.option("diff_method")
        class_code = self.exp.mergersem.work_dir + "/class_code"
        diff_express_id = self.api_exp.add_express_diff(params=params, samples=sample, compare_column=compare_column,
                                                        compare_column_specimen=compare_column_specimen,ref_all='ref',value_type=self.option("exp_way"),
                                                        class_code=class_code, diff_exp_dir=path + "/diff_stat_dir",
                                                        express_id=self.express_id,
                                                        express_method="rsem",
                                                        is_duplicate=self.option("is_duplicate"),
                                                        query_type="gene", major=True,
                                                        group_id=params["group_id"], workflow=True)
        self.api_exp.add_diff_summary_detail(diff_express_id, count_path = merge_path, ref_all='ref',query_type='gene',
                                            class_code=class_code,workflow=True)



    def export_cor(self):
        self.api_cor = self.api.refrna_corr_express
        correlation = self.exp.output_dir + "/correlation/genes_correlation"
        group_id = str(self.group_id)
        group_detail = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            group_detail[key] = value
        self.api_cor.add_correlation_table(correlation=correlation, group_id=group_id, group_detail=group_detail,
                                           express_id=self.express_id, detail=True, seq_type="gene")

    def export_pca(self):
        self.api_pca = self.api.refrna_corr_express
        pca_path = self.exp.output_dir + "/pca/genes_pca"
        group_detail = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            group_detail[key] = value
        self.api_pca.add_pca_table(pca_path, group_id=str(self.group_id), group_detail=group_detail,
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
        self.api_as = self.api.api("ref_rna.refrna_splicing_rmats")
        if self.option("strand_specific"):
            lib_type = "fr-firststrand",
            am = "fr_firststrand",
        else:
            lib_type = "fr-unstranded"
            am = "fr_unstranded"
        if self.option("fq_type") == "PE":
            seq_type = "paired"
        else:
            seq_type = "single"
        params = {
            "ana_mode": "P",
            "analysis_mode": am,
            "novel": 1,
            "as_diff": 0.001,
            "group_id": str(self.group_id),
            "lib_type": lib_type,
            "read_len": 150,
            "ref_gtf": self.filecheck.option("gtf").prop["path"],
            "seq_type": seq_type,
            "control_file": str(self.control_id),
            "gname": "group1",
            "submit_location": "splicingrmats",
            "task_type": ""
        }
        if len(self.group_category) > 2:
            self.logger.info("出现两组以上对照组，取前两组进行导表")
        params['group_detail'] = dict()
        group = dict()
        for i in range(len(self.group_category)):
            if i == 2:
                break
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            params['group_detail'][key] = value
            if i == 0:
                group[key] = "s1"
            else:
                group[key] = "s2"
        outpath = self.altersplicing.output_dir
        self.logger.info(params)
        if self.as_on:
            self.api_as.add_sg_splicing_rmats(params=params, major=True, group=group, ref_gtf=self.filecheck.option("gtf").prop["path"], name=None, outpath=outpath)
        else:
            self.api_as.add_sg_splicing_rmats(params=params, major=False, group=group, ref_gtf=self.filecheck.option("gtf").prop["path"], name=None, outpath=outpath)

    def export_ppi(self):
        api_ppinetwork = self.api.ppinetwork
        if self.transet_id == []:
            return
        geneset_id = None
        file_name = self.network_trans.option("diff_exp_gene").prop["path"]
        name = os.path.split(os.path.basename(file_name))[0]
        if name.startswith("network_"):
            name = name.split("network_")[1]
            for key in self.trans_gs_id_name.keys():
                if self.trans_gs_id_name[key] == name:
                    geneset_id = key
                    break
        if not geneset_id:
            self.logger.info("没找到对应的基因集")
            return
        self.ppi_id = api_ppinetwork.add_ppi_main_id(str(geneset_id), self.option("combine_score"), "trans", self.taxon_id)
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
        api_ppinetwork.add_network_cluster_degree(file1_path=network_node_degree_path,file2_path=network_clustering_path,file3_path=all_nodes_path,table_id=self.ppi_id)  # 节点的聚类与degree，画折线图
        api_ppinetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.ppi_id)  # 中心信息
        api_ppinetwork.add_degree_distribution(file_path=degree_distribution_path, table_id=self.ppi_id)  # 度分布
        # self.ppi_id = api_ppinetwork.add_ppi_main_id(str(self.geneset_id[0]), self.option("combine_score"), "gene", self.taxon_id)
        # self.ppi_id = str(self.ppi_id)
        # all_nodes_path = self.network_gene.output_dir + '/ppinetwork_predict/all_nodes.txt'   # 画图节点属性文件
        # interaction_path = self.network_gene.output_dir + '/ppinetwork_predict/interaction.txt'  # 画图的边文件
        # network_stats_path = self.network_gene.output_dir + '/ppinetwork_predict/network_stats.txt'  # 网络全局属性统计
        # network_centrality_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_centrality.txt'
        # network_clustering_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_clustering.txt'
        # network_transitivity_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_transitivity.txt'
        # degree_distribution_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_degree_distribution.txt'
        # network_node_degree_path = self.network_gene.output_dir + '/ppinetwork_topology/protein_interaction_network_node_degree.txt'
        # api_ppinetwork.add_node_table(file_path=all_nodes_path, table_id=self.ppi_id)   # 节点的属性文件（画网络图用）
        # api_ppinetwork.add_edge_table(file_path=interaction_path, table_id=self.ppi_id)  # 边信息
        # api_ppinetwork.add_network_attributes(file1_path=network_transitivity_path, file2_path=network_stats_path, table_id=self.ppi_id)  # 网络全局属性
        # api_ppinetwork.add_network_cluster_degree(file1_path=network_node_degree_path,file2_path=network_clustering_path, table_id=self.ppi_id)  # 节点的聚类与degree，画折线图
        # api_ppinetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.ppi_id)  # 中心信息
        # api_ppinetwork.add_degree_distribution(file_path=degree_distribution_path, table_id=self.ppi_id)  # 度分布

    def export_snp(self):
        self.api_snp = self.api.api("ref_rna.ref_snp")
        snp_anno = self.snp_rna.output_dir
        self.api_snp.add_snp_main(snp_anno)

    def export_cluster_trans(self):
        api_cluster = self.api.denovo_cluster  # #不确定,增加一个database
        my_param = dict()
        my_param['submit_location']="geneset_cluster_trans"
        my_param['type']= "transcript"
        # my_param['distance_method']=data.distance_method # 距离算法
        my_param['method']= "hclust"
        my_param['log']= 10
        my_param['level']= self.option("exp_way")
        my_param['sub_num']= 5
        my_param['group_id']= str(self.group_id)
        tmp = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            tmp[key] = value
        my_param['group_detail'] = self.group_detail_sort(tmp)
        my_param['express_method']= "rsem"
        my_param['geneset_id']= str(self.geneset_id[0])
        my_param['genes_distance_method'] = "complete"
        my_param['samples_distance_method'] = "complete"
        self.logger.info("开始导mongo表！")
        hclust_path = os.path.join(self.exp_diff_gene.output_dir, "cluster/hclust")
        sub_clusters = os.listdir(hclust_path)
        with open(self.exp_diff_gene.cluster.work_dir + '/hc_gene_order') as r:
            genes = [i.strip('\n') for i in r.readlines()]
        with open(self.exp_diff_gene.cluster.work_dir + '/hc_sample_order') as r:
            specimen = [i.strip('\n') for i in r.readlines()]
        sample_tree = self.exp_diff_gene.output_dir + "/cluster/hclust/samples_tree.txt"
        gene_tree = self.exp_diff_gene.output_dir + "/cluster/hclust/genes_tree.txt"
        id = api_cluster.add_cluster(my_param, express_id=self.express_id, sample_tree=sample_tree, gene_tree=gene_tree, samples=specimen, genes=genes, project='ref')
        for sub_cluster in sub_clusters:
            if re.match('subcluster', sub_cluster):  # 找到子聚类的文件进行迭代
                sub = sub_cluster.split("_")[1]
                sub_path = os.path.join(hclust_path, sub_cluster)
                api_cluster.add_cluster_detail(cluster_id=id, sub=sub, sub_path=sub_path,project='ref')
                self.logger.info("开始导子聚类函数！")
            if re.search('samples_tree', sub_cluster):  # 找到sample_tree
                self.logger.info("sample_tree产生")

            if re.search('genes_tree', sub_cluster):  # 找到gene_tree
                self.logger.info("gene_tree产生")

    def export_cluster_gene(self):
        api_cluster = self.api.denovo_cluster  # #不确定,增加一个database
        my_param = dict()
        my_param['submit_location']="geneset_cluster_gene"
        my_param['type'] = "gene"
        # my_param['distance_method']=data.distance_method # 距离算法
        my_param['method'] = "hclust"
        my_param['log'] = 10
        my_param['level'] = self.option("exp_way")
        my_param['sub_num'] = 5
        my_param['group_id'] = str(self.group_id)
        tmp = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            tmp[key] = value
        my_param['group_detail'] = self.group_detail_sort(tmp)
        my_param['express_method'] = "rsem"
        my_param['geneset_id'] = str(self.geneset_id[0])
        my_param['genes_distance_method'] = "complete"
        my_param['samples_distance_method'] = "complete"
        self.logger.info("开始导mongo表！")
        hclust_path = os.path.join(self.exp_diff_gene.output_dir, "cluster/hclust")
        sub_clusters = os.listdir(hclust_path)
        with open(self.exp_diff_gene.cluster.work_dir + '/hc_gene_order') as r:
            genes = [i.strip('\n') for i in r.readlines()]
        with open(self.exp_diff_gene.cluster.work_dir + '/hc_sample_order') as r:
            specimen = [i.strip('\n') for i in r.readlines()]
        sample_tree = self.exp_diff_gene.output_dir + "/cluster/hclust/samples_tree.txt"
        gene_tree = self.exp_diff_gene.output_dir + "/cluster/hclust/genes_tree.txt"
        id = api_cluster.add_cluster(my_param, express_id=self.express_id, sample_tree=sample_tree, gene_tree=gene_tree, samples=specimen, genes=genes, project='ref')
        for sub_cluster in sub_clusters:
            if re.match('subcluster', sub_cluster):  # 找到子聚类的文件进行迭代
                sub = sub_cluster.split("_")[1]
                sub_path = os.path.join(hclust_path, sub_cluster)
                api_cluster.add_cluster_detail(cluster_id=id, sub=sub, sub_path=sub_path,project='ref')
                self.logger.info("开始导子聚类函数！")
            if re.search('samples_tree', sub_cluster):  # 找到sample_tree
                self.logger.info("sample_tree产生")
            if re.search('genes_tree', sub_cluster):  # 找到gene_tree
                self.logger.info("gene_tree产生")

    @staticmethod
    def group_detail_sort(detail):
        if isinstance(detail, dict):
            table_dict = detail
        else:
            table_dict = json.loads(detail)
        if not isinstance(table_dict, dict):
            raise Exception("传入的table_dict不是一个字典")
        for keys in table_dict.keys():
            table_dict[keys] = sorted(table_dict[keys])
        sort_key = OrderedDict(sorted(table_dict.items(), key=lambda t: t[0]))
        table_dict = sort_key
        return table_dict

    def export_go_regulate(self):
        self.api_regulate = self.api.ref_rna_geneset
        trans_dir = self.exp_diff_trans.output_dir
        gene_dir = self.exp_diff_gene.output_dir
        trans_go_regulate_dir = trans_dir + "/go_regulate"
        gene_go_regulate_dir = gene_dir + "/go_regulate"
        for trans_id in self.trans_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(self.up_down_trans_id)
            params["anno_type"] = "go"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "transcript"
            inserted_id = self.api_regulate.add_main_table(collection_name = "sg_geneset_go_class", params =params, name = "go_regulate_main_table")
            for dir in os.listdir(trans_go_regulate_dir):
                if self.trans_gs_id_name[trans_id] in dir:
                    dir_path = os.path.join(trans_go_regulate_dir, dir)
                    self.api_regulate.add_go_regulate_detail(go_regulate_dir=dir_path + "/GO_regulate.xls", go_regulate_id=str(inserted_id))
        for gene_id in self.gene_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(self.up_down_gene_id)
            params["anno_type"] = "go"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "gene"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_go_class", params=params, name="go_regulate_main_table")
            for dir in os.listdir(gene_go_regulate_dir):
                if self.gene_gs_id_name[gene_id] in dir:
                    dir_path = os.path.join(trans_go_regulate_dir, dir)
                    self.api_regulate.add_go_regulate_detail(go_regulate_dir=dir_path + "/GO_regulate.xls", go_regulate_id=str(inserted_id))

    def export_go_enrich(self):
        self.api_regulate = self.api.ref_rna_geneset
        trans_dir = self.exp_diff_trans.output_dir
        gene_dir = self.exp_diff_gene.output_dir
        trans_go_regulate_dir = trans_dir + "/go_rich"
        gene_go_regulate_dir = gene_dir + "/go_rich"
        for trans_id in self.trans_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(trans_id)
            params["method"] = "fdr"
            params["anno_type"] = "go"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "transcript"
            inserted_id = self.api_regulate.add_main_table(collection_name = "sg_geneset_go_enrich", params =params, name = "go_enrich_main_table")
            for dir in os.listdir(trans_go_regulate_dir):
                if self.trans_gs_id_name[trans_id] in dir:
                    dir_path = os.path.join(trans_go_regulate_dir, dir)
                    self.api_regulate.add_go_enrich_detail(go_enrich_dir=dir_path + "/go_enrich_{}.xls".format(self.trans_gs_id_name[trans_id]), go_enrich_id=str(inserted_id))
        for gene_id in self.gene_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(gene_id)
            params["anno_type"] = "go"
            params["method"] = "fdr"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "gene"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_go_enrich", params=params, name="go_enrich_main_table")
            for dir in os.listdir(gene_go_regulate_dir):
                if self.gene_gs_id_name[str(gene_id)] in dir:
                    dir_path = os.path.join(trans_go_regulate_dir, dir)
                    self.api_regulate.add_go_enrich_detail(go_enrich_dir=dir_path + "/go_enrich_{}.xls".format(self.gene_gs_id_name[gene_id]), go_enrich_id=str(inserted_id))

    def export_kegg_regulate(self):
        self.api_regulate = self.api.ref_rna_geneset
        trans_dir = self.exp_diff_trans.output_dir
        gene_dir = self.exp_diff_gene.output_dir
        trans_kegg_regulate_dir = trans_dir + "/kegg_regulate"
        gene_kegg_regulate_dir = gene_dir + "/kegg_regulate"
        for trans_id in self.trans_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(self.up_down_trans_id)
            params["anno_type"] = "kegg"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "transcript"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_kegg_class", params=params, name="kegg_class_main_table")
            self.logger.info(inserted_id)
            for dir in os.listdir(trans_kegg_regulate_dir):
                if self.trans_gs_id_name[str(trans_id)] in dir:
                    dir_path = os.path.join(trans_kegg_regulate_dir, dir)
                    self.api_regulate.add_kegg_regulate_detail(kegg_regulate_table=dir_path + "/kegg_regulate_stat.xls", regulate_id=str(inserted_id))
                    self.api_regulate.add_kegg_regulate_pathway(pathway_dir=dir_path + "/pathways", regulate_id=str(inserted_id))
        for gene_id in self.gene_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(self.up_down_gene_id)
            params["anno_type"] = "kegg"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "gene"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_kegg_class", params=params, name="kegg_class_main_table")
            for dir in os.listdir(gene_kegg_regulate_dir):
                if self.gene_gs_id_name[str(gene_id)] in dir:
                    dir_path = os.path.join(gene_kegg_regulate_dir, dir)
                    self.api_regulate.add_kegg_regulate_detail(kegg_regulate_table=dir_path + "/kegg_regulate_stat.xls", regulate_id=str(inserted_id))
                    self.api_regulate.add_kegg_regulate_pathway(pathway_dir=dir_path + "/pathways", regulate_id=str(inserted_id))

    def export_kegg_enrich(self):
        self.api_regulate = self.api.ref_rna_geneset
        trans_dir = self.exp_diff_trans.output_dir
        gene_dir = self.exp_diff_gene.output_dir
        trans_kegg_regulate_dir = trans_dir + "/kegg_rich"
        gene_kegg_regulate_dir = gene_dir + "/kegg_rich"
        for trans_id in self.trans_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(trans_id)
            params["anno_type"] = "kegg"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "transcript"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_kegg_enrich", params=params, name="kegg_enrich_main_table")
            for dir in os.listdir(trans_kegg_regulate_dir):
                if self.trans_gs_id_name[str(trans_id)] in dir:
                    for tool in self.exp_diff_trans.kegg_rich_tool:
                        list_path = tool.option('diff_list').prop["path"]
                        if dir in list_path:
                            geneset_list_path = list_path
                            break
                    geneset_list_path = "/mnt/ilustre/users/sanger-dev/workspace/20170615/Refrna_ore_test_for_api/Express/output/diff/trans_diff/diff_list_dir/A_vs_B"
                    dir_path = os.path.join(trans_kegg_regulate_dir, dir)
                    self.api_regulate.add_kegg_enrich_detail(kegg_enrich_table=dir_path + "/{}.kegg_enrichment.xls".format(
                        self.trans_gs_id_name[str(trans_id)]), enrich_id=str(inserted_id), geneset_list_path=geneset_list_path, all_list_path=self.exp_diff_trans.option("all_list").prop["path"])
        for gene_id in self.gene_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(gene_id)
            params["anno_type"] = "kegg"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "gene"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_kegg_enrich", params=params, name="kegg_enrich_main_table")
            for dir in os.listdir(gene_kegg_regulate_dir):
                if self.gene_gs_id_name[str(gene_id)] in dir:
                    for tool in self.exp_diff_gene.kegg_rich_tool:
                        list_path = tool.option('diff_list').prop["path"]
                        if dir in list_path:
                            geneset_list_path = list_path
                            break
                    geneset_list_path = "/mnt/ilustre/users/sanger-dev/workspace/20170615/Refrna_ore_test_for_api/Express/output/diff/genes_diff/diff_list_dir/A_vs_B"
                    dir_path = os.path.join(gene_kegg_regulate_dir, dir)
                    self.api_regulate.add_kegg_enrich_detail(kegg_enrich_table=dir_path + "/{}.kegg_enrichment.xls".format(
                        self.gene_gs_id_name[gene_id]), enrich_id=str(inserted_id), geneset_list_path=geneset_list_path, all_list_path=self.exp_diff_gene.option("all_list").prop["path"])

    def export_cog_class(self):
        self.api_regulate = self.api.ref_rna_geneset
        trans_dir = self.exp_diff_trans.output_dir
        gene_dir = self.exp_diff_gene.output_dir
        trans_cog_class_dir = trans_dir + "/cog_class"
        gene_cog_class_dir = gene_dir + "/cog_class"
        for trans_id in self.trans_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(trans_id)
            params["anno_type"] = "cog"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "transcript"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_cog_class", params=params, name="CogClass_transcript")
            for dir in os.listdir(trans_cog_class_dir):
                if self.trans_gs_id_name[trans_id] in dir:
                    dir_path = os.path.join(trans_cog_class_dir, dir)
                    self.api_regulate.add_geneset_cog_detail(geneset_cog_table=dir_path + "/cog_summary.xls", geneset_cog_id=inserted_id)
        for gene_id in self.gene_gs_id_name.keys():
            params = dict()
            params["geneset_id"] = str(gene_id)
            params["anno_type"] = "kegg"
            params["submit_location"] = "geneset_class"
            params["task_type"] = ""
            params["geneset_type"] = "gene"
            inserted_id = self.api_regulate.add_main_table(collection_name="sg_geneset_cog_class", params=params, name="CogClass_gene")
            for dir in os.listdir(gene_cog_class_dir):
                if self.gene_gs_id_name[str(gene_id)] in dir:
                    dir_path = os.path.join(gene_cog_class_dir, dir)
                    self.api_regulate.add_geneset_cog_detail(geneset_cog_table=dir_path + "/cog_summary.xls", geneset_cog_id=inserted_id)
