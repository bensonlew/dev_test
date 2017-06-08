# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""有参转录一键化工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
from mbio.files.sequence.fasta import FastaFile
import os
import json
import shutil


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

            {"name": "diff_method", "type": "string", "default": "edgeR"},
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
            # self.ref_genome = FastaFile()
            # self.ref_genome.set_path(self.json_dict[self.option("ref_genome")]["ref_genome"])
            # self.ref_genome.check()
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
        self.final_tools = [self.snp_rna, self.altersplicing, self.exp_diff_gene, self.exp_diff_trans,
                            self.exp_alter, self.exp_fc]
        self.genome_status = True
        self.step.add_steps("qcstat", "mapping", "assembly", "annotation", "exp", "map_stat",
                            "seq_abs", "network_analysis", "sample_analysis", "altersplicing")

        
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
        self.qc.on('start', self.set_step, {'start': self.step.qcstat})
        self.qc.on('end', self.set_step, {'end': self.step.qcstat})
        self.qc.run()
        
    def run_seq_abs(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
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
            "ref_genome_gtf": self.filecheck.option("gtf"),
            "taxonomy": self.option("kegg_database"),
            "nr_annot": False
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
        self.new_annotation.on('end', self.set_step, {'end': self.step.annotation})
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
            "merged_gtf": self.assembly.option("merged_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "ref_genome_custom": self.assembly.option("merged_fa"),
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
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "fc":self.option("fc"),
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
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "fc": self.option("fc"),
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
        with open(self.exp.output_dir + "/diff/trans_diff/network_diff_list", "r") as ft:
            ft.readline()
            content = ft.read()
        if not content:
            self.logger.info("无差异转录本，不进行网络分析")
            self.network_trans.start_listener()
            self.network_trans.fire("end")
        else:
            opts = {
                "diff_exp_gene": self.exp.output_dir + "/diff/trans_diff/network_diff_list",
                "species": int(self.taxon_id),
                "combine_score": self.option("combine_score")
            }
            self.network_trans.set_options(opts)
            self.network_trans.on("end", self.set_output, "network_analysis")
            self.network_trans.on('start', self.set_step, {'start': self.step.network_analysis})
            self.network_trans.on('end', self.set_step, {'end': self.step.network_analysis})
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
            self.network_gene.on('start', self.set_step, {'start': self.step.network_analysis})
            self.network_gene.on('end', self.set_step, {'end': self.step.network_analysis})
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
        self.altersplicing.on('start', self.set_step, {'start': self.step.altersplicing})
        self.altersplicing.on('end', self.set_step, {'end': self.step.altersplicing})
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
            self.logger.info("无差异基因，不进行差异分析")
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
            return True
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
        self.on_rely([self.qc, self.seq_abs], self.run_map_gene)
        self.map_gene.on("end", self.run_map_assess_gene)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp_rsem_default)
        self.assembly.on("end", self.run_exp_rsem_alter)
        self.assembly.on("end", self.run_exp_fc)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        if self.taxon_id != "":
            self.exp.on("end", self.run_network_trans)
            self.exp.on("end", self.run_network_gene)
            self.final_tools.append(self.network_gene)
            self.final_tools.append(self.network_trans)
        self.on_rely(self.final_tools, self.end)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def test_run(self):
        self.filecheck.option("gtf", "/mnt/ilustre/users/sanger-dev/workspace/20170603/Refrna_arab_test/FilecheckRef/Arabidopsis_thaliana.TAIR10.32.gff3.gtf")
        self.filecheck.option("bed", "/mnt/ilustre/users/sanger-dev/workspace/20170603/Refrna_arab_test/FilecheckRef/Arabidopsis_thaliana.TAIR10.32.gff3.gtf.bed")
        self.qc.option("sickle_dir", "/mnt/ilustre/users/sanger-dev/workspace/20170603/Refrna_arab_test/HiseqQc/output/sickle_dir")
        self.mapping.option("bam_output", "/mnt/ilustre/users/sanger-dev/workspace/20170604/Refrna_arab_test/RnaseqMapping/output")
        self.seq_abs.on('end', self.run_annotation)
        self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "diamond")
        self.on_rely([self.new_annotation, self.annotation], self.run_merge_annot)
        self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        # self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        # self.qc.on('end', self.run_mapping)
        self.qc.on("end", self.run_star_mapping)
        self.on_rely([self.qc, self.seq_abs], self.run_map_gene)
        self.map_gene.on("end", self.run_map_assess_gene)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp_rsem_default)
        self.assembly.on("end", self.run_exp_rsem_alter)
        self.assembly.on("end", self.run_exp_fc)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        if self.taxon_id != "":
            self.exp.on("end", self.run_network_trans)
            self.exp.on("end", self.run_network_gene)
            self.final_tools.append(self.network_gene)
            self.final_tools.append(self.network_trans)
        self.on_rely(self.final_tools, self.end)
        self.run_seq_abs()
        self.run_gs()
        # self.qc.start_listener()
        # self.qc.fire("end")
        self.start_listener()
        self.fire("start")
        self.qc.start_listener()
        self.qc.fire("end")
        self.mapping.start_listener()
        self.mapping.fire("end")
        self.rpc_server.run()

    def __check(self):
        super(RefrnaWorkflow, self).__check()

    def run_new_gene_abs_test(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome_gtf": self.assembly.output_dir + "/NewTranscripts/new_genes.gtf"
        }
        self.new_gene_abs.set_options(opts)
        self.new_gene_abs.run()

    def run_new_transcripts_abs_test(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome_gtf": self.assembly.output_dir + "/NewTranscripts/new_transcripts.gtf"
        }
        self.new_trans_abs.set_options(opts)
        self.new_trans_abs.run()

    def run_annotation_test(self):
        opts = {
            "gos_list_upload": self.para_anno.output_dir + "/go.list",
            "kos_list_upload": self.para_anno.output_dir + "/kegg.list",
            "blast_string_table": self.para_anno.output_dir + "/cog.list",
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

    def end(self):
        super(RefrnaWorkflow, self).end()
