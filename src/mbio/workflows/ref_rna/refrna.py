# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""有参转录一键化工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
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
            {"name": "workflow_type", "type":"string", "default": "transcriptome"},  # 转录组
            {"name": "genome_status", "type": "bool", "default": True},
            # 基因组结构完整程度，True表示基因组结构注释文件可以支持rna编辑与snp分析
            {"name": "assemble_or_not", "type": "bool", "default": True},
            {"name": "blast_method", "type" :"string", "default": "diamond"},
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
            {"name": "database", "type": "string", "default": 'go,nr,cog,kegg,swissprot'},
            # 全部五个注释

            {"name": "seq_method", "type": "string", "default": "Tophat"},  # 比对方法，Tophat or Hisat
            {"name": "map_assess_method", "type": "string", "default":
                "saturation,duplication,distribution,coverage"},
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
            {"name": "exp_analysis", "type": "string", "default": "cluster,kegg_rich,go_rich,cog_class"},
            # 差异表达富集方法,聚类分析, GO富集分析, KEGG富集分析, cog统计分析

            {"name": "human_or_not", "type": "bool", "default": True},  # 是否为人类基因组
            {"name": "gene_structure_analysis", "type": "string", "default": "alter-splicing,SNP"},
            # 基因结构分析，分为alter-splicing, SNP, RNA-editing, gene-fusion
            {"name": "alter_splicing_method", "type": "string", "default": "rMATS"},
            # 可变剪切分析软件: rMATS, ASprofile, MapSplice, SpliceGrapher, CLASS2
            {"name": "gene_fusion_reads_number", "type": "int", "default": 3},
            # 基因融合支持的最小read数目值

            {"name": "protein_analysis", "type": "string", "default": ""},
            # 蛋白质分析
            {"name": "tf_database_type", "type": "string", "default": "iTAK"},  # 转录因子分析所用到的数据库

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
        self.map_gene = self.add_module("rna.rnaseq_mapping")
        self.assembly = self.add_module("assemble.refrna_assemble")
        self.exp = self.add_module("rna.express")
        self.exp_diff_trans = self.add_module("denovo_rna.express.diff_analysis")
        self.exp_diff_gene = self.add_module("denovo_rna.express.diff_analysis")
        self.snp_rna = self.add_module("gene_structure.snp_rna")
        self.seq_abs = self.add_tool("annotation.transcript_abstract")
        self.new_abs = self.add_tool("annotation.transcript_abstract")
        self.annotation = self.add_module('annotation.ref_annotation')
        self.new_annotation = self.add_module('annotation.ref_annotation')
        self.network = self.add_module("protein_regulation.ppinetwork_analysis")
        self.tf = self.add_tool("protein_regulation.TF_predict")
        self.merge_trans_annot = self.add_tool("annotation.merge_annot")
        self.merge_gene_annot = self.add_tool("annotation.merge_annot")
        self.ref_genome = ""
        self.geno_database = ["cog"]  # 用于参考基因组提取出的序列的注释
        if not self.option("go_upload_file").is_set:
            self.geno_database.append("go")
            self.geno_database.append("nr")
        elif not self.option("kegg_upload_file").is_set:
            self.geno_database.append("kegg")
        self.step.add_steps("qcstat", "mapping", "assembly", "annotation", "exp", "map_stat",
                            "seq_abs", "transfactor_analysis", "network_analysis", "sample_analysis",
                            "altersplicing")
        
    def check_options(self):
        """
        检查选项
        """
        if not self.option("ref_genome") == "customer_mode":
            if not self.option("genome_structure_file").is_set:
                raise OptionError("未设置gff文件或gtf文件")
            if not self.option("ref_genome_custom").is_set:
                raise OptionError("未设置fa文件")
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
            if i not in ["saturation", "duplication", "distribution", "coverage"]:
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
        }
        if self.option("ref_genome") == "customer_mode":  # 如果是自定义模式,须用户上传基因组
            # self.logger.info(dir(self.option('genome_structure_file')))
            if self.option('genome_structure_file').format == "gene_structure.gff3":
                self.gff = self.option('genome_structure_file').prop["path"]
                opts.update({
                    "gff": self.gff,
                    "ref_genome_custom": self.option("ref_genome_custom")
                })
            else:
                opts.update({
                    "in_gtf": self.option('genome_structure_file').prop["path"],
                    "ref_genome_custom": self.option("ref_genome_custom")
                })
        else:
            self.gff = self.json_dict[self.option("ref_genome")]["gff"]
            opts.update({
                    "gff": self.gff,
                    "ref_genome_custom": self.json[self.option("ref_genome")]["ref_genome"]
                })
        if self.option('group_table').is_set:
            opts.update({'group_table': self.option('group_table')})
        self.filecheck.set_options(opts)
        self.filecheck.run()

    def run_gs(self):
        if self.option("ref_genome") != "customer_mode":
            ref = self.json_dict[self.option("ref_genome")]["ref_genome"]
        else:
            ref = self.option("ref_genome_custom").prop["path"]
        opts = {
            "in_fasta": ref,
            "in_gtf": self.filecheck.option("gtf")
        }
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
        
    def run_seq_abs(self):  # 修改
        if self.option("ref_genome") != "customer_mode":
            ref = self.json_dict[self.option("ref_genome")]["ref_genome"]
        else:
            ref = self.option("ref_genome_custom").prop["path"]
        opts = {
            "ref_genome_custom": ref,
            "ref_genome_gtf": self.filecheck.option("gtf")
        }
        self.seq_abs.set_options(opts)
        self.seq_abs.on('start', self.set_step, {'start': self.step.seq_abs})
        self.seq_abs.on('end', self.set_step, {'end': self.step.seq_abs})
        self.seq_abs.run()

    def run_diamond(self):
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
        if 'go' in self.geno_database:
            self.blast_nr = self.add_module('align.diamond')
            blast_opts.update(
                {
                    'database': self.option("database").split(",")[-1],
                    'evalue': self.option('nr_blast_evalue')
                }
            )
            self.blast_nr.set_options(blast_opts)
            self.blast_modules.append(self.blast_nr)
            self.blast_nr.on('end', self.set_output, 'nrblast')
            self.blast_nr.run()
        if 'cog' in self.geno_database:
            self.blast_string = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.blast_string.set_options(blast_opts)
            self.blast_modules.append(self.blast_string)
            self.blast_string.on('end', self.set_output, 'stringblast')
            self.blast_string.run()
        if 'kegg' in self.geno_database:
            self.blast_kegg = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.blast_kegg.set_options(blast_opts)
            self.blast_modules.append(self.blast_kegg)
            self.blast_kegg.on('end', self.set_output, 'keggblast')
            self.blast_kegg.run()
        self.on_rely(self.blast_modules, self.run_change_diamond, True)

    def run_change_diamond(self, event):
        if event["data"]:
            self.change_tool_before = self.add_tool("align.change_diamondout")
            opts = {
                "nr_out": self.blast_nr.option('outxml'),
                "kegg_out": self.blast_kegg.option('outxml'),
                "string_out": self.blast_string.option('outxml')
            }
            self.change_tool_before.set_options(opts)
            self.change_tool_before.on("end",self.run_diamond_annotation)
            self.change_tool_before.run()
        else:
            self.change_tool_after = self.add_tool("align.change_diamondout")
            opts = {
                "nr_out": self.new_blast_nr.option('outxml'),
                "kegg_out": self.new_blast_kegg.option('outxml'),
                "string_out": self.new_blast_string.option('outxml')
            }
            self.change_tool_after.set_options(opts)
            self.change_tool_after.on("end",self.run_new_diamond_annotation)
            self.change_tool_after.run()

    def run_blast(self):
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
        if 'go' in self.geno_database:
            self.blast_nr = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'nr', 'evalue': self.option('nr_blast_evalue')}
            )
            self.blast_nr.set_options(blast_opts)
            self.blast_modules.append(self.blast_nr)
            self.blast_nr.on('end', self.set_output, 'nrblast')
            self.blast_nr.run()
        if 'cog' in self.geno_database:
            self.blast_string = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.blast_string.set_options(blast_opts)
            self.blast_modules.append(self.blast_string)
            self.blast_string.on('end', self.set_output, 'stringblast')
            self.blast_string.run()
        if 'kegg' in self.geno_database:
            self.blast_kegg = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.blast_kegg.set_options(blast_opts)
            self.blast_modules.append(self.blast_kegg)
            self.blast_kegg.on('end', self.set_output, 'keggblast')
            self.blast_kegg.run()
        self.on_rely(self.blast_modules, self.run_blast_annotation)

    def run_go_upload(self):
        opts = {
            "go_list_upload": self.option("go_upload_file")
        }
        self.go_upload = self.add_tool("annotation.go_upload")
        self.go_upload.set_options(opts)
        self.go_upload.run()

    def run_kegg_upload(self):
        taxonmy = ""  # 读取参考基因组taxonmy
        opts = {
            "kos_list_upload": self.option("kegg_upload_file"),
            "taxonmy": None
        }
        self.kegg_upload = self.add_tool("annotation.kegg_upload")
        self.kegg_upload.set_options(opts)
        self.kegg_upload.run()

    def run_blast_annotation(self):
        anno_opts = {
            'gene_file': self.seq_abs.option('gene_file'),
        }
        if 'go' in self.geno_database:
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.blast_nr.option('outxml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.geno_database:
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.blast_nr.option('outxml'),
                'blast_nr_table': self.blast_nr.option('outtable')
            })
        else:
            anno_opts.update(
                {
                    'nr_annot': False,
                    'gos_list_upload':self.option("go_upload_file")
                 }
            )
        if 'kegg' in self.geno_database:
            anno_opts.update({
                'blast_kegg_xml': self.blast_kegg.option('outxml'),
                'blast_kegg_table': self.blast_kegg.option('outtable')
            })
        else:
            anno_opts.update(
                {
                    'nr_annot': False,
                    'kos_list_upload':self.option("kegg_upload_file")
                 }
            )
        if 'cog' in self.geno_database:
            anno_opts.update({
                'blast_string_xml': self.blast_string.option('outxml'),
                'blast_string_table': self.blast_string.option('outtable')
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.annotation.set_options(anno_opts)
        self.annotation.on('end', self.set_output, 'annotation')
        self.annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.annotation.run()

    def run_diamond_annotation(self):
        anno_opts = {
            'gene_file': self.seq_abs.option('gene_file'),
        }
        if 'go' in self.geno_database:
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.change_tool_before.option('blast_nr_xml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.geno_database:
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.change_tool_before.option('blast_nr_xml'),
            })
        else:
            anno_opts.update(
                {
                    'nr_annot': False,
                    'gos_list_upload':self.option("go_upload_file")
                 }
            )
        if 'kegg' in self.geno_database:
            anno_opts.update({
                'blast_kegg_xml': self.change_tool_before.option('blast_kegg_xml')
            })
        else:
            anno_opts.update(
                {
                    'nr_annot': False,
                    'kos_list_upload':self.option("kegg_upload_file")
                 }
            )
        if 'cog' in self.geno_database:
            anno_opts.update({
                'blast_string_xml': self.change_tool_before.option('blast_string_xml'),
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.annotation.set_options(anno_opts)
        self.annotation.on('end', self.set_output, 'annotation')
        self.annotation.on('end', self.set_step, {'end': self.step.annotation})
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
        if self.option("ref_genome") != "customer_mode":
            self.ref_genome = self.json_dict[self.option("ref_genome")]["ref_genome"]
        else:
            self.ref_genome = self.option("ref_genome_custom").prop["path"]
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

    def run_new_abs(self):
        opts = {
            "ref_genome_custom": self.ref_genome,
            "ref_genome_gtf": self.assembly.option("new_gtf")
        }
        self.new_abs.set_options(opts)
        self.new_abs.run()

    def run_new_diamond(self):
        self.new_blast_modules = []
        self.gene_list = self.new_abs.option('gene_file')
        blast_lines = int(self.new_abs.option('query').prop['seq_number']) / 10
        self.logger.info('.......blast_lines:%s' % blast_lines)
        blast_opts = {
            'query': self.new_abs.option('query'),
            'query_type': 'nucl',
            'database': None,
            'blast': 'blastx',
            'evalue': None,
            'outfmt': 5,
            'lines': blast_lines,
        }
        if 'go' in self.option('database') or 'nr' in self.option('database'):
            self.new_blast_nr = self.add_module('align.diamond')
            blast_opts.update(
                {
                    'database': self.option("database").split(",")[-1],
                    'evalue': self.option('nr_blast_evalue')
                }
            )
            self.new_blast_nr.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_nr)
            self.new_blast_nr.on('end', self.set_output, 'new_nrblast')
            self.new_blast_nr.run()
        if 'cog' in self.option('database'):
            self.new_blast_string = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.new_blast_string.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_string)
            self.new_blast_string.on('end', self.set_output, 'new_stringblast')
            self.new_blast_string.run()
        if 'kegg' in self.option('database'):
            self.new_blast_kegg = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.new_blast_kegg.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_kegg)
            self.new_blast_kegg.on('end', self.set_output, 'new_keggblast')
            self.new_blast_kegg.run()
        # 增加swissprot
        self.on_rely(self.new_blast_modules, self.run_change_diamond, False)  # false只是表示后进行的change tool

    def run_new_blast(self):
        self.new_blast_modules = []
        self.gene_list = self.new_abs.option('gene_file')
        blast_lines = int(self.new_abs.option('query').prop['seq_number']) / 10
        self.logger.info('.......blast_lines:%s' % blast_lines)
        blast_opts = {
            'query': self.new_abs.option('query'),
            'query_type': 'nucl',
            'database': None,
            'blast': 'blastx',
            'evalue': None,
            'outfmt': 5,
            'lines': blast_lines,
        }
        if 'go' in self.option('database') or 'nr' in self.option('database'):
            self.new_blast_nr = self.add_module('align.blast')
            blast_opts.update(
                {
                    'database': self.option("database").split(",")[-1],
                    'evalue': self.option('nr_blast_evalue')
                }
            )
            self.new_blast_nr.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_nr)
            self.new_blast_nr.on('end', self.set_output, 'new_nrblast')
            self.new_blast_nr.run()
        if 'cog' in self.option('database'):
            self.new_blast_string = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.new_blast_string.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_string)
            self.new_blast_string.on('end', self.set_output, 'new_stringblast')
            self.new_blast_string.run()
        if 'kegg' in self.option('database'):
            self.new_blast_kegg = self.add_module('align.blast')
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.new_blast_kegg.set_options(blast_opts)
            self.new_blast_modules.append(self.new_blast_kegg)
            self.new_blast_kegg.on('end', self.set_output, 'new_keggblast')
            self.new_blast_kegg.run()
        # 增加swissprot
        self.on_rely(self.new_blast_modules, self.run_new_blast_annotation)  # false只是表示后进行的change tool

    def run_new_diamond_annotation(self):
        anno_opts = {
            'gene_file': self.new_abs.option('gene_file'),
        }
        if 'go' in self.option('database'):
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.change_tool_after.option('blast_nr_xml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.option('database'):
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.change_tool_after.option('blast_nr_xml'),
            })
        else:
            anno_opts.update({'nr_annot': False})
        if 'kegg' in self.option('database'):
            anno_opts.update({
                'blast_kegg_xml': self.change_tool_after.option('blast_kegg_xml'),
            })
        if 'cog' in self.option('database'):
            anno_opts.update({
                'blast_string_xml': self.change_tool_after.option('blast_string_xml'),
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.new_annotation.set_options(anno_opts)
        self.new_annotation.on('end', self.set_output, 'new_annotation')
        self.new_annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.new_annotation.run()

    def run_new_blast_annotation(self):
        anno_opts = {
            'gene_file': self.new_abs.option('gene_file'),
        }
        if 'go' in self.option('database'):
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.new_blast_nr.option('blast_nr_xml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.option('database'):
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.new_blast_nr.option('blast_nr_xml'),
            })
        else:
            anno_opts.update({'nr_annot': False})
        if 'kegg' in self.option('database'):
            anno_opts.update({
                'blast_kegg_xml': self.new_blast_kegg.option('blast_kegg_xml'),
            })
        if 'cog' in self.option('database'):
            anno_opts.update({
                'blast_string_xml': self.new_blast_string.option('blast_string_xml'),
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.new_annotation.set_options(anno_opts)
        self.new_annotation.on('end', self.set_output, 'new_annotation')
        self.new_annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.new_annotation.run()

    def run_snp(self):
        if not self.filecheck.option("genome_status"):
            self.logger.info("基因组结构不完整")
            self.snp_rna.fire("end")
        else:
            if self.option("ref_genome") != "customer_mode":
                ref_genome = self.json_dict[self.option("ref_genome")]["ref_genome"]
            else:
                ref_genome = self.option("ref_genome_custom").prop["path"]
            opts = {
                "ref_genome_custom": ref_genome,
                "ref_genome":  "customer_mode",
                "ref_gtf": self.filecheck.option("gtf"),
                "seq_method": self.option("fq_type"),
                "fastq_dir": self.qc.option("sickle_dir")
            }
            self.snp_rna.set_options(opts)
            self.snp_rna.on("end", self.set_output, "snp_rna")
            self.snp_rna.run()
        
    def run_map_assess(self):
        # assess_method = self.option("map_assess_method") + ",stat"
        assess_method = "saturation,duplication,coverage,stat"
        opts = {
            "bam": self.mapping.option("bam_output").prop["path"],
            "analysis": assess_method,
            "bed": self.filecheck.option("bed").prop["path"]
        }
        self.map_qc.set_options(opts)
        self.map_qc.on("end", self.set_output, "map_qc")
        self.map_qc.run()
    
    def run_exp(self):  # 表达量与表达差异模块
        self.logger.info("开始运行表达量模块")
        opts = {
            "fq_type": self.option("fq_type"),
            "ref_gtf": self.filecheck.option("gtf"),
            "merged_gtf": self.assembly.option("merged_gtf"),
            "cmp_gtf": self.assembly.option("cuff_gtf"),
            "sample_bam": self.mapping.option("bam_output"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_ci": self.option("diff_ci"),
            "strand_dir": self.option("strand_dir")
        }
        tool = self.exp
        tool.set_options(opts)
        tool.on("end", self.set_output, "exp")
        tool.on('start', self.set_step, {'start': "exp"})
        tool.on('end', self.set_step, {'end': "exp"})
        tool.run()

    def run_network(self):
        if self.option("ref_genome") != "customer_mode":
            opts = {
                "diff_exp": self.exp.option("diff_list"),
                "species_list": "",
                "species": self.option("ref_genome"),
                "combine_score": self.option("combine_score"),
                "logFC": self.option("logFC")
            }
            self.network.set_options(opts)
            self.network.on("end", self.set_output, "network_analysis")
            self.network.on('start', self.set_step, {'start': self.step.network_analysis})
            self.network.on('end', self.set_step, {'end': self.step.network_analysis})
            self.network.run()
        else:
            self.network.fire("end")

    def run_altersplicing(self):
        if not self.filecheck.option("genome_status"):
            self.logger.info("基因组结构不完整")
            self.altersplicing.fire("end")
        else:
            if self.option("strand_specific"):
                lib_type = "fr-firststrand"
            else:
                lib_type = "fr-unstranded"
            gtf_path = self.filecheck.option("gtf").prop["path"]
            opts = {
                "sample_bam_dir": self.mapping.option("bam_output"),
                "lib_type": lib_type,
                "ref_gtf": gtf_path,
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
            self.altersplicing.run()

    def run_tf(self):
        if self.option("ref_genome") != "customer_mode":
            opts = {
                "diff_gene_list": self.exp.option("diff_list"),
                "database": self.option("tf_database_type")
            }
            self.tf.set_options(opts)
            self.tf.on("end", self.set_output, "tf")
            self.tf.on('start', self.set_step, {'start': self.step.transfactor_analysis})
            self.tf.on('end', self.set_step, {'end': self.step.transfactor_analysis})
            self.tf.run()
        else:
            self.tf.fire("end")

    def run_merge_annot(self):
        if not self.option("go_upload_file").is_set:
            gos_dir1_trans = self.annotation.work_dir + "/GoAnnotation/output/query_gos.list"
            gos_dir1_gene = self.annotation.work_dir + "/RefAnnoStat/go_stat/gene_gos.list"
        else:
            gos_dir1_trans = self.go_upload.output_dir + "/query_gos.list"
            gos_dir1_gene = self.annotation.option("gene_go_list").prop["path"]
        gos_dir2_trans = self.new_annotation.work_dir + "/GoAnotation/ouptut/query_gos.list"
        gos_dir2_gene = self.new_annotation.work_dir + "/RefAnnoStat/go_stat/gene_gos.list"
        if not self.option("go_upload_file").is_set:
            kegg_table_dir1_trans = self.annotation.work_dir + "/KeggAnnotation/output/kegg_table.xls"
            kegg_table_dir1_gene = self.annotation.work_dir + "/RefAnnoStat/kegg_stat/gene_kegg_table.xls"
        else:
            kegg_table_dir1_trans = self.kegg_upload.output_dir + "/kegg_table.xls"
            kegg_table_dir1_gene = self.annotation.option("gene_kegg_table").prop["path"]
        kegg_table_dir2_trans = self.new_annotation.work_dir + "/KeggAnnotation/output/kegg_table.xls"
        kegg_table_dir2_gene = self.new_annotation.work_dir + "/RefAnnoStat/kegg_stat/gene_kegg_table.xls"
        cog_table_dir1_trans = self.annotation.work_dir + "/String2cogv9/output/cog_table.xls"
        cog_table_dir2_trans = self.new_annotation.work_dir + "/String2cogv9/output/cog_table.xls"
        cog_table_dir1_gene = self.annotation.work_dir + "/RefAnnoStat/cog_stat/gene_cog_table.xls"
        cog_table_dir2_gene = self.new_annotation.work_dir + "/RefAnnoStat/cog_stat/gene_cog_table.xls"
        # transcripts参数生成
        gos_dir_trans = gos_dir1_trans + ";" + gos_dir2_trans
        kegg_table_dir_trans = kegg_table_dir1_trans + ";" + kegg_table_dir2_trans
        cog_table_dir_trans = cog_table_dir1_trans + ";" + cog_table_dir2_trans
        # gene参数生成
        gos_dir_gene = gos_dir1_gene + ";" + gos_dir2_gene
        kegg_table_dir_gene = kegg_table_dir1_gene + ";" + kegg_table_dir2_gene
        cog_table_dir_gene = cog_table_dir1_gene + ";" + cog_table_dir2_gene
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
        if self.exp.diff_gene:
            exp_diff_opts = {
                'diff_fpkm': self.exp.option('diff_fpkm'),
                'analysis': self.option('exp_analysis')
            }
            if 'network' in self.option('exp_analysis'):
                exp_diff_opts.update({'gene_file': self.exp.option('gene_file')})
            elif 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'kegg_path': self.merge_trans_annot.option('kegg_table'),
                    'diff_list_dir': self.exp.option('diff_list_dir')
                })
            elif 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'go_list': self.merge_trans_annot.option('golist_out'),
                    'diff_list_dir': self.exp.option('diff_list_dir'),
                    'all_list': self.exp.option('all_list'),
                    'go_level_2': self.merge_trans_annot.option('go2level_out')
                })
            elif 'cog_class' in self.option('exp_analysis'):  # 需要修改
                exp_diff_opts.update({
                    'cog_table': self.merge_trans_annot.option('cog_table'),
                    'diff_list_dir': self.exp.option('diff_list_dir'),
                })
            else:
                pass
            self.exp_diff_trans.set_options(exp_diff_opts)
            self.exp_diff_trans.on('end', self.set_output, 'exp_diff_trans')
            self.exp_diff_trans.run()
        else:
            self.logger.info("输入文件数据量过小，没有检测到差异基因，差异基因相关分析将忽略")

    def run_exp_gene_diff(self):
        if self.exp.diff_gene:
            exp_diff_opts = {
                'diff_fpkm': self.exp.option('diff_fpkm'),
                'analysis': self.option('exp_analysis')
            }
            if 'network' in self.option('exp_analysis'):
                exp_diff_opts.update({'gene_file': self.exp.option('gene_file')})
            elif 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'kegg_path': self.merge_gene_annot.option('kegg_table'),
                    'diff_list_dir': self.exp.option('diff_list_dir')
                })
            elif 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'go_list': self.merge_gene_annot.option('golist_out'),
                    'diff_list_dir': self.exp.option('diff_list_dir'),
                    'all_list': self.exp.option('all_list'),
                    'go_level_2': self.merge_gene_annot.option('go2level_out')
                })
            elif 'cog_class' in self.option('exp_analysis'):  # 需要修改
                exp_diff_opts.update({
                    'cog_tbale': self.merge_gene_annot.option('cog_table'),
                    'diff_list_dir': self.exp.option('diff_list_dir'),
                })
            else:
                pass
            self.exp_diff_gene.set_options(exp_diff_opts)
            self.exp_diff_gene.on('end', self.set_output, 'exp_diff_gene')
            self.exp_diff_gene.run()
        else:
            self.logger.info("输入文件数据量过小，没有检测到差异基因，差异基因相关分析将忽略")
        
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
        if event['data'] == 'tf':
            self.move2outputdir(obj.output_dir, 'transfactor_analysis')
            self.logger.info("transfactor_analysis文件移动完成")
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
            
    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_seq_abs)
        if self.option("blast_method") == "diamond":
            self.seq_abs.on('end', self.run_diamond)
            self.new_abs.on("end", self.run_new_diamond)
        else:
            self.seq_abs.on('end', self.run_blast)
            self.new_abs.on("end", self.run_new_blast)
        self.module_before_merge = [self.new_annotation, self.annotation]
        if self.option("go_upload_file").is_set:
            self.filecheck.on('end', self.run_go_upload)
            self.module_before_merge.append(self.go_upload)
        if self.option("kegg_upload_file").is_set:
            self.filecheck.on('end', self.run_kegg_upload)
            self.module_before_merge.append(self.kegg_upload)
        self.on_rely(self.module_before_merge, self.run_merge_annot)
        self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        self.filecheck.on("end", self.run_gs)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        self.qc.on('end', self.run_snp)
        self.on_rely([self.qc, self.seq_abs], self.run_map_gene)
        self.mapping.on("end", self.run_altersplicing)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp)
        self.assembly.on("end", self.run_new_abs)
        self.exp.on("end", self.run_tf)
        self.exp.on("end", self.run_network)
        self.on_rely([self.snp_rna, self.network, self.altersplicing, self.tf, self.exp_diff_gene,
                      self.exp_diff_trans], self.end)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def test_run(self):
        """
        ref-rna workflow test_run方法
        :return:
        """
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_seq_abs)
        # if self.option("blast_method") == "diamond":
        #     self.seq_abs.on('end', self.run_diamond)
        #     self.new_abs.on("end", self.run_new_diamond)
        # else:
        #     self.seq_abs.on('end', self.run_blast)
        #     self.new_abs.on("end", self.run_new_blast)
        # self.module_before_merge = [self.new_annotation, self.annotation]
        # if self.option("go_upload_file").is_set:
        #     self.filecheck.on('end', self.run_go_upload)
        #     self.module_before_merge.append(self.go_upload)
        # if self.option("kegg_upload_file").is_set:
        #     self.filecheck.on('end', self.run_kegg_upload)
        #     self.module_before_merge.append(self.kegg_upload)
        # self.on_rely(self.module_before_merge, self.run_merge_annot)
        # self.on_rely([self.merge_trans_annot, self.exp], self.run_exp_trans_diff)
        # self.on_rely([self.merge_gene_annot, self.exp], self.run_exp_gene_diff)
        self.filecheck.on("end", self.run_gs)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        self.qc.on('end', self.run_snp)
        self.on_rely([self.qc, self.seq_abs], self.run_map_gene)
        self.mapping.on("end", self.run_altersplicing)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp)
        self.assembly.on("end", self.run_new_abs)
        self.exp.on("end", self.run_tf)
        self.exp.on("end", self.run_network)
        self.on_rely([self.snp_rna, self.network, self.altersplicing, self.tf, self.exp_diff_gene,
                      self.exp_diff_trans], self.end)
        #self.on_rely([self.gs, self.seq_abs], self.end)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()
        
    def end(self):
        super(RefrnaWorkflow, self).end()
