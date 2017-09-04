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
import time
from gevent.monkey import patch_all
import gevent
import functools

patch_all()

def time_count(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        start = time.time()
        func(*args, **kw)
        end = time.time()
        print("{}函数执行耗时{}s".format(func.__name__, end - start))
    return wrapper

class RefrnaWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(RefrnaWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "taxonmy", "type":"string", "default": "Animal"},
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
            {"name": "qc_quality", "type": "int", "default": 20},  # 质量剪切中保留的最小质量值
            {"name": "qc_length", "type": "int", "default": 30},  # 质量剪切中保留的最短序列长度

            {"name": "ref_genome", "type": "string", "default": "Custom"},  # 参考基因组
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组

            # 增加evalue参数，再转换为float传给module使用
            {"name": "nr_evalue", "type": "string", "default": "1e-3"},
            {"name": "string_evalue", "type": "string", "default": "1e-3"},
            {"name": "kegg_evalue", "type": "string", "default": "1e-3"},
            {"name": "swissprot_evalue", "type": "string", "default": "1e-3"},

            {"name": "nr_blast_evalue", "type": "float", "default": 1e-3},  # NR比对e值
            {"name": "string_blast_evalue", "type": "float", "default": 1e-3},  # String比对使用的e值
            {"name": "kegg_blast_evalue", "type": "float", "default": 1e-3},  # KEGG注释使用的e值
            {"name": "swissprot_blast_evalue", "type": "float", "default": 1e-3},  # Swissprot比对使用的e值
            {"name": "database", "type": "string", "default": 'go,nr,cog,kegg,swissprot,pfam'},
            # 全部六个注释
            {"name": "nr_database", "type": "string", "default": "All"},  # nr库类型
            {"name": "kegg_database", "type": "string", "default": "All"},  # kegg注释库类型

            {"name": "seq_method", "type": "string", "default": "Hisat"},  # 比对方法，Tophat or Hisat
            {"name": "map_assess_method", "type": "string", "default":
                "saturation,duplication,distribution,coverage,chr_stat"},
            # 比对质量评估分析
            {"name": "mate_std", "type": "int", "default": 50},  # 末端配对插入片段长度标准差
            {"name": "mid_dis", "type": "int", "default": 50},  # 两个成对引物间的距离中间值
            {"name": "result_reserved", "type": "int", "default": 1},  # 最多保留的比对结果数目

            {"name": "assemble_method", "type": "string", "default": "stringtie"},
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

            {"name": "combine_score", "type": "int", "default": 300},  # 蛋白质分析
            {"name": "p_length", "type": "int", "default": 100},  # pfam参数
            {"name": "markov_length", "type": "int", "default": 3000},  # markov_length


        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.json_path = self.config.SOFTWARE_DIR + "/database/Genome_DB_finish/annot_species.json"
        self.json_dict = self.get_json()
        self.filecheck = self.add_tool("rna.filecheck_ref")
        self.qc = self.add_module("sequence.hiseq_qc")
        self.qc_stat_before = self.add_module("sequence.hiseq_reads_stat")
        self.qc_stat_after = self.add_module("sequence.hiseq_reads_stat")
        self.mapping = self.add_module("rna.rnaseq_mapping")
        self.altersplicing = self.add_module("gene_structure.rmats")
        self.map_qc = self.add_module("denovo_rna.mapping.map_assessment")
        self.assembly = self.add_module("assemble.refrna_assemble")
        self.exp = self.add_module("rna.express")
        self.exp_fc = self.add_module("rna.express")
        self.snp_rna = self.add_module("gene_structure.snp_rna")
        self.new_gene_abs = self.add_tool("annotation.transcript_abstract")
        self.new_trans_abs = self.add_tool("annotation.transcript_abstract")
        self.new_annotation = self.add_module('annotation.ref_annotation')
        self.merge_trans_annot = self.add_tool("annotation.merge_annot")
        self.merge_gene_annot = self.add_tool("annotation.merge_annot")
        self.pfam = self.add_tool("denovo_rna.gene_structure.orf")
        if self.option("ref_genome") != "Custom":
            self.ref_genome = os.path.join(os.path.split(self.json_path)[0],
                                           self.json_dict[self.option("ref_genome")]["dna_fa"])
            self.option("ref_genome_custom", self.ref_genome)
            self.taxon_id = self.json_dict[self.option("ref_genome")]["taxon_id"]
            if "anno_path" not in self.json_dict[self.option("ref_genome")]:
                raise Exception("json文件中不存在注释文件，程序退出")
            self.anno_path = os.path.join(os.path.split(self.json_path)[0],
                                          self.json_dict[self.option("ref_genome")]["anno_path"])
            if not os.path.exists(self.anno_path):
                raise Exception("不存在注释文件，程序退出")
            self.logger.info("注释文件路径为： " + self.anno_path)
        else:
            self.ref_genome = self.option("ref_genome_custom")
            self.taxon_id = ""
        self.gff = ""
        if self.option("ref_genome") != "Custom":
            gtf_path = os.path.join(os.path.split(self.json_path)[0],
                                           self.json_dict[self.option("ref_genome")]["gtf"])
            self.option('genome_structure_file', gtf_path)
        else:
            if self.option("genome_structure_file").format == "gene_structure.gff3":
                self.gff = self.option('genome_structure_file').prop["path"]
        self.final_tools = [self.snp_rna, self.altersplicing, self.exp_fc, self.exp, self.merge_trans_annot,
                            self.merge_gene_annot]
        self.genome_status = True
        self.step.add_steps("filecheck", "rna_qc", "mapping", "assembly", "new_annotation", "express", "snp_rna")
        if self.option("ref_genome") == "Custom":
            self.option("ref_genome", "customer_mode")  # 统一转化为customer_mode


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
        if self.option("kegg_database") == "All":
            self.option("kegg_database","None")
        elif self.option("kegg_database") == "Animal":
            self.option("kegg_database","Animals")
        elif self.option("kegg_database") == "Plant":
            self.option("kegg_database","Plants")
        elif self.option("kegg_database") == "Protist":
            self.option("kegg_database","Protists")
        if self.option("nr_database") == "All":
            self.option("nr_database", "nr")
        else:
            nr = self.option("nr_database").lower()
            self.option("nr_database", nr)
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

    def run_qc(self):
        self.qc.set_options({
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type')
        })
        self.qc.on('end', self.set_output, 'qc')
        self.qc.on('start', self.set_step, {'start': self.step.rna_qc})
        self.qc.on('end', self.set_step, {'end': self.step.rna_qc})
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

    def run_mapping(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome": self.option("ref_genome"),
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
            "ref_genome_gtf": self.filecheck.option("gtf"),
            'length_file': self.new_trans_abs.option('length_file'),
            'new_gtf': self.assembly.option("new_transcripts_gtf")
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
        if os.path.exists(self.pfam.output_dir + "/pfam_domain"):
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
            "in_bam": self.mapping.option("bam_output")
        }
        self.snp_rna.set_options(opts)
        self.snp_rna.on("start", self.set_step, {"start": self.step.snp_rna})
        self.snp_rna.on("end", self.set_step, {"end": self.step.snp_rna})
        self.snp_rna.on("end", self.set_output, "snp")
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
        mod.run()

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
        self.altersplicing.run()

    def run_merge_annot(self):
        """
        根据新加入模块操作，修改self.annotation
        :return:
        """
        if self.option("ref_genome") == "customer_mode":
            self.anno_path = self.annotation.output_dir
        gos_dir_trans = self.anno_path + "/go/query_gos.list" + \
            ";" + self.new_annotation.output_dir + "/go/query_gos.list"
        kegg_table_dir_trans = self.anno_path + "/kegg/kegg_table.xls" + \
            ";" + self.new_annotation.output_dir + "/kegg/kegg_table.xls"
        pathway_table_dirs_trans = self.anno_path + "/kegg/pathway_table.xls" + \
            ";" + self.new_annotation.output_dir + "/kegg/pathway_table.xls"
        cog_table_dir_trans = self.anno_path + "/cog/cog_table.xls" + \
            ";" + self.new_annotation.output_dir + "/cog/cog_table.xls"
        gos_dir_gene = self.anno_path + "/anno_stat/go_stat/gene_gos.list" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/go_stat/gene_gos.list"
        kegg_table_dir_gene = self.anno_path + "/anno_stat/kegg_stat/gene_kegg_table.xls" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/kegg_stat/gene_kegg_table.xls"
        pathway_table_dirs_gene = self.anno_path + "/anno_stat/kegg_stat/gene_pathway_table.xls" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/kegg_stat/gene_pathway_table.xls"
        cog_table_dir_gene = self.anno_path + "/anno_stat/cog_stat/gene_cog_table.xls" + \
            ";" + self.new_annotation.output_dir + "/anno_stat/cog_stat/gene_cog_table.xls"
        trans_opts = {
            "gos_dir": gos_dir_trans,
            "kegg_table_dir": kegg_table_dir_trans,
            "cog_table_dir": cog_table_dir_trans,
            "pathway_table_dir": pathway_table_dirs_trans
        }
        gene_opts = {
            "gos_dir": gos_dir_gene,
            "kegg_table_dir": kegg_table_dir_gene,
            "cog_table_dir": cog_table_dir_gene,
            "pathway_table_dir": pathway_table_dirs_gene
        }
        self.merge_trans_annot.set_options(trans_opts)
        self.merge_gene_annot.set_options(gene_opts)
        self.merge_trans_annot.run()
        self.merge_gene_annot.run()

    def move2outputdir(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下
        """
        start = time.time()
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            os.makedirs(newdir)
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
            self.move_file(oldfiles[i], newfiles[i])
        end = time.time()
        duration = end - start
        self.logger.info("文件夹{}到{}移动耗时{}s".format(olddir, newdir, duration))

    def move_file(self, old_file, new_file):
        if os.path.isfile(old_file):
            os.link(old_file, new_file)
        else:
            os.mkdir(new_file)
            for file in os.listdir(old_file):
                file_path = os.path.join(old_file, file)
                new_path = os.path.join(new_file, file)
                self.move_file(file_path, new_path)

    def set_output(self, event):
        # pass
        obj = event["bind_object"]
        if event['data'] == 'qc':
            self.move2outputdir(obj.output_dir, 'QC_stat')
        if event['data'] == 'qc_stat_before':
            self.move2outputdir(obj.output_dir, 'QC_stat/before_qc')
            self.logger.info("开始设置qc的输出目录")
        if event['data'] == 'qc_stat_after':
            self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
        if event['data'] == 'mapping':
            self.move2outputdir(obj.output_dir, 'mapping')
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, 'map_qc')
        if event['data'] == 'assembly':
            self.move2outputdir(obj.output_dir, 'assembly')
        if event['data'] == 'snp':
            self.move2outputdir(obj.output_dir, 'snp')
        if event['data'] == 'exp':
            self.move2outputdir(obj.output_dir, 'exp')
        if event['data'] == 'exp_fc_all':
            self.move2outputdir(obj.output_dir, 'exp_fc_all')

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
        self.move2outputdir(self.new_blast_swissprot.output_dir, 'new_swissprotblast')
        self.move2outputdir(self.pfam.output_dir, 'pfam')
        self.move2outputdir(self.altersplicing.output_dir, 'altersplicing')
        self.logger.info("结果文件导入完成！")


    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        task_info = self.api.api('task_info.ref')
        task_info.add_task_info()
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        self.mapping.on('end', self.run_assembly)
        self.mapping.on('end', self.run_snp)
        self.mapping.on('end', self.run_altersplicing)
        self.mapping.on('end', self.run_map_assess)
        self.assembly.on("end", self.run_exp_rsem_default)
        self.assembly.on("end", self.run_exp_fc)
        self.assembly.on("end", self.run_new_transcripts_abs)
        self.assembly.on("end", self.run_new_gene_abs)
        self.on_rely([self.new_gene_abs, self.new_trans_abs], self.run_new_align, "diamond")
        self.new_annotation.on("end", self.run_merge_annot)
        self.on_rely(self.final_tools, self.end)
        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def end(self):
        self.run_api_and_set_output()
        self.modify_output()
        super(RefrnaWorkflow, self).end()

    def modify_output(self):
        if os.path.exists(self.work_dir + "/upload_results"):
            os.removedirs(self.work_dir + "/upload_results")
        os.mkdir(self.work_dir + "/upload_results")





    def run_api_and_set_output(self, test=False):
        greenlets_list_first = []
        greenlets_list_sec = []
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_AFTER_END = False
        for file in os.listdir(self.filecheck.work_dir):
            if file.endswith("gtf") and not "tmp" in file:
                self.logger.info(file)
        self.filecheck.option("gtf", self.filecheck.work_dir + "/" + file)
        self.export_qc()
        self.logger.info("开始进行表达量rsem导表")
        start = time.time()
        self.export_exp_rsem_default()
        end = time.time()
        self.logger.info("表达量rsem导表结束，用时{}s".format(end - start))
        greenlets_list_first.append(gevent.spawn(self.export_as))
        greenlets_list_first.append(gevent.spawn(self.export_annotation))
        greenlets_list_first.append(gevent.spawn(self.export_assembly))
        greenlets_list_first.append(gevent.spawn(self.export_snp))
        greenlets_list_first.append(gevent.spawn(self.export_map_assess))
        greenlets_list_first.append(gevent.spawn(self.export_exp_fc, test))
        greenlets_list_first.append(gevent.spawn(self.export_ref_gene_set))
        greenlets_list_first.append(gevent.spawn(self.export_ref_diff_gene))
        greenlets_list_first.append(gevent.spawn(self.export_ref_diff_trans))
        greenlets_list_first.append(gevent.spawn(self.export_cor))
        greenlets_list_first.append(gevent.spawn(self.export_pca))
        gevent.joinall(greenlets_list_first)
        greenlets_list_sec.append(gevent.spawn(self.export_gene_set))
        greenlets_list_sec.append(gevent.spawn(self.export_diff_gene))
        greenlets_list_sec.append(gevent.spawn(self.export_diff_trans))
        gevent.joinall(greenlets_list_sec)
        self.logger.info("导表完成")
        # self.export_as()
        # self.export_annotation()
        # self.export_assembly()
        # self.export_snp()
        # self.export_map_assess()
        # self.export_exp_rsem_default()
        # self.export_ref_gene_set()
        # self.export_gene_set()
        # self.export_ref_diff_gene()
        # self.export_ref_diff_trans()
        # self.export_diff_gene()
        # self.export_diff_trans()
        # self.export_cor()
        # self.export_pca()

    @time_count
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
        self.group_id, self.group_detail, self.group_category = self.api_qc.add_specimen_group(self.option("group_table").prop["path"])
        self.logger.info("group_detail为：" + str(self.group_detail))
        self.control_id, self.compare_detail = self.api_qc.add_control_group(self.option("control_file").prop["path"], self.group_id)
        self.api_qc.add_bam_path(self.mapping.output_dir)

    @time_count
    def export_assembly(self):
        self.api_assembly = self.api.api("ref_rna.ref_assembly")
        if self.option("assemble_method") == "cufflinks":
            all_gtf_path = self.assembly.output_dir + "/Cufflinks"
            merged_path = self.assembly.output_dir + "/Cuffmerge"
        else:
            all_gtf_path = self.assembly.output_dir + "/Stringtie"
            merged_path = self.assembly.output_dir + "/StringtieMerge"
        self.api_assembly.add_assembly_result(all_gtf_path=all_gtf_path, merged_path=merged_path, Statistics_path=self.assembly.output_dir + "/Statistics")

    @time_count
    def export_map_assess(self):
        self.api_map = self.api.ref_rna_qc
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

    @time_count
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

    @time_count
    def export_exp_fc(self,test=True):
        if test:
            distri_path = self.exp_fc.work_dir + "/Featurecounts"
        else:
            distri_path = self.exp_fc.featurecounts.work_dir
        self.api_exp = self.api.refrna_express
        feature_dir = self.exp_fc.output_dir + "/featurecounts"
        if self.option("is_duplicate"):
            if test:
                group_fpkm_path = self.exp_fc.work_dir + "/Featurecounts/group"
            else:
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
        # distri_path = self.exp_fc.featurecounts.work_dir
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

    @time_count
    def export_gene_set(self):  # ref_and_new
        self.api_geneset = self.api.refrna_express
        group_id = self.group_id
        path = self.exp.output_dir + "/diff/trans_diff/diff_stat_dir"
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           group_id=group_id, name=name, compare_name=compare_name,
                                                           ref_new="refandnew",
                                                           express_method="rsem", type="transcript", up_down='up_down', major=True)
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
                                                           ref_new="refandnew",
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene",up_down='up_down', major=True)
                else:
                    self.logger.info("基因name和compare_name匹配错误")

    @time_count
    def export_ref_gene_set(self):
        self.api_geneset = self.api.refrna_express
        group_id = self.group_id
        path = self.exp.output_dir + "/ref_diff/trans_ref_diff/diff_stat_dir"
        for files in os.listdir(path):
            if re.search(r'edgr_stat.xls',files):
                m_ = re.search(r'(\w+?)_vs_(\w+?).edgr_stat.xls', files)
                if m_:
                    name = m_.group(1)
                    compare_name = m_.group(2)
                    up_down = self.api_geneset.add_geneset(diff_stat_path=path+"/"+files,
                                                           ref_new="ref",
                                                           group_id=group_id, name=name, compare_name=compare_name,
                                                           express_method="rsem", type="transcript", up_down='up_down', major=True)
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
                                                           ref_new="ref",
                                                           name=name, compare_name=compare_name, express_method="rsem",
                                                           type="gene",up_down='up_down', major=True)
                else:
                    self.logger.info("基因name和compare_name匹配错误")

    @time_count
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

    @time_count
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

    @time_count
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

    @time_count
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

    @time_count
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
                                           express_level=self.option("exp_way"),
                                           express_id=self.express_id, detail=True, seq_type="gene")

    @time_count
    def export_pca(self):
        self.api_pca = self.api.refrna_corr_express
        pca_path = self.exp.output_dir + "/pca/genes_pca"
        group_detail = dict()
        for i in range(len(self.group_category)):
            key = self.group_category[i]
            value = self.group_detail[i].keys()
            group_detail[key] = value
        self.api_pca.add_pca_table(pca_path, group_id=str(self.group_id), group_detail=group_detail,
                                   express_level=self.option("exp_way"),
                                   express_id=self.express_id, detail=True, seq_type="gene")
    @time_count
    def export_annotation(self):
        self.api_anno = self.api.api("ref_rna.ref_annotation")
        ref_anno_path = self.anno_path
        params = {
            "nr_evalue": self.option("nr_blast_evalue"),
            "nr_similarity": 0,
            "nr_score": 0,
            "nr_identity": 0,
            "swissprot_evalue": self.option("swissprot_blast_evalue"),
            "swissprot_similarity": 0,
            "swissprot_score": 0,
            "swissprot_identity": 0,
        }
        params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        new_anno_path = self.new_annotation.output_dir
        pfam_path = self.pfam.output_dir + "/pfam_domain"
        merge_tran_output = self.merge_trans_annot.output_dir
        merge_gene_output = self.merge_gene_annot.output_dir
        self.api_anno.add_annotation(name=None, params=params,
                                     ref_anno_path=ref_anno_path,
                                     new_anno_path=new_anno_path,
                                     pfam_path=pfam_path, merge_tran_output=merge_tran_output,
                                     merge_gene_output=merge_gene_output)
    @time_count
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
        for file in os.listdir(self.altersplicing.output_dir):
            tmp_group_list = file.split("_vs_")
            group_a = tmp_group_list[0]
            group_b = tmp_group_list[1]
            params['group_detail'] = {}
            group = dict()
            for i in range(len(self.group_category)):
                value = self.group_detail[i].keys()
                if self.group_category[i] == group_a:
                    params['group_detail'][group_a] = value
                elif self.group_category[i] == group_b:
                    params['group_detail'][group_b] = value
            group[group_a] = "s1"
            group[group_b] = "s2"
            self.logger.info(params)
            outpath = os.path.join(self.altersplicing.output_dir, file)
            self.api_as.add_sg_splicing_rmats(params=params, major=True, group=group,
                                          ref_gtf=self.filecheck.option("gtf").prop["path"], name=None, outpath=outpath)

    @time_count
    def export_snp(self):
        self.api_snp = self.api.api("ref_rna.ref_snp")
        snp_anno = self.snp_rna.output_dir
        self.api_snp.add_snp_main(snp_anno)