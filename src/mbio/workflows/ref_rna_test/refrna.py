# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""本地基因组注释用工作流"""

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
            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq_dir"},  # Fastq文件夹
            {"name": "qc_quality", "type": "int", "default": 30},  # 质量剪切中保留的最小质量值
            {"name": "qc_length", "type": "int", "default": 50},  # 质量剪切中保留的最短序列长度

            {"name": "ref_genome", "type": "string", "default": "customer_mode"},  # 参考基因组
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组
            {"name": "gff", "type": "infile", "format": "ref_rna.reads_mapping.gff"},
            # Ensembl上下载的gff格式文件

            {"name": "nr_blast_evalue", "type": "float", "default": 1e-5},  # NR比对e值
            {"name": "string_blast_evalue", "type": "float", "default": 1e-5},  # String比对使用的e值
            {"name": "kegg_blast_evalue", "type": "float", "default": 1e-5},  # KEGG注释使用的e值
            {"name": "swissprot_blast_evalue", "type": "float", "default": 1e-5},  # Swissprot比对使用的e值
            {"name": "database", "type": "string", "default": 'go,nr,cog,kegg,swissprot'},
            # 全部五个注释

            {"name": "seq_method", "type": "string", "default": "Hisat"},  # 比对方法，Tophat or Hisat
            {"name": "strand_specific", "type": "bool", "default": False},
            # 当为PE测序时，是否有链特异性, 默认是False, 无特异性
            {"name": "strand_dir", "type": "string", "default": "None"},
            # 当链特异性时为True时，正义链为forward，反义链为reverse
            {"name": "map_assess_method", "type": "string", "default":
                "saturation,duplication,distribution,coverage"},
            # 比对质量评估分析
            {"name": "mate_std", "type": "int", "default": 50},  # 末端配对插入片段长度标准差
            {"name": "mid_dis", "type": "int", "default": 50},  # 两个成对引物间的距离中间值
            {"name": "result_reserved", "type": "int", "default": 1},  # 最多保留的比对结果数目

            {"name": "assemble_or_not", "type": "bool", "default": True},  # 是否进行拼接
            {"name": "assemble_method", "type": "string", "default": "cufflinks"},
            # 拼接方法，Cufflinks or Stringtie or None

            {"name": "express_method", "type": "string", "default": "featurecounts"},
            # 表达量分析手段: Htseq, Featurecount, Kallisto, RSEM
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},
            # 对照表
            {"name": "diff_method", "type": "string", "default": "edgeR"},
            # 差异表达分析方法
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            # {"name": "sort_type", "type": "string", "default": "pos"},  # 排序方法
            {"name": "exp_analysis", "type": "string", "default": "cluster,kegg_rich,go_rich"},
            # 差异表达富集方法,聚类分析, GO富集分析, KEGG富集分析

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
        self.json = self.get_json()
        self.filecheck = self.add_tool("ref_rna.filecheck.filecheck_ref")
        self.qc = self.add_module("denovo_rna.qc.quality_control")
        self.qc_stat_before = self.add_module("denovo_rna.qc.qc_stat")
        self.qc_stat_after = self.add_module("denovo_rna.qc.qc_stat")
        self.mapping = self.add_module("ref_rna.mapping.rnaseq_mapping")
        self.map_qc = self.add_module("ref_rna.mapping.ref_assessment")
        self.assembly = self.add_module("ref_rna_test.assembly.assembly")
        self.exp_ref = self.add_module("ref_rna_test.express.express")  # 需要修改
        self.exp_new_transcripts = self.add_module("ref_rna_test.express.express")  # 需要修改
        self.exp_new_genes = self.add_module("ref_rna_test.express.express")  # 需要修改
        self.exp_merged = self.add_module("ref_rna_test.express.express")  # 需要修改
        self.exp_diff = self.add_module("denovo_rna.express.diff_analysis")
        self.snp_rna = self.add_module("ref_rna.gene_structure.snp_rna")
        self.seq_abs = self.add_tool("ref_rna.annotation.transcript_abstract")
        self.annotation = self.add_module('denovo_rna.annotation.denovo_annotation')
        self.change_tool = self.add_tool("align.diamond.change_diamondout")
        # self.altersplicing = self.add_tool('ref_rna.gene_structure.altersplicing.rmats')
        self.network = self.add_module("ref_rna.ppinetwork_analysis")
        self.tf = self.add_tool("ref_rna.protein_regulation.TF_predict")
        self.sample_analysis = self.add_tool("ref_rna.express.sample_analysis")
        self.step.add_steps("qcstat", "mapping", "assembly", "annotation", "exp_ref", "exp_new_transcripts",
                            "exp_new_genes", "exp_merged", "map_stat",
                            "seq_abs", "transfactor_analysis", "network_analysis", "sample_analysis",
                            "altersplicing")

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
        if self.option('ref_genome') == "customer_mode":
            if not self.option("ref_genome_custom").is_set or not self.option("gff").is_set:
                raise OptionError("未设置参考基因组序列文件和gff格式文件")
        else:
            pass  # 此处加上对ref_genome选项的检查
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
        # if self.option()
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
            'control_file': self.option('control_file')
        }
        if self.option('ref_genome') == "customer_mode":
            opts.update({'gff': self.option('gff')})
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

    def run_seq_abs(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome": self.option("ref_genome"),
            "ref_genome_gff": self.option("gff")
        }
        self.seq_abs.set_options(opts)
        self.seq_abs.on('start', self.set_step, {'start': self.step.seq_abs})
        self.seq_abs.on('end', self.set_step, {'end': self.step.seq_abs})
        self.seq_abs.run()

    def test_run(self):
        self.seq_abs.on("end", self.run_blast)
        self.change_tool.on("end", self.end)
        self.run_seq_abs()
        super(RefrnaWorkflow, self).run()

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
        if 'go' in self.option('database') or 'nr' in self.option('database'):
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
        if 'cog' in self.option('database'):
            self.blast_string = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'string', 'evalue': self.option('string_blast_evalue')}
            )
            self.blast_string.set_options(blast_opts)
            self.blast_modules.append(self.blast_string)
            self.blast_string.on('end', self.set_output, 'stringblast')
            self.blast_string.run()
        if 'kegg' in self.option('database'):
            self.blast_kegg = self.add_module('align.diamond')
            blast_opts.update(
                {'database': 'kegg', 'evalue': self.option('kegg_blast_evalue')}
            )
            self.blast_kegg.set_options(blast_opts)
            self.blast_modules.append(self.blast_kegg)
            self.blast_kegg.on('end', self.set_output, 'keggblast')
            self.blast_kegg.run()
        self.on_rely(self.blast_modules, self.run_change_diamond)

    def run_change_diamond(self):
        opts = {
            "nr_out": self.blast_nr.option('outxml'),
            "kegg_out": self.blast_kegg.option('outxml'),
            "string_out": self.blast_string.option('outxml')
        }

        self.change_tool.set_options(opts)
        # self.change_tool.on("end",self.run_annotation)
        self.change_tool.run()

    def run_annotation(self):
        anno_opts = {
            'gene_file': self.seq_abs.option('gene_file'),
        }
        if 'go' in self.option('database'):
            anno_opts.update({
                'go_annot': True,
                'blast_nr_xml': self.change_tool.option('blast_nr_xml')
            })
        else:
            anno_opts.update({'go_annot': False})
        if 'nr' in self.option('database'):
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.change_tool.option('blast_nr_xml'),
            })
        else:
            anno_opts.update({'nr_annot': False})
        if 'kegg' in self.option('database'):
            anno_opts.update({
                'blast_kegg_xml': self.change_tool.option('blast_kegg_xml'),
            })
        if 'cog' in self.option('database'):
            anno_opts.update({
                'blast_string_xml': self.change_tool.option('blast_string_xml'),
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
        self.mapping.run()

    def run_assembly(self):
        self.logger.info("开始运行拼接步骤")
        opts = {
            "sample_bam_dir": self.mapping.option("bam_output"),
            "assemble_method": self.option("assemble_method")
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
        # 如果为平台自带的参考基因组
        if self.option("ref_genome") == "customer_mode":
            gtf_path = self.filecheck.option("gtf").prop["path"]
            ref_path = self.option("ref_genome_custom").prop["path"]
        else:
            gtf_path = ""
            ref_path = ""
        opts.update(
            {
                "ref_gtf": gtf_path,
                "ref_fa": ref_path
            }
        )
        self.assembly.set_options(opts)
        self.assembly.on("end", self.set_output, "assembly")
        self.assembly.on('start', self.set_step, {'start': self.step.assembly})
        self.assembly.on('end', self.set_step, {'end': self.step.assembly})
        self.assembly.run()

    def run_snp(self):
        opts = {
            "ref_genome_custom": self.option("ref_genome_custom"),
            "ref_genome": self.option("ref_genome"),
            "seq_method": self.option("fq_type"),
            "fastq_dir": self.qc.option("sickle_dir")
        }
        self.snp_rna.set_options(opts)
        self.snp_rna.on("end", self.set_output, "snp_rna")
        self.snp_rna.run()

    def run_map_assess(self):
        assess_method = self.option("map_assess_method") + ",stat"
        opts = {
            "bam": self.mapping.option("bam_output"),
            "analysis": assess_method
        }
        if self.option("ref_genome") == "customer_mode":
            bed_path = self.filecheck.option("bed").prop["path"]
        else:
            bed_path = ""
        opts.update = (
            {
                "bed": bed_path
            }
        )
        self.map_qc.set_options(opts)
        self.map_qc.on("end", self.set_output, "map_qc")
        self.map_qc.run()

    def run_exp(self, event):  # 表达量与表达差异模块
        gtf_type = event["data"]
        self.logger.info("开始运行表达量模块")
        if gtf_type == "ref":
            if self.option("ref_genome") == "customer_mode":
                gtf_path = self.filecheck.option("gtf").prop["path"]
            else:
                gtf_path = ""
        elif gtf_type == "new_transcripts":
            gtf_path = self.assembly.output_dir + "/assembly_newtranscripts/new_transcripts.gtf"
        elif gtf_type == "new_genes":
            gtf_path = self.assembly.output_dir + "/assembly_newtranscripts/new_genes.gtf"
        else:
            gtf_path = self.assembly.output_dir + "/assembly_newtranscripts/merged.gtf"
        opts = {
            "fq_type": self.option("fq_type"),
            "ref_gtf": gtf_path,
            "gtf_type": gtf_type,
            "express_method": self.option("express_method"),
            "sample_bam": self.mapping.option("bam_output"),
            "strand_specific": self.option("strand_specific"),
            "control_file": self.option("control_file"),
            "edger_group": self.option("group_table"),
            "method": self.option("diff_method"),
            "diff_ci": self.option("diff_ci"),
            "strand_dir": self.option("strand_dir")
        }
        if gtf_type == "ref":
            tool = self.exp_ref
            step = self.step.exp_ref
            output_dir_name = "express_ref"
        elif gtf_type == "new_transcripts":
            tool = self.exp_new_transcripts
            step = self.step.exp_new_transcripts
            output_dir_name = "express_new_transcripts"
        elif gtf_type == "new_genes":
            tool = self.exp_new_genes
            step = self.step.exp_new_genes
            output_dir_name = "express_new_genes"
        else:
            tool = self.exp_merged
            step = self.step.exp_merged
            output_dir_name = "express_merged"
        tool.set_options(opts)
        tool.on("end", self.set_output, output_dir_name)
        tool.on('start', self.set_step, {'start': step})
        tool.on('end', self.set_step, {'end': step})
        tool.run()

    def run_exp_diff(self):  # 表达差异富集分析
        if self.exp.diff_gene:
            exp_diff_opts = {
                'diff_fpkm': self.exp.option('diff_fpkm'),
                'analysis': self.option('exp_analysis')
            }
            if 'network' in self.option('exp_analysis'):
                exp_diff_opts.update({'gene_file': self.exp.option('gene_file')})
            elif 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'kegg_path': self.annotation.option('kegg_path'),
                    'diff_list_dir': self.exp.option('diff_list_dir')
                })
            elif 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'go_list': self.annotation.option('go_list'),
                    'diff_list_dir': self.exp.option('diff_list_dir'),
                    'all_list': self.exp.option('all_list'),
                    'go_level_2': self.annotation.option('go_level_2')
                })
            self.exp_diff.set_options(exp_diff_opts)
            self.exp_diff.on('end', self.set_output, 'exp_diff')
            self.exp_diff.run()
            self.final_tools.append(self.exp_diff)
        else:
            self.logger.info("输入文件数据量过小，没有检测到差异基因，差异基因相关分析将忽略")

    def run_network(self):
        if self.option("ref_genome") != "customer_mode":
            opts = {
                "diff_exp": self.exp.option("diff_list"),  #
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

    def run_altersplicing(self):
        if self.option("ref_genome") == "customer_mode":
            gtf_path = self.filecheck.option("gtf").prop["path"]
        else:
            gtf_path = ""
        opts = {
            "sequencing_library_type": self.option("sequencing_library_type"),
            "whether_to_find_novel_splice_sites": self.option("whether_to_find_novel_splice_sites"),
            "genome_annotation_file": gtf_path,
            "group_table": self.option("group_table"),
            "control_file": self.option("control_file")
        }
        if self.option("fq_type") == "PE":
            opts.update({"sequencing_type": "paired"})
        else:
            opts.update({"sequencing_type": "single"})
        self.altersplicing.set_options(opts)
        self.altersplicing.on("end", self.set_output, "altersplicing")
        self.altersplicing.on('start', self.set_step, {'start': self.step.altersplicing})
        self.altersplicing.on('end', self.set_step, {'end': self.step.altersplicing})
        self.altersplicing.run()

    def run_tf(self):
        opts = {
            "diff_gene_list": self.exp.option("diff_list"),
            "database": self.option("tf_database_type")
        }
        self.tf.set_options(opts)
        self.tf.on("end", self.set_output, "tf")
        self.tf.on('start', self.set_step, {'start': self.step.transfactor_analysis})
        self.tf.on('end', self.set_step, {'end': self.step.transfactor_analysis})
        self.tf.run()

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
        if event['data'] == 'express_ref':
            self.move2outputdir(obj.output_dir, 'express_ref')
            self.logger.info('未拼接express文件移动完成')
        if event['data'] == 'express_new_transcripts':
            self.move2outputdir(obj.output_dir, 'express_new_transcripts')
            self.logger.info('新转录本express文件移动完成')
        if event['data'] == 'express_new_genes':
            self.move2outputdir(obj.output_dir, 'express_new_genes')
            self.logger.info('新基因express文件移动完成')
        if event['data'] == 'express_merged':
            self.move2outputdir(obj.output_dir, 'express_merged')
            self.logger.info('拼接转录本express文件移动完成')
        if event['data'] == 'exp_diff':
            self.move2outputdir(obj.output_dir, 'express_diff')
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
        if event['data'] == 'altersplicing':
            self.move2outputdir(obj.output_dir, 'altersplicing')
            self.logger.info("altersplicing文件移动完成")

    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        # self.filecheck.on('end', self.end)
        self.filecheck.on('end', self.run_qc)
        """
        self.filecheck.on('end', self.run_seq_abs)
        self.seq_abs.on('end', self.run_blast)
        """
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_mapping)
        # self.mapping.on('end', self.end)
        self.mapping.on('end', self.run_assembly)
        # self.assembly.on('end', self.run_exp, "ref")
        self.mapping.on("end", self.run_exp, "ref")
        self.assembly.on("end", self.run_exp, "merged")
        self.on_rely([self.exp_ref, self.exp_merged], self.end)
        # self.exp_ref.on("end", self.end)
        # self.exp_ref.on("end", self.end)
        # self.on_rely([self.annotation, self.exp_ref], self.exp_diff)
        # self.exp_diff.on("end", self.end)

        # self.assembly.on('end', self.run_exp, "new_transcripts")
        # self.assembly.on('end', self.run_exp, "new_genes")
        # self.assembly.on('end', self.run_exp, "merged")
        # self.on_rely([self.exp_merged, self.exp_new_genes, self.exp_ref, self.exp_new_transcripts], self.end)
        # self.qc.on('end', self.run_seq_abs)
        # self.annotation.on('end', self.run_mapping)
        # self.qc.on('end',self.run_snp)
        # self.seq_abs.on("end",self.run_annotation)
        # self.on_rely([self.exp,self.annotation],self.exp_diff)
        # self.sample_analysis.on("end",self.end)
        # self.exp_diff.on("end",self.run_tf)
        # self.exp_diff.on("end",self.run_network)
        # self.on_rely([self.tf,self.network,self.sample_analysis,self.snp_rna,self.altersplicing],self.end)

        self.run_filecheck()
        super(RefrnaWorkflow, self).run()

    def end(self):
        super(RefrnaWorkflow, self).end()
