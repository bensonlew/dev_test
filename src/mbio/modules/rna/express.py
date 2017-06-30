# -*- coding: utf-8 -*-
# __author__ == konghualei
# last_modify: 2016.11.11

from biocluster.module import Module
import os
import re
from biocluster.core.exceptions import OptionError
import glob
import json
from mbio.files.sequence.file_sample import FileSampleFile
from mbio.packages.ref_rna.express.set_strand import set_strand
#from mbio.packages.ref_rna.express.change_sample_name import *
#from mbio.packages.ref_rna.express.gene_list_file_change import *
from mbio.packages.ref_rna.express.single_sample import *
from mbio.packages.ref_rna.express.cmp_ref_cls import *
from mbio.packages.ref_rna.express.express_distribution import distribution
import shutil

class ExpressModule(Module):
    def __init__(self,work_id):
        super(ExpressModule,self).__init__(work_id)
        options=[
            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组的gtf文件
            # {"name": "merged_gtf", "type": "infile", "format": "gene_structure.gtf"}, #拼接生成的merged.gtf文件
            # {"name": "cmp_gtf", "type": "infile", "format": "gene_structure.gtf"}, #gttcompare生成的annotated.gtf文件
            {"name":"new_gtf","type":"infile","format":"gene_structure.gtf"}, #新转录本的gtf文件  RNA流程为assembly模式时需要设置此参数
            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam_dir"},  # 所有样本的bam文件夹 适用于featureCoutns软件
            {"name": "fastq_dir", "type":"infile", "format":"sequence.fastq, sequence.fastq_dir"}, #所有样本的fastq_dir文件夹，适用于rsem, kallisto软件
            {"name": "ref_genome", "type": "string"}, # 参考基因组参数
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  #转录本的fasta 适用于kallisto软件
            {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "strand_dir", "type": "string"},  # 链特异性时选择正链, 默认不设置此参数"forward" "reverse"
            {"name": "express_method", "type": "string"},  # 选择计算表达量的方法 默认"rsem"
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹  不压缩的fq文件
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "is_duplicate", "type":"bool"}, #是否有生物学重复
            {"name": "diff_rate", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts", "type": "int", "default": 20},  # 离散值估计检验的最小计数值
            {"name": "control_file", "type": "infile", "format": "sample.control_table"},  # 对照组文件，格式同分组文件
            {"name": "edger_group", "type": "infile", "format": "sample.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # pvalue显著性水平
            {"name": "diff_fdr_ci","type":"float",'default':0.05}, #padjust显著性水平
            {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
            {"name": "genes_all_list", "type": "outfile", "format": "rna.gene_list"},  # 全部基因名称文件
            {"name":"genes_diff_list","type":"outfile","format":"rna.gene_list"}, #差异基因/转录本文件
            {"name": "trans_all_list", "type": "outfile", "format": "rna.gene_list"},  # 全部基因名称文件
            {"name": "trans_diff_list", "type": "outfile", "format": "rna.gene_list"},  # 差异基因/转录本文件
            {"name": "method", "type": "string", "default": "edgeR"},  # 分析差异基因选择的方法
            # {"name": "change_sample_name", "type": "bool"}, #选择是否更改样本的名称
            {"name": "exp_way", "type": "string", "default": "fpkm"}, #默认选择fpkm进行表达量的计算  rsem (fpkm,tpm) featurecounts(fpkm tpm all)
            {"name": "genes_diff_list_dir", "type": "outfile", "format": "rna.diff_stat_dir"}, #差异分组对应的差异基因
            {"name": "trans_diff_list_dir", "type": "outfile", "format": "rna.diff_stat_dir"}, # 差异分组对应的差异基因
            {"name": "genes_diff_fpkm","type":"outfile","format":"rna.express_matrix"}, #差异基因的fpkm表
            {"name": "diff_list_dir", "type": "outfile", "format": "rna.diff_stat_dir"},
            {"name": "trans_diff_fpkm", "type": "outfile", "format": "rna.express_matrix"},  # 差异转录本的fpkm表
            {"name":  "fc", "type":"float","default":2}, #设置fc的值
            {"name": "is_express_assembly", "type": "string", "default": "assembly"},  # 表达量RNA流程分析或转录组RNA流程分析
            {"name":"is_class_code","type":"bool","default":True}, #是否对merged gtf文件生成class_code信息
            {"name":"network_diff_list","type":"outfile","format":"rna.ppi"}, #为生成蛋白调控网络的diff_list文件
        ]
        self.add_option(options)
        self.count_output = 0

    def check_options(self):
        self.logger.info(self.option("express_method"))
        if self.option("express_method").lower() == 'featurecounts':
            self.step.add_steps("combine_gtf", "featurecounts", "fpkm_diffRexp", "transcript_abstract", "tpm_diffRexp", "fpkm_corr", "fpkm_pca","tpm_corr","tpm_pca")
            self.featurecounts=self.add_tool("rna.featureCounts")
            self.fpkm_corr = self.add_tool("statistical.correlation")
            self.tpm_corr = self.add_tool("statistical.correlation")
            self.fpkm_pca = self.add_tool("meta.beta_diversity.pca")
            self.tpm_pca = self.add_tool("meta.beta_diversity.pca")
            self.fpkm_diffRexp = self.add_tool("rna.diff_exp")
            self.tpm_diffRexp = self.add_tool('rna.diff_exp')
        elif self.option("express_method").lower() == 'rsem':
            self.step.add_steps('combine_gtf',"rsem", "mergersem", "mergersem1", "genes_diffRexp", "transcript_abstract",'genes_diffRexp_ref','trans_diffRexp_ref', "trans_diffRexp", "trans_corr", "trans_pca", "genes_corr", "genes_pca")
            self.genes_corr = self.add_tool("statistical.correlation")
            self.genes_pca = self.add_tool("meta.beta_diversity.pca")
            self.genes_diffRexp = self.add_tool("rna.diff_exp")
            self.trans_corr = self.add_tool("statistical.correlation")
            self.trans_pca = self.add_tool("meta.beta_diversity.pca")
            self.trans_diffRexp = self.add_tool("rna.diff_exp")
            self.genes_diffRexp_ref = self.add_tool("rna.diff_exp")
            self.trans_diffRexp_ref = self.add_tool("rna.diff_exp")

        elif self.option("express_method").lower() == 'kallisto':
            self.step.add_steps("kallisto", "mergekallisto", "transcript_abstract", "trans_diffRexp", "trans_corr", "trans_pca")
        self.tool_lists = []
        self.samples = []
        self.sumtool = []
        self.diff_count = 0
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        if self.option("is_express_assembly") == 'assembly':
            if not self.option("new_gtf").is_set:
                raise Exception("RNA分析流程为assembly模式，请设置new_gtf参数！")
        #     self.gtf_path = self.work_dir + "/ref_new.gtf"
        #     cmd = """cat {} {} > {}""".format(self.option("ref_gtf").prop['path'], self.option("new_gtf").prop['path'],
        #                                       self.gtf_path)
        #     os.system(cmd)
        # else:
        #     self.gtf_path = self.option("ref_gtf").prop['path']
        return True

    def combine_gtf_run(self):
        self.combine_gtf = self.add_tool("rna.cat")
        self.logger.info("开始cat ref和new gtf文件！")
        opts = {
            "file1":self.option("ref_gtf").prop['path'],
            "file2":self.option("new_gtf").prop['path']
        }
        self.combine_gtf.set_options(opts)
        self.combine_gtf.on("end", self.set_step, {'end': self.step.combine_gtf})
        self.combine_gtf.on('end', self.transcript_abstract_run)
        self.combine_gtf.run()

    def transcript_abstract_run(self):
        self.logger.info("开始进行转录本的提取")
        if self.option("is_express_assembly") == 'assembly':
            self.gtf_path = self.combine_gtf.work_dir + "/ref_new.gtf"
        else:
            self.gtf_path = self.option('ref_gtf')
        self.transcript_abstract = self.add_tool("annotation.transcript_abstract")
        tool_opt = {
            "ref_genome_custom":self.option("ref_genome_custom").prop['path'],
        }
        tool_opt["ref_genome_gtf"]=self.gtf_path
        # if self.option("is_express_assembly") == "assembly":
        #     tool_opt["ref_genome_gtf"] = self.option("merged_gtf")
        # if self.option("is_express_assembly") == 'express':
        #     tool_opt["ref_genome_gtf"] =  self.option("ref_gtf")
        self.transcript_abstract.set_options(tool_opt)
        self.transcript_abstract.on("end",self.set_step, {'end': self.step.transcript_abstract})
        if self.option("express_method").lower() == 'rsem':
            self.transcript_abstract.on("end", self.rsem_run)
        if self.option("express_method").lower() == 'featurecounts':
            self.transcript_abstract.on('end',self.featurecounts_run)
        self.transcript_abstract.run()


    def featurecounts_run(self):
        n=0
        """发送信息给前端"""
        print "featureCounts开始计算表达量！"
        self.step.featurecounts.start()
        self.step.update()
        print self.option('fq_type')
        tool_opt = {
            "strand_specific": self.option("strand_specific"),
            "cpu":10,
            "max_memory": "100G",
            "bam": self.option("sample_bam"),
            "fq_type": self.option('fq_type'),
            "exp_way": self.option("exp_way")
        }

        if self.option("is_express_assembly") == "assembly":
            tool_opt["ref_gtf"] =self.gtf_path
        if self.option("is_express_assembly") == "express":
            tool_opt["ref_gtf"] = self.option("ref_gtf")

        if self.option("is_duplicate"):
            tool_opt.update({
                "is_duplicate":self.option("is_duplicate"),
                "edger_group":self.option("edger_group")
            })
        if self.option("strand_specific"):
             tool_opt.update({
                 "strand_dir":self.option("strand_dir")
             })

        self.featurecounts.set_options(tool_opt)
        self.featurecounts.on('end', self.set_output, 'featurecounts')
        self.featurecounts.on('end', self.set_step, {'end': self.step.featurecounts})
        self.featurecounts.run()

    def rsem_run(self):
        n=0
        self.logger.info("开始打印self.gtf_path路径")
        self.logger.info(self.gtf_path)

        self.step.rsem.start()
        self.step.update()
        if os.path.isdir(self.option("fastq_dir").prop['path']):
            self.logger.info("检验fastq_dir成功！")
            #先只添加用户自定义，上传fasta文件，不涉及平台参考库
            self.file_get_list()
            # if self.option("is_express_assembly") == "assembly":
            #     tool_opt = {
            #             "ref_gtf": self.option("merged_gtf")
            #
            #         }
            # if self.option("is_express_assembly") == "express":
            #     tool_opt = {
            #         "ref_gtf":self.option("ref_gtf")
            #     }
            # tool_opt = {"ref_gtf": self.gtf_path}
            transcript_fa = self.transcript_abstract.output_dir+"/exons.fa"
            if not os.path.exists(transcript_fa):
                raise Exception("rsem Tool 没有设置转录本fa文件!")
            tool_opt = {
                "ref_gtf":self.gtf_path,
                "transcript_fa":transcript_fa
            }
            # tool_opt["transcript_fa"]=self.option("ref_genome_custom")
            self.logger.info(tool_opt["transcript_fa"])
            if self.option("fq_type") =="SE":
                for sam, single in self.samples.items():
                    tool_opt.update({
                        "fq_type": "SE",
                        "fq_s": os.path.join(self.option("fastq_dir").prop['path'], single),
                        "sample_name": sam
                    })
                    self.rsem = self.add_tool("rna.rsem")
                    self.rsem.set_options(tool_opt)
                    self.logger.info(tool_opt)
                    self.tool_lists.append(self.rsem)
                    self.rsem.run()
            elif self.option("fq_type") == "PE":
                self.logger.info(self.samples)
                self.logger.info("haha")
                for sam, single in self.samples.items():
                        l_reads=os.path.join(self.option("fastq_dir").prop['path'], single["l"])
                        r_reads=os.path.join(self.option("fastq_dir").prop['path'], single["r"])
                        tool_opt.update({
                            "fq_type": "PE",
                            "fq_l": l_reads,
                            "fq_r": r_reads,
                            "sample_name": sam #样本名称
                        })
                        self.logger.info(l_reads)
                        self.logger.info(r_reads)
                        self.rsem = self.add_tool("rna.rsem")
                        if self.option("strand_specific") == True:
                            tool_opt.update({
                                "strand_dir": self.strand_dir
                            })
                        self.rsem.set_options(tool_opt)
                        self.logger.info(tool_opt)
                        self.tool_lists.append(self.rsem)
                        self.rsem.run()
        self.logger.info(self.tool_lists)
        self.logger.info(len(self.tool_lists))
        if len(self.tool_lists) != 1:
            self.on_rely(self.tool_lists, self.set_output, "rsem")
            # for i in self.tool_lists:
                # i.run()
        else:
            self.rsem.on('end', self.set_output, 'rsem') #得到单个样本的表达量，无法进行下游分析
            # self.rsem.run()
            self.logger.info("单个样本只能计算表达量，无法进行样本间相关性评估、venn图和差异分析！")

    def file_get_list(self):
        #获得样本信息
        list_path = os.path.join(self.option("fastq_dir").prop['path'],"list.txt")
        import time
        start = time.time()
        file_sample = FileSampleFile()
        self.logger.info(list_path)
        file_sample.set_path(list_path)
        self.samples = file_sample.get_list()
        self.logger.info(self.samples)
        end = time.time()
        duration=end-start
        self.logger.info("file_get_list函数共耗时{}s".format(str(duration)))
        if self.samples!=None:
            if self.samples.keys() != None and self.samples.values() != None:
                pass
            else:
                raise Exception("根据list.txt文件获取样本名称失败！")

    def mergersem_run(self):
        self.mergersem = self.add_tool("rna.merge_rsem")
        dir_path = self.output_dir + '/rsem'
        # dir_path = "/mnt/ilustre/users/sanger-dev/workspace/20170621/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_12/Expresstest1/output/rsem/rsem"
        self.logger.info(dir_path)
        if not os.path.exists(dir_path):
            raise Exception("{}文件不存在，请检查！".format(dir_path))
        opts = {
            "rsem_files": dir_path,
            "exp_way": self.option("exp_way"),
            "is_duplicate": self.option("is_duplicate"),
            "gtf_ref": self.option("ref_gtf")
        }
        # self.gtf_path = "/mnt/ilustre/users/sanger-dev/workspace/20170620/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_9/Expresstest1/ref_new.gtf"
        if self.option("is_class_code"):
            opts["is_class_code"]=self.option("is_class_code")
            opts["gtf_merged"] = self.gtf_path
            # opts["gtf_merged"] = "/mnt/ilustre/users/sanger-dev/workspace/20170623/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_merge_1/Expresstest2/ref_new.gtf"
            # opts["gtf_merged"] = "/mnt/ilustre/users/sanger-dev/workspace/20170622/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_7/Expresstest2/ref_new.gtf"
            # opts["gtf_merged"] = self.option("merged_gtf").prop['path']
            # opts["gtf_merged"] = "/mnt/ilustre/users/sanger-dev/workspace/20170620/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_9/Expresstest1/ref_new.gtf"
        if self.option("is_duplicate"):
            opts.update({
                "edger_group": self.option("edger_group")
            })
        self.logger.info(opts)
        print opts
        self.mergersem.set_options(opts)
        self.mergersem.on('end', self.set_output, 'mergersem')
        self.mergersem.on('end', self.set_step, {'end': self.step.mergersem})
        self.mergersem.run()

    def mergersem1_run(self):
        self.mergersem1 = self.add_tool("rna.merge_rsem")
        dir_path = self.output_dir+ '/rsem'
        # dir_path = "/mnt/ilustre/users/sanger-dev/workspace/20170621/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_12/Expresstest1/output/rsem/rsem"
        self.logger.info(dir_path)
        if not os.path.exists(dir_path):
            raise Exception("{}文件不存在，请检查！".format(dir_path))
        if self.option("exp_way") == 'fpkm':
            exp_way = 'tpm'
        else:
            exp_way = 'fpkm'
        opts = {
            "rsem_files": dir_path,
            "exp_way": exp_way,
            "is_duplicate": self.option("is_duplicate"),
            "gtf_ref": self.option("ref_gtf")
        }
        # self.gtf_path = "/mnt/ilustre/users/sanger-dev/workspace/20170620/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_9/Expresstest1/ref_new.gtf"
        if self.option("is_class_code"):
            opts["is_class_code"] = self.option("is_class_code")
            # opts["gtf_merged"] = "/mnt/ilustre/users/sanger-dev/workspace/20170622/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_7/Expresstest2/ref_new.gtf"
            opts["gtf_merged"] = self.gtf_path
            # opts["gtf_merged"] = self.option("merged_gtf").prop['path']
            # opts["gtf_merged"] = "/mnt/ilustre/users/sanger-dev/workspace/20170620/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_9/Expresstest1/ref_new.gtf"
        if self.option("is_duplicate"):
            opts.update({
                "edger_group": self.option("edger_group")
            })
        self.logger.info(opts)
        print opts
        self.mergersem1.set_options(opts)
        self.mergersem1.on('end', self.set_output, 'mergersem1')
        self.mergersem1.on('end', self.set_step, {'end': self.step.mergersem1})
        self.mergersem1.run()

    def sample_correlation(self):
        if self.option("express_method").lower() == 'rsem':
            self.genes_corr.set_options({"fpkm": self.rsem_genes_fpkm})
            self.genes_corr.on("end", self.set_output, "genes_correlation")
            self.genes_corr.run()
            self.trans_corr.set_options({"fpkm": self.rsem_transcripts_fpkm})
            self.trans_corr.on("end", self.set_output,"trans_correlation")
            self.trans_corr.run()
            self.genes_pca.set_options({"otutable": self.rsem_genes_fpkm})
            self.genes_pca.on("end", self.set_output, "genes_pca")
            if self.get_list() > 2:
                self.genes_pca.run()
            self.trans_pca.set_options({"otutable": self.rsem_transcripts_fpkm})
            self.trans_pca.on("end", self.set_output, "trans_pca")
            if self.get_list() > 2:
                self.trans_pca.run()
        elif self.option("express_method").lower() == 'featurecounts':
            """fpkm、tpm表达量相关性/pca"""
            self.fpkm_corr.set_options({"fpkm": self.feature_fpkm_path})
            self.fpkm_pca.set_options({"otutable": self.feature_fpkm_path})
            self.fpkm_corr.on("end",self.set_output,"fpkm_correlation")
            self.fpkm_pca.on("end",self.set_output,"fpkm_pca")
            self.fpkm_corr.run()
            self.tpm_corr.set_options({"fpkm": self.feature_tpm_path})
            self.tpm_pca.set_options({"otutable": self.feature_tpm_path})
            self.tpm_corr.on("end",self.set_output,"tpm_correlation")
            self.tpm_pca.on("end",self.set_output,"tpm_pca")
            self.tpm_corr.run()
            if self.get_list() > 2:
                self.fpkm_pca.run()
                self.tpm_pca.run()
            else:
                self.logger.info("样本数目小于等于2，无法进行样本间pca分析！")

    def sample_venn_table(self, otutable, venn_group_path, venn_name):
        if not os.path.exists(otutable):
            raise Exception("{}文件不存在！".format(otutable))
        if not os.path.exists(venn_group_path):
            raise Exception("{}文件不存在！".format(venn_group_path))
        opts={
            "otutable": otutable,
            "group_table": venn_group_path
        }
        self.sample_venn.set_options(opts)
        self.sample_venn.on('end', self.set_output, venn_name)
        self.sample_venn.run()

    def diff_Rexp_run(self, genes_count_path=None, trans_count_path=None, genes_fpkm_path=None,
                          trans_fpkm_path=None, ref_genes_count_path=None,
                          ref_trans_count_path=None, ref_genes_fpkm_path=None, ref_trans_fpkm_path=None, ref_all=False):
        self.logger.info('开始进行差异表达分析！')
        tool_opt = {
            "control_file": self.option('control_file'),
            "gname": "group",
            "method": self.option("method"),
            'diff_ci': self.option("diff_ci"),
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "pvalue_padjust": "padjust",  # 工作流此参数写死
            "fc": self.option("fc")
        }
        genes_opt = tool_opt
        trans_opt = tool_opt
        if ref_all:
            ref_genes_opt = tool_opt
            ref_trans_opt = tool_opt
        self.diff_count += 1
        if self.option("express_method").lower() == 'rsem':

            genes_opt["count"] = genes_count_path
            genes_opt["fpkm"] = genes_fpkm_path
            if self.option('edger_group').is_set:
                edger_group_path = self.option('edger_group')
                genes_opt['edger_group'] = edger_group_path
            self.genes_diffRexp.set_options(genes_opt)
            self.genes_diffRexp.on('end', self.set_output, 'genes_diff')
            self.genes_diffRexp.on("end", self.set_step, {"end": self.step.genes_diffRexp})
            self.genes_diffRexp.on("end", self.sample_correlation)
            self.logger.info("开始打印ref_genes_opt参数！")
            self.logger.info(ref_genes_opt)
            self.genes_diffRexp.run()
            trans_opt["count"] = trans_count_path
            trans_opt["fpkm"] = trans_fpkm_path
            if self.option('edger_group').is_set:
                edger_group_path = self.option('edger_group')
                trans_opt["edger_group"] = edger_group_path
            self.trans_diffRexp.set_options(trans_opt)
            self.trans_diffRexp.on('end', self.set_output, 'trans_diff')
            self.trans_diffRexp.on("end", self.set_step, {"end": self.step.trans_diffRexp})
            # if self.option("express_method").lower() == 'featurecounts':
            #     self.genes_diffRexp.on("end", self.sample_correlation)
            self.trans_diffRexp.run()
            self.logger.info("计算转录本差异分析成功！")
            if ref_all:
                ref_trans_opt = tool_opt
                ref_genes_opt = tool_opt
                ref_genes_opt["count"] = ref_genes_count_path
                ref_genes_opt["fpkm"] = ref_genes_fpkm_path
                if self.option('edger_group').is_set:
                    edger_group_path = self.option('edger_group')
                    ref_genes_opt['edger_group'] = edger_group_path
                self.logger.info("开始打印ref_trans_opt参数！")
                self.logger.info(ref_trans_opt)
                self.genes_diffRexp_ref.set_options(ref_genes_opt)
                self.genes_diffRexp_ref.on('end', self.set_output, 'genes_ref_diff')
                self.genes_diffRexp_ref.on("end", self.set_step, {"end": self.step.genes_diffRexp_ref})
                # self.genes_diffRexp_ref.on("end", self.sample_correlation)
                self.genes_diffRexp_ref.run()
                self.logger.info("计算ref基因差异分析成功！")
                ############ref转录本的差异分析
                ref_trans_opt["count"] = ref_trans_count_path
                ref_trans_opt["fpkm"] = ref_trans_fpkm_path
                if self.option('edger_group').is_set:
                    edger_group_path = self.option('edger_group')
                    ref_trans_opt["edger_group"] = edger_group_path
                self.logger.info("开始打印ref_trans_opt参数！")
                self.logger.info(ref_trans_opt)
                self.trans_diffRexp_ref.set_options(ref_trans_opt)
                self.trans_diffRexp_ref.on('end', self.set_output, 'trans_ref_diff')  # 这步的设置为什么不管用呢？
                self.trans_diffRexp_ref.on("end", self.set_step, {"end": self.step.trans_diffRexp_ref})
                self.trans_diffRexp_ref.run()
                self.logger.info("计算ref转录本差异分析成功！")

        if self.option("express_method").lower() == 'featurecounts':
            fpkm_opt = tool_opt
            fpkm_opt["count"] = self.feature_count_path
            fpkm_opt["fpkm"] = self.feature_fpkm_path

            tpm_opt = tool_opt
            tpm_opt["count"] = self.feature_count_path
            tpm_opt["fpkm"] = self.feature_tpm_path

            if self.option('edger_group').is_set:
                edger_group_path = self.option('edger_group')
                fpkm_opt['edger_group'] = edger_group_path
                tpm_opt['edger_group'] = edger_group_path

            self.fpkm_diffRexp.set_options(fpkm_opt)
            self.fpkm_diffRexp.on('end', self.set_output, 'genes_diff_fpkm')
            self.tpm_diffRexp.on('end', self.set_output, 'genes_diff_tpm')

            self.fpkm_diffRexp.on("end", self.set_step, {"end": self.step.fpkm_diffRexp})
            self.fpkm_diffRexp.on("end", self.sample_correlation)

            self.fpkm_diffRexp.run()
            self.logger.info("准备计算fpkm差异分析！")

            self.tpm_diffRexp.set_options(tpm_opt)
            self.tpm_diffRexp.on("end", self.set_step, {"end": self.step.tpm_diffRexp})
            # self.tpm_diffRexp.on("end", self.sample_correlation)

            self.tpm_diffRexp.run()
            self.logger.info("准备计算tpm差异分析！")

    def diff_Rexp_ref_run(self,genes_count_path=None,trans_count_path=None,genes_fpkm_path=None,trans_fpkm_path=None):
        self.logger.info('开始进行差异表达分析！')
        tool_opt = {
            "control_file": self.option('control_file'),
            "gname": "group",
            "method": self.option("method"),
            'diff_ci': self.option("diff_ci"),
            "diff_fdr_ci": self.option("diff_fdr_ci"),
            "pvalue_padjust": "padjust",  # 工作流此参数写死
            "fc": self.option("fc")
        }
        trans_opt = tool_opt
        genes_opt = tool_opt
        genes_opt["count"] = genes_count_path
        genes_opt["fpkm"] = genes_fpkm_path
        if self.option('edger_group').is_set:
            edger_group_path = self.option('edger_group')
            genes_opt['edger_group'] = edger_group_path
        self.genes_diffRexp_ref.set_options(genes_opt)
        self.genes_diffRexp_ref.on('end', self.set_output, 'genes_ref_diff')
        self.genes_diffRexp_ref.on("end", self.set_step, {"end": self.step.genes_diffRexp_ref})
        # self.genes_diffRexp_ref.on("end", self.sample_correlation)
        self.genes_diffRexp_ref.run()
        self.logger.info("计算ref基因差异分析成功！")
        ############ref转录本的差异分析
        trans_opt["count"] = trans_count_path
        trans_opt["fpkm"] = trans_fpkm_path
        if self.option('edger_group').is_set:
            edger_group_path = self.option('edger_group')
            trans_opt["edger_group"] = edger_group_path
        self.trans_diffRexp_ref.set_options(trans_opt)
        self.trans_diffRexp_ref.on('end', self.set_output, 'trans_ref_diff')   #这步的设置为什么不管用呢？
        self.trans_diffRexp_ref.on("end", self.set_step, {"end": self.step.trans_diffRexp_ref})
        self.trans_diffRexp_ref.run()
        self.logger.info("计算ref转录本差异分析成功！")

    def get_list(self):
        if self.option("express_method").lower() == "featurecounts":
            list_path = self.option("sample_bam").prop['path']
            sample_number = len(os.listdir(list_path))
        else:
            list_path = self.option("fastq_dir").prop['path']
            sample_number = len(os.listdir(list_path))-1   #含有list.txt文件
        return sample_number

    def linkdir(self, dirpath, dirname, output_dir):
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i)for i in allfiles]
        newfiles = [os.path.join(newdir, i)for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def set_output(self, event):
        obj=event['bind_object']
        self.count_output += 1
        self.logger.info(event['data'])
        self.logger.info("设置输出结果")
        if event['data'] == "featurecounts":
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("开始进行差异分析！")
            if self.get_list() <= 1:
                self.logger.info("单个样本只能计算表达量，无法进行样本间相关性评估和差异分析！")
            else:
                self.feature_count_path = self.featurecounts.output_dir + "/count.xls"
                if self.option("exp_way") == 'fpkm':
                    self.feature_fpkm_path = self.featurecounts.output_dir + "/fpkm_tpm.fpkm.xls"
                    self.diff_Rexp_run(genes_count_path = self.feature_count_path, genes_fpkm_path = self.feature_fpkm_path)
                if self.option("exp_way") == "tpm":
                    self.feature_fpkm_path = self.featurecounts.output_dir + "/fpkm_tpm.tpm.xls"
                    self.diff_Rexp_run(genes_count_path = self.feature_count_path, genes_fpkm_path = self.feature_fpkm_path)
                if self.option("exp_way") == 'all':
                    self.feature_fpkm_path = self.featurecounts.output_dir + "/fpkm_tpm.fpkm.xls"
                    self.feature_tpm_path = self.featurecounts.output_dir + "/fpkm_tpm.tpm.xls"
                    self.diff_Rexp_run()
                    self.logger.info("tpm, fpkm开始进行差异分析！")
        elif event['data'] == 'rsem':
            """ 除掉'gene:'、'transcript:' 信息"""
            # os.system(""" sed -i "s/gene://g" %s """ % (self.output_dir+"/class_code"))
            # os.system(""" sed -i "s/transcript://g" %s """ % (self.output_dir+"/class_code"))
            rsem_path = os.path.join(self.output_dir, 'rsem')
            if not os.path.exists(rsem_path):
                os.mkdir(rsem_path)
            ss=0
            self.logger.info(self.tool_lists)
            for tool in self.tool_lists:
                for files in os.listdir(tool.output_dir):
                    ss+=1
                    if re.search(r'genes.results', files):
                        os.link(os.path.join(tool.output_dir, files), os.path.join(rsem_path, files))
                    if re.search(r'isoforms.results', files):
                        os.link(os.path.join(tool.output_dir, files), os.path.join(rsem_path, files))
            self.linkdir(rsem_path, 'rsem1', self.output_dir)  #设置rsem的结果目录
            if self.get_list() > 1:
                self.mergersem_run()
            else:
                self.logger.info("样本数目小于等于2，无法进行样本间分析、差异分析、差异统计，表达量分析已结束！")
        elif event['data'] == "mergersem":
            def check_class_code():
                if os.path.exists(obj.work_dir+"/class_code"):
                    return True
            class_code = check_class_code()
            rsem_path = self.output_dir + "/rsem"
            # old_rsem_path = self.output_dir + "/oldrsem"
            # if not os.path.exists(old_rsem_path):
            #     os.mkdir(old_rsem_path)
            ss = 0
            for files in os.listdir(obj.output_dir):
                files_path = os.path.join(obj.output_dir, files)
                new_files_path = os.path.join(self.output_dir+"/rsem",files)
                os.link(files_path, new_files_path)
            # for f in os.listdir(obj.work_dir+"/"+"oldrsem"):
            #     os.link(obj.work_dir+"/"+"oldrsem/"+f, old_rsem_path+"/"+f)
            #     self.logger.info("设置gene count 和 fpkm 输出结果成功！")
            self.rsem_genes_count = self.output_dir+'/rsem/genes.counts.matrix'
            self.rsem_transcripts_count = self.output_dir + '/rsem/transcripts.counts.matrix'
            self.rsem_genes_count_ref = obj.work_dir + '/ref_rsem/genes.counts.matrix'
            self.rsem_transcripts_count_ref = obj.work_dir + '/ref_rsem/transcripts.counts.matrix'
            if self.option("exp_way").lower() == 'fpkm':
                self.rsem_genes_fpkm = self.output_dir + '/rsem/genes.TMM.fpkm.matrix'
                self.rsem_transcripts_fpkm = self.output_dir + '/rsem/transcripts.TMM.fpkm.matrix'
                self.rsem_genes_fpkm_ref = obj.work_dir + "/ref_rsem/genes.TMM.fpkm.matrix"
                self.rsem_transcript_fpkm_ref = obj.work_dir + '/ref_rsem/transcripts.TMM.fpkm.matrix'
            elif self.option("exp_way").lower() == 'tpm':
                self.rsem_genes_fpkm = self.output_dir + '/rsem/genes.TMM.EXPR.matrix'
                self.rsem_transcripts_fpkm = self.output_dir + '/rsem/transcripts.TMM.EXPR.matrix'
                self.rsem_genes_fpkm_ref = obj.work_dir + "/ref_rsem/genes.TMM.EXPR.matrix"
                self.rsem_transcript_fpkm_ref = obj.work_dir + '/ref_rsem/transcripts.TMM.EXPR.matrix'
            # self.diff_Rexp_run(genes_count_path=self.rsem_genes_count, genes_fpkm_path=self.rsem_genes_fpkm,trans_count_path = self.rsem_transcripts_count, trans_fpkm_path = self.rsem_transcripts_fpkm)
            self.diff_Rexp_run(genes_count_path=self.rsem_genes_count, genes_fpkm_path=self.rsem_genes_fpkm,
                               trans_count_path=self.rsem_transcripts_count, trans_fpkm_path=self.rsem_transcripts_fpkm,
                               ref_genes_count_path=self.rsem_genes_count_ref,
                               ref_trans_count_path=self.rsem_transcripts_count_ref,
                               ref_genes_fpkm_path=self.rsem_genes_fpkm_ref,
                               ref_trans_fpkm_path=self.rsem_transcript_fpkm_ref, ref_all=True)
            # self.mergersem1_run()
        elif event['data'] == 'mergersem1':
            self.logger.info("开始设置mergersem1分析!")
            if not os.path.exists(self.output_dir+"/rsem1"):
                os.mkdir(self.output_dir+"/rsem1")
            for files in os.listdir(obj.output_dir):
                files_path = os.path.join(obj.output_dir, files)
                new_files_path = os.path.join(self.output_dir + "/rsem1", files)
                os.link(files_path, new_files_path)
            self.logger.info("设置mergersem1分析完毕!")
            self.logger.info("haha")
            self.logger.info("heihei")
        # elif re.search(r'genes_diff', event['data']) or re.search(r'trans_diff', event['data']):
        elif event['data'] == 'genes_diff' or event['data'] == 'trans_diff':
            self.logger.info("开始设置差异分析结果目录！")
            if self.option('express_method') == 'rsem':
                # def module_output_diff_dir_name(diff_dir_name='diff')
                diff_dir_name='diff'
                if not os.path.exists(self.output_dir+'/{}'.format(diff_dir_name)):
                    os.mkdir(self.output_dir+'/{}'.format(diff_dir_name))
                self.linkdir(obj.output_dir, event['data'], self.output_dir+'/{}'.format(diff_dir_name))
                stat_path = self.output_dir+'/{}/'.format(diff_dir_name)+event['data']+"/diff_stat_dir"
                if not os.path.exists(stat_path):
                    os.mkdir(stat_path)
                for files in os.listdir(self.output_dir+"/{}/".format(diff_dir_name)+event['data']):  #把差异统计表移到diff_stat_dir文件夹
                    if re.search(r'edgr_stat.xls',files):
                        os.system("""mv %s %s"""%(self.output_dir+"/{}/".format(diff_dir_name)+event['data']+"/"+files, stat_path+"/"+files))
                if os.path.exists(obj.work_dir+"/diff_list"):
                    os.link(obj.work_dir+"/diff_list", self.output_dir+"/{}/".format(diff_dir_name)+event['data']+"/diff_list")
                    # self.add_gene_id(self.output_dir + "/{}/".format(diff_dir_name) + event['data'] + "/diff_list",
                    #                      self.output_dir + "/{}/".format(diff_dir_name) + event['data'] )
                if os.path.exists(os.path.join(obj.work_dir, 'diff_list_dir')):
                    self.linkdir(obj.work_dir+'/diff_list_dir', 'diff_list_dir', self.output_dir+'/{}/'.format(diff_dir_name)+event['data'])
                    network_file = os.listdir(self.output_dir + "/{}/".format(diff_dir_name) + event["data"] + "/diff_list_dir")[0]
                    self.add_gene_id(os.path.join(self.output_dir + "/{}/".format(diff_dir_name) + event["data"] + "/diff_list_dir", network_file),
                                     self.output_dir + "/{}/".format(diff_dir_name) + event["data"], 'network_{}'.format(network_file))
                    if os.path.exists(self.output_dir + "/{}/".format(diff_dir_name) + event["data"] + '/network_{}'.format(network_file)):
                        self.option("network_diff_list",self.output_dir + "/{}/".format(diff_dir_name) + event["data"] + '/network_{}'.format(network_file))
                        self.logger.info("已经设置network_diff_list参数！")
                    else:
                        raise Exception("没有生成对应的差异基因network_{}".format(network_file))
                else:
                    self.logger.info("{}分析没有生成diff_list_dir文件夹！".format(event['data']))
                self.logger.info("差异分析结果目录设置成功！")
            # elif re.search(r'genes_ref_diff', event['data']) or re.search(r'trans_ref_diff', event['data']):
        elif event['data'] == 'genes_ref_diff' or event['data'] == 'trans_ref_diff':
                self.logger.info("开始设置差异分析结果目录！")
                # def module_output_diff_dir_name(diff_dir_name='diff')
                diff_dir_name = 'ref_diff'
                if not os.path.exists(self.output_dir + '/{}'.format(diff_dir_name)):
                    os.mkdir(self.output_dir + '/{}'.format(diff_dir_name))
                self.linkdir(obj.output_dir, event['data'], self.output_dir + '/{}'.format(diff_dir_name))
                stat_path = self.output_dir + '/{}/'.format(diff_dir_name) + event['data'] + "/diff_stat_dir"
                if not os.path.exists(stat_path):
                    os.mkdir(stat_path)
                for files in os.listdir(self.output_dir + "/{}/".format(diff_dir_name) + event['data']):  # 把差异统计表移到diff_stat_dir文件夹
                    if re.search(r'edgr_stat.xls', files):
                        os.system("""mv %s %s""" % (self.output_dir + "/{}/".format(diff_dir_name) + event['data'] + "/" + files,stat_path + "/" + files))
                if os.path.exists(obj.work_dir + "/diff_list"):
                    os.link(obj.work_dir + "/diff_list", self.output_dir + "/{}/".format(diff_dir_name) + event['data'] + "/diff_list")
                    # self.add_gene_id(self.output_dir + "/{}/".format(diff_dir_name) + event['data'] + "/diff_list",
                    #                      self.output_dir + "/{}/".format(diff_dir_name) + event['data'] )
                if os.path.exists(os.path.join(obj.work_dir, 'diff_list_dir')):
                    self.linkdir(obj.work_dir + '/diff_list_dir', 'diff_list_dir',self.output_dir + '/{}/'.format(diff_dir_name) + event['data'])
                    network_file = os.listdir(self.output_dir + "/ref_diff/" + event["data"] + "/diff_list_dir")[0]
                    self.add_gene_id(os.path.join(self.output_dir + "/ref_diff/" + event["data"] + "/diff_list_dir", network_file),
                                     self.output_dir + "/ref_diff/" + event["data"], 'network_{}'.format(network_file))
                    if os.path.exists(self.output_dir + "/ref_diff/" + event["data"] + '/network_{}'.format(network_file)):
                        self.option("network_diff_list",self.output_dir + "/ref_diff/" + event["data"] + '/network_{}'.format(network_file))
                        self.logger.info("已经设置network_diff_list参数！")
                    else:
                        raise Exception("没有生成对应的差异基因network_{}".format(network_file))
                else:
                    self.logger.info("{}分析没有生成diff_list_dir文件夹！".format(event['data']))
                self.logger.info("ref差异分析结果目录设置成功！")
        
        #elif self.option('express_method').lower() == "featurecounts":
        elif event['data'] == 'genes_diff_fpkm' or event['data'] == 'genes_diff_tpm':
                if not os.path.exists(self.output_dir+"/diff"):
                    os.mkdir(self.output_dir+"/diff")
                self.linkdir(obj.output_dir, event['data'], self.output_dir+'/diff')
                if os.path.exists(os.path.join(obj.work_dir, 'diff_list_dir')):
                    self.linkdir(obj.work_dir+'/diff_list_dir', 'diff_list_dir', self.output_dir+'/diff/'+event['data'])
                    self.option("diff_list_dir").set_path(self.output_dir+'/diff/'+event['data']+'/diff_list_dir')
                    self.logger.info("diff_list_dir文件已拷贝成功！")
                else:
                    self.logger.info("没有生成diff_list_dir文件夹！")
                self.logger.info("差异分析结果目录设置成功！")
                self.logger.info("开始进行样本间相关性分析！")
        elif re.search(r'correlation',event['data']) or re.search(r'trans_correlation',event['data']):
            if not os.path.exists(self.output_dir+"/correlation"):
                os.mkdir(self.output_dir+"/correlation")
            self.linkdir(obj.output_dir, event['data'], self.output_dir+"/correlation")
            self.logger.info("设置correlation输出结果成功！")
        elif re.search(r'pca',event['data']) or re.search(r'trans_pca',event['data']):
            if not os.path.exists(self.output_dir+"/pca"):
                os.mkdir(self.output_dir + "/pca")
            self.linkdir(obj.output_dir, event['data'], self.output_dir+"/pca")
            self.logger.info("设置pca输出结果成功！")
        if self.count_output >= 10:
            self.end()

    def add_gene_id(self, old_diff_list, new_path, filename=None):
        if os.path.exists(old_diff_list):
            tmp = os.path.join(new_path + "/tmp")
            shutil.copy2(old_diff_list, tmp)
            new_diff_list = os.path.join(new_path, filename)
            os.system("""sed '1i gene_id' {}>{}""".format(tmp, new_diff_list))
            os.remove(tmp)
            return new_diff_list
        else:
            raise Exception("{}文件不存在".format(old_diff_list))

    def run(self):
        super(ExpressModule, self).run()
        if self.get_list() > 2:
            if self.option("express_method").lower() == 'rsem':
                pass
                # self.on_rely([self.genes_diffRexp, self.trans_diffRexp, self.genes_diffRexp_ref,self.trans_diffRexp_ref, self.genes_corr, self.genes_pca, self.trans_corr, self.trans_pca],self.end)
            elif self.option("express_method").lower() == 'kallisto':
                self.on_rely([self.trans_diffRexp, self.trans_corr, self.trans_pca], self.end)
            elif self.option("express_method").lower() == 'featurecounts':
                self.on_rely([self.fpkm_diffRexp, self.tpm_diffRexp, self.fpkm_corr, self.fpkm_pca, self.tpm_corr, self.tpm_pca], self.end)
        else:
            if self.option("express_method").lower() == 'rsem':
                self.on_rely([self.genes_diffRexp, self.trans_diffRexp, self.genes_diffRexp_ref,self.trans_diffRexp_ref, self.genes_corr, self.trans_corr],self.end)
            elif self.option("express_method").lower() == 'kallisto':
                self.on_rely([self.trans_diffRexp, self.trans_corr], self.end)
            elif self.option("express_method").lower() == 'featurecounts':
                self.on_rely([self.fpkm_diffRexp, self.tpm_diffRexp, self.fpkm_corr, self.tpm_corr], self.end)
        if self.option("express_method").lower() == "featurecounts":
            if self.option("is_express_assembly") == 'assembly':
                self.combine_gtf_run()
            else:
                self.transcript_abstract_run()
        elif self.option("express_method") == "rsem":
            if self.option("is_express_assembly") == 'assembly':
                self.combine_gtf_run()
            else:
                self.transcript_abstract_run()
            # old_rsem = "/mnt/ilustre/users/sanger-dev/workspace/20170622/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_7/Expresstest2/output/rsem1"
            # old_rsem = "/mnt/ilustre/users/sanger-dev/workspace/20170623/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_13/Expresstest3/output/rsem1"
            # old_rsem = "/mnt/ilustre/users/sanger-dev/workspace/20170623/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_merge_1/Expresstest2/output/rsem"
            # new_rsem = self.output_dir
            # self.linkdir(old_rsem,'rsem',new_rsem)
            # merged_gtf = "/mnt/ilustre/users/sanger-dev/workspace/20170623/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_merge_1/Expresstest2/ref_new.gtf"
            # os.link(merged_gtf, self.work_dir+"/ref_new.gtf")
            # self.mergersem_run()
            # if self.count_output >= 8:
            #     self.end()
            # old_rsem = '/mnt/ilustre/users/sanger-dev/workspace/20170620/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_9/Expresstest2/output/rsem'
            # old_rsem = "/mnt/ilustre/users/sanger-dev/workspace/20170621/Single_rsem_stringtie_fpkm_ore_add_gene_location_ref_new_12/Expresstest1/output/rsem/rsem"
            # new_rsem = self.output_dir
            # self.linkdir(old_rsem,'rsem',new_rsem)
            # self.mergersem_run()
            # self.rsem_run()
            # path = "/mnt/ilustre/users/sanger-dev/workspace/20170510/Single_rsem_cufflinks_fpkm_3/Express/output/rsem"
            # genes_count_path = path + "/genes.counts.matrix"
            # trans_count_path = path + "/transcripts.counts.matrix"
            # genes_fpkm_path = path + "/genes.TMM.fpkm.matrix"
            # trans_fpkm_path = path +"/transcripts.TMM.fpkm.matrix"
            # self.rsem_genes_fpkm = genes_fpkm_path
            # self.rsem_transcripts_fpkm = trans_fpkm_path
            # self.diff_Rexp_run(genes_count_path=genes_count_path,trans_count_path=trans_count_path,genes_fpkm_path=genes_fpkm_path,trans_fpkm_path=trans_fpkm_path)
        elif self.option("express_method") == "kallisto":
            self.file_get_list()
            self.transcript_abstract()


    def end(self):
        if self.option("express_method") == "rsem":
            repaths = [
                [".","","表达量分析模块结果输出目录"],
                ["./rsem/genes.counts.matrix",'',"基因counts表达量"],
                ["./rsem/transcripts.counts.matrix","","转录本counts表达量"],
                ["./rsem/trans_list","","转录本ID list"],
                ["./rsem/gene_list","","基因IDlist"],
                ["./diff/genes_diff/diff_list","","差异基因list"],
                ["./diff/genes_diff/merge.xls","xls","差异结果统计"],
                ["./diff/genes_diff/merge_name.xls","xls","差异基因名统计"],
                ["./diff/genes_diff/network_diff_list","xls","差异基因list"],
                ["./diff/genes_diff/diff_stat_dir","","差异结果表"],
                ["./diff/genes_diff/diff_list_dir","","差异基因list"],
                ["./diff/genes_diff/diff_list", "", "差异基因list"],
                ["./diff/trans_diff/merge.xls", "xls", "差异结果统计"],
                ["./diff/trans_diff/merge_name.xls", "xls", "差异基因名统计"],
                ["./diff/trans_diff/network_diff_list", "xls", "差异基因list"],
                ["./diff/trans_diff/diff_stat_dir", "", "差异结果表"],
                ["./diff/trans_diff/diff_list_dir", "", "差异基因list"],
            ]

        # repaths=[
        #     [".","","表达量分析模块结果输出目录"],
        #     ['./express/gene_count', 'txt', '基因count值'],
        #     ['./express/gene_fpkm', 'txt', '基因fpkm值'],
        #     ['./diff/diff_count', '', '差异基因的count值'],
        #     ['./diff/diff_fpkm', '', '差异基因的fpkm值'],
        #     ['./diff/gene_file', '', '差异基因的列表'],
        # ]
        regexps=[
            [r"./rsem/", "", "rsem分析结果输出目录"],
        ]
        super(ExpressModule, self).end()
