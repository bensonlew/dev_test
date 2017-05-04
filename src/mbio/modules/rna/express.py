#!/usr/bin/python
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
from mbio.packages.ref_rna.express.cmp_ref_cls_relation import *
from mbio.packages.denovo_rna.express.express_distribution import distribution
import shutil

class ExpressModule(Module):
    def __init__(self,work_id):
        super(ExpressModule,self).__init__(work_id)
        options=[
            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组的gtf文件
            {"name": "merged_gtf", "type": "infile", "format": "gene_structure.gtf"}, #拼接生成的merged.gtf文件
            {"name": "cmp_gtf", "type": "infile", "format": "gene_structure.gtf"}, #gttcompare生成的annotated.gtf文件
            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam_dir"},  # 所有样本的bam文件夹 适用于featureCoutns软件
            {"name": "fastq_dir", "type":"infile", "format":"sequence.fastq, sequence.fastq_dir"}, #所有样本的fastq_dir文件夹，适用于rsem, kallisto软件
            {"name": "ref_genome", "type": "string"}, # 参考基因组参数
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  #转录本的fasta 适用于kallisto软件
            {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "strand_dir", "type": "string"},  # 链特异性时选择正链, 默认不设置此参数"forward" "reverse"
            {"name": "express_method", "type": "string", "default": "rsem"},  # 选择计算表达量的方法 默认"rsem"
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹  不压缩的fq文件
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq"},  # SE测序，包含所有样本的fq文件的文件夹
            {"name": "is_duplicate", "type":"bool"}, #是否有生物学重复
            {"name": "diff_rate", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
            {"name": "control_file", "type": "infile", "format": "sample.control_table"},  # 对照组文件，格式同分组文件
            {"name": "edger_group", "type": "infile", "format": "sample.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
            {"name": "all_list", "type": "outfile", "format": "rna.gene_list"},  # 全部基因名称文件
            {"name": "method", "type": "string", "default": "edgeR"},  # 分析差异基因选择的方法
            # {"name": "change_sample_name", "type": "bool"}, #选择是否更改样本的名称
            {"name": "exp_way", "type": "string", "default": "fpkm"}, #默认选择fpkm进行表达量的计算
            {"name": "diff_list_dir", "type": "outfile", "format": "rna.diff_stat_dir"}  #差异分组对应的差异基因
        ]
        self.add_option(options)
        self.logger.info(self.option("express_method"))
        if self.option("express_method").lower() == 'featurecounts':
            self.step.add_steps("featurecounts", "genes_diffRexp", "genes_corr", "genes_pca")
            self.featurecounts=self.add_tool("rna.featureCounts")
        elif self.option("express_method").lower() == 'rsem':
            self.step.add_steps("rsem", "mergersem",  "transcript_abstract", "genes_diffRexp", "trans_diffRexp", "trans_corr", "trans_pca", "genes_corr", "genes_pca")
        elif self.option("express_method").lower() == 'kallisto':
            self.step.add_steps("kallisto", "mergekallisto", "transcript_abstract", "trans_diffRexp", "trans_corr", "trans_pca")
        if self.option('express_method').lower() == 'featurecounts' or self.option("express_method").lower()=='rsem':
            self.genes_corr = self.add_tool("denovo_rna.mapping.correlation")
            self.genes_pca = self.add_tool("meta.beta_diversity.pca")
            self.genes_diffRexp = self.add_tool("rna.diff_exp")
        if self.option("express_method").lower() == 'rsem' or self.option("express_method").lower()=='kallisto':
            self.trans_corr = self.add_tool("denovo_rna.mapping.correlation")
            self.trans_pca = self.add_tool("meta.beta_diversity.pca")
            self.trans_diffRexp = self.add_tool("rna.diff_exp")
        self.tool_lists = []
        self.samples = []
        self.sumtool = []

    def check_options(self):
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        return True

    def featurecounts_run(self):
        n=0
        """发送信息给前端"""
        print "featureCounts开始计算表达量！"
        self.step.featurecounts.start()
        self.step.update()
        print self.option('fq_type')
        tool_opt = {
            "ref_gtf": self.option("merged_gtf").prop['path'],
            "strand_specific": self.option("strand_specific"),
            "cpu":10,
            "max_memory": "100G",
            "bam": self.option("sample_bam").prop['path'],
            "fq_type": self.option('fq_type')
        }
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
        self.step.rsem.start()
        self.step.update()
        if os.path.isdir(self.option("fastq_dir").prop['path']):
            self.logger.info("检验fastq_dir成功！")
            #先只添加用户自定义，上传fasta文件，不涉及平台参考库
            self.file_get_list()
            tool_opt = {
                        "ref_gtf": self.option("cmp_gtf").prop['path']
                    }
            tool_opt["transcript_fa"] = self.option("ref_genome_custom").prop['path']
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
                    self.tool_lists.append(self.rsem)
                    # self.rsem.run()
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
        file_sample = FileSampleFile()
        self.logger.info(list_path)
        file_sample.set_path(list_path)
        self.samples = file_sample.get_list()
        self.logger.info(self.samples)
        if self.samples!=None:
            if self.samples.keys() != None and self.samples.values() != None:
                pass
            else:
                raise Exception("根据list.txt文件获取样本名称失败！")
    
    def transcript_abstract(self):
        #生成transcript.fasta文件，传给kallisto软件使用;默认使用gtf文件计算表达量
        self.transcript_abstract = self.add_tool("annotation.transcript_abstract")
        self.step.transcript_abstract.start()
        self.step.update()
        if self.option("ref_genome") == "customer_mode":
            fasta = self.option("ref_genome_custom").prop["path"]
            self.gtf = self.option("merged_gtf").prop["path"]
        else:
            with open("/mnt/ilustre/users/sanger-dev/app/database/refGenome/scripts/ref_genome.json", "r") as a:
                dict = json.loads(a.read())
                fasta = dict[self.option("ref_genome_custom")]["fasta"]
                self.gtf = dict[self.option("ref_genome")]["gtf"]
        tool_opt = {
            "ref_genome_custom":fasta,
            "ref_genome_gtf":self.gtf
        }
        if self.option("ref_genome") == 'customer_mode':
            tool_opt['ref_genome'] = 'customer_mode'
        self.logger.info(fasta)
        self.logger.info(self.gtf)
        self.logger.info(tool_opt['ref_genome'])
        self.transcript_abstract.set_options(tool_opt)
        self.transcript_abstract.on('end', self.set_output, 'transcript_abstract')
        #self.transcript_abstract.on('end', self.set_step, {'end': self.step.transcript_abstract})
        self.transcript_abstract.run()
        #self.logger.info('transcript_abstract运行结束！')
        
    def kallisto_run(self):
        self.step.kallisto.start()
        self.step.update()
        if os.path.isdir(self.option("fastq_dir").prop['path']):
            #先只添加用户自定义，上传fasta文件，不涉及平台参考库
            self.file_get_list()
            self.transcript_fasta=self.output_dir+'/transcript_abstract/'+'exon.fasta'
            if os.path.exists(self.transcript_fasta):
                tool_opt = {
                        "ref_genome_custom": self.transcript_fasta
                        }
            else:
                raise Exception("transcript_fasta文件不存在！")
            if self.option("fq_type") =="SE":
                for single in self.samples.values():
                    tool_opt.update({
                        "fq_type": "SE",
                        "single_end_reads": os.path.join(self.option("fastq_dir").prop['path'], single)
                    })
                    self.kallisto = self.add_tool("ref_rna.express.kallisto")
                    self.kallisto.set_options(tool_opt)
                    self.tool_lists.append(self.kallisto)
                    #self.kallisto.run()
            elif self.option("fq_type") == "PE":
                for single in self.samples.values():
                    if single.keys() in ["l", "r"]:
                        l_reads=os.path.join(self.option("fastq_dir").prop['path'], single["l"])
                        r_reads=os.path.join(self.option("fastq_dir").prop['path'], single["r"])
                        tool_opt.update({
                            "fq_type": "PE",
                            "left_reads": l_reads,
                            "right_reads": r_reads
                        })
                        if self.option("strand_specific") ==True:
                            tool_opt.update({
                                "strand_dir": self.strand_dir
                            })
                        self.kallisto = self.add_tool("ref_rna.express.kallisto")
                        self.kallisto.set_options(tool_opt)
                        self.tool_lists.append(self.kallisto)
                        #self.kallisto.run()
        if len(self.tool_lists)!=1:
            self.on_rely(self.tool_lists, self.set_output, "kallisto")
            for i in self.tool_lists:
                i.run()
            #self.on_rely(self.tool_lists, self.mergekallisto_run, 'mergekallisto')
            #self.on_rely(self.tool_lists, self.set_step, {'end': self.step.kallisto, 'start': self.step.mergekallisto})
        else:
            self.kallisto.on('end', self.set_output, 'kallisto') #得到单个样本的表达量，无法进行下游分析
            self.kallisto.run()
            self.logger.info("单个样本只能计算表达量，无法进行样本间相关性评估、venn图和差异分析！")
    
    def mergekallisto_run(self):
        self.mergekallisto = self.add_tool("ref_rna.express.mergekallisto")
        dir_path = self.output_dir + "/kallisto/"
        self.mergekallisto.set_options({
            "kallisto_output_dir": dir_path
        })
        self.mergekallisto.on('end', self.set_output, 'mergekallisto')
        self.mergekallisto.on('end', self.set_step, {'end': self.step.mergekallisto})
        self.mergekallisto.run()
        #self.wait()
        self.logger.info("mergekallisto 运行结束！")
        
    def mergersem_run(self):
        self.mergersem = self.add_tool("rna.merge_rsem")
        dir_path = self.output_dir + '/rsem'
        self.logger.info(dir_path)
        if not os.path.exists(dir_path):
            raise Exception("{}文件不存在，请检查！".format(dir_path))
        opts = {
            "rsem_files": dir_path,
            "exp_way": self.option("exp_way"),
            "is_duplicate": self.option("is_duplicate"),
            "gtf_ref": self.option("ref_gtf").prop['path'],
            "gtf_cmp": self.option("cmp_gtf").prop['path'],
            "is_class_code": True
        }
        if self.option("is_duplicate"):
            opts.update({
                "edger_group": self.option("edger_group").prop['path']
            })
        self.mergersem.set_options(opts)
        self.mergersem.on('end', self.set_output, 'mergersem')
        self.mergersem.on('end', self.set_step, {'end': self.step.mergersem})
        self.mergersem.run()
        
        
    def sample_correlation(self):
        if self.option("express_method").lower() == 'rsem':
            # if self.option("change_sample_name") == True:
                # self.logger.info(self.genes_new_fpkm_path)
                # self.logger.info(self.new_fpkm_path)
                # self.genes_corr.set_options({
                    # "fpkm": self.genes_new_fpkm_path
                # })
                # self.genes_corr.on("end", self.set_output, "genes_correlation")
                # self.genes_corr.run()
                # self.trans_corr.set_options({"fpkm": self.new_fpkm_path})
                # self.trans_corr.on("end", self.set_output, "trans_correlation")
                # self.trans_corr.run()
                # self.genes_pca.set_options({"otutable": self.genes_new_fpkm_path})
                # self.genes_pca.on("end", self.set_output, "genes_pca")
                # if self.get_list() > 2:
                    # self.genes_pca.run()
                # self.trans_pca.set_options({"otutable": self.new_fpkm_path})
                # self.trans_pca.on("end", self.set_output, "trans_pca")
                # if self.get_list() > 2:
                    # self.trans_pca.run()
            # else:
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
            # if self.option("change_sample_name") == True:
                # self.genes_corr.set_options({"fpkm": self.genes_new_fpkm_path})
                # self.genes_pca.set_options({"otutable": self.genes_new_fpkm_path})
            # else:
            self.genes_corr.set_options({"fpkm": self.feature_fpkm_path})
            self.genes_pca.set_options({"otutable": self.feature_fpkm_path})
            self.genes_corr.on("end",self.set_output,"genes_correlation")
            self.genes_pca.on("end",self.set_output,"genes_pca")
            self.genes_corr.run()
            if self.get_list() > 2:
                self.genes_pca.run()
            else:
                self.logger.info("样本数目小于等于2，无法进行样本间pca分析！")
        elif self.option("express_method") == "kallisto":
            # if self.option("change_sample_name") == True:
                # self.trans_corr.set_options({"fpkm": self.new_fpkm_path})
                # self.trans_pca.set_options({"otutable": self.new_fpkm_path})
            # else:
            self.trans_corr.set_options({"fpkm": self.kallito_fpkm_path})
            self.trans_pca.set_options({"otutable": self.kallito_fpkm_path})
            self.trans_corr.on("end", self.set_output, "trans_correlation")
            self.trans_pca.on("end", self.set_output, "trans_pca")
            self.trans_corr.run()
            if self.get_list() > 2:
                self.trans_pca.run()
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
        
    def diff_Rexp_run(self, genes_count_path = None, trans_count_path = None, genes_fpkm_path = None, trans_fpkm_path = None):
        self.logger.info('开始进行差异表达分析！')
        tool_opt = {
            "control_file": self.option('control_file').prop['path'],
            "gname": "group",
            "method": self.option("method"),
            'diff_ci': self.option("diff_ci")
        }
        edger_group_path = self.option('edger_group').prop['path']
        #venn_group_table = venn_group_table
        # if self.option("change_sample_name") == True:
            # if self.option("express_method").lower() == 'rsem' or self.option("express_method").lower() == "featurecounts":
                # self.genes_new_count_path, self.sample_name_dict, self.edger_group_path, self.genes_new_fpkm_path = changesamplename(file_path_count=genes_count_path, file_path_fpkm=genes_fpkm_path, out_count="genes_count.txt", out_fpkm="genes_fpkm.txt",\
                       # group_file=edger_group_path)
                # genes_opt = tool_opt
                # genes_opt["count"] = self.genes_new_count_path
                # genes_opt["fpkm"] = self.genes_new_fpkm_path
                # genes_opt["edger_group"] = self.edger_group_path
                # self.logger.info("更新基因的count，fpkm，edger_group成功！")
                # self.genes_diffRexp.set_options(genes_opt)
                # self.genes_diffRexp.on('end', self.set_output,'genes_diff')
                # self.genes_diffRexp.on("end", self.set_step, {"end": self.step.genes_diffRexp})
                # self.genes_diffRexp.on("end", self.sample_correlation)
                # self.genes_diffRexp.run()
                # self.logger.info("计算基因差异分析成功！")
            # if self.option("express_method").lower() == 'rsem' or self.option("express_method").lower() == "kallisto":
                # self.new_count_path, self.sample_name_dict, self.edger_group_path ,self.new_fpkm_path = changesamplename(file_path_count=trans_count_path, file_path_fpkm=trans_fpkm_path, out_count="trans_count.txt", out_fpkm="trans_fpkm.txt",\
                       # group_file=edger_group_path)
                # self.logger.info(self.new_count_path)
                # self.logger.info(self.edger_group_path)
                # self.logger.info(self.new_fpkm_path)
                # trans_opt = tool_opt
                # trans_opt["count"] = self.new_count_path
                # trans_opt["fpkm"] = self.new_fpkm_path
                # trans_opt["edger_group"] = self.edger_group_path
                # self.logger.info("更新转录本的count，fpkm，edger_group成功！")
                # self.trans_diffRexp.set_options(trans_opt)
                # self.trans_diffRexp.on('end', self.set_output,'trans_diff')
                # self.trans_diffRexp.on("end", self.set_step, {"end": self.step.trans_diffRexp})
                # if self.option("express_method").lower() == 'kallisto':
                    # self.genes_diffRexp.on("end", self.sample_correlation)
                # self.trans_diffRexp.run()
                # self.logger.info("计算转录本差异分析成功！")
        # else:
        if self.option("express_method").lower() == 'rsem' or self.option("express_method").lower() == "featurecounts":
            genes_opt = tool_opt
            genes_opt["count"] = genes_count_path
            genes_opt["fpkm"] = genes_fpkm_path
            genes_opt['edger_group'] = edger_group_path
            self.genes_diffRexp.set_options(genes_opt)
            self.genes_diffRexp.on('end', self.set_output, 'genes_diff')
            self.genes_diffRexp.on("end", self.set_step, {"end": self.step.genes_diffRexp})
            self.genes_diffRexp.on("end", self.sample_correlation)
            self.genes_diffRexp.run()
            self.logger.info("计算基因差异分析成功！")
        if self.option("express_method").lower() == 'rsem' or self.option("express_method").lower() == "kallisto":
            trans_opt = tool_opt
            trans_opt["count"] = trans_count_path
            trans_opt["fpkm"] = trans_fpkm_path
            trans_opt["edger_group"] = edger_group_path
            self.trans_diffRexp.set_options(trans_opt)
            self.trans_diffRexp.on('end', self.set_output, 'trans_diff')
            self.trans_diffRexp.on("end", self.set_step, {"end": self.step.trans_diffRexp})
            if self.option("express_method").lower() == 'featurecounts':
                self.genes_diffRexp.on("end", self.sample_correlation)
            self.trans_diffRexp.run()
            self.logger.info("计算转录本差异分析成功！")
        
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
                elif self.option("exp_way") == "tpm":
                    self.feature_fpkm_path = self.featurecounts.output_dir + "/fpkm_tpm.tpm.xls"
                self.diff_Rexp_run(genes_count_path = self.feature_count_path, genes_fpkm_path = self.feature_fpkm_path)
        elif event['data'] == 'transcript_abstract':
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("transcript_abstract设置结果目录成功！")
            if self.option("express_method") == "kallisto":
                self.kallisto_run()
            elif self.option("express_method") == 'rsem':
                self.rsem_run()
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
                        shutil.copy2(os.path.join(tool.output_dir, files), os.path.join(rsem_path, files))
                    if re.search(r'isoforms.results', files):
                        shutil.copy2(os.path.join(tool.output_dir, files), os.path.join(rsem_path, files))
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
            old_rsem_path = self.output_dir + "/oldrsem"
            if not os.path.exists(old_rsem_path):
                os.mkdir(old_rsem_path)
            ss = 0
            for files in os.listdir(obj.output_dir):
                files_path = os.path.join(obj.output_dir, files)
                new_files_path = os.path.join(self.output_dir+"/rsem",files)
                shutil.copy2(files_path, new_files_path)
            for f in os.listdir(obj.work_dir+"/"+"oldrsem"):
                shutil.copy2(obj.work_dir+"/"+"oldrsem/"+f, old_rsem_path+"/"+f)
                self.logger.info("设置gene count 和 fpkm 输出结果成功！")
            self.rsem_genes_count = self.output_dir+'/rsem/genes.counts.matrix'
            self.rsem_transcripts_count = self.output_dir + '/rsem/transcripts.counts.matrix'
            if self.option("exp_way").lower() == 'fpkm':
                self.rsem_genes_fpkm = self.output_dir + '/rsem/genes.TMM.fpkm.matrix'
                self.rsem_transcripts_fpkm = self.output_dir + '/rsem/transcripts.TMM.fpkm.matrix'
            elif self.option("exp_way").lower() == 'tpm':
                self.rsem_genes_fpkm = self.output_dir + '/rsem/genes.TMM.EXPR.matrix'
                self.rsem_transcripts_fpkm = self.output_dir + '/rsem/transcripts.TMM.EXPR.matrix'
            self.diff_Rexp_run(genes_count_path=self.rsem_genes_count, genes_fpkm_path=self.rsem_genes_fpkm,trans_count_path = self.rsem_transcripts_count, trans_fpkm_path = self.rsem_transcripts_fpkm)
        elif event['data'] == "genes_diff" or event['data'] == 'trans_diff':
            self.logger.info("开始设置差异分析结果目录！")
            if self.option('express_method') == 'rsem':
                if not os.path.exists(self.output_dir+'/diff'):
                    os.mkdir(self.output_dir+'/diff')
                self.linkdir(obj.output_dir, event['data'], self.output_dir+'/diff')
                if os.path.exists(os.path.join(obj.work_dir, 'diff_list_dir')):
                    self.linkdir(obj.work_dir+'/diff_list_dir', 'diff_list_dir', self.output_dir+'/diff/'+event['data'])
                else:
                    self.logger.info("{}分析没有生成diff_list_dir文件夹！".format(event['data']))
                self.logger.info("差异分析结果目录设置成功！")
            else:
                self.linkdir(obj.output_dir, event['data'], self.output_dir)
                if os.path.exists(os.path.join(obj.work_dir, 'diff_list_dir')):
                    self.linkdir(obj.work_dir+'/diff_list_dir', 'diff_list_dir', self.output_dir+'/'+event['data'])
                    self.option("diff_list_dir").set_path(self.output_dir+'/'+event['data']+'/diff_list_dir')
                    self.logger.info("diff_list_dir文件已拷贝成功！")
                else:
                    self.logger.info("没有生成diff_list_dir文件夹！")
                self.logger.info("差异分析结果目录设置成功！")
                self.logger.info("开始进行样本间样本间相关性分析！")
        elif event['data'] == 'kallisto':
            new_dir = os.path.join(self.output_dir, event['data'])
            self.logger.info(new_dir)
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)
            else:
                os.system("rm -rf {}".format(new_dir)) #如果kallisto文件夹存在，则删除重建
                self.logger.info("删除旧的kallisto文件夹并生成新的！")
                os.mkdir(new_dir)
            for tool in self.tool_lists:
                for files in os.listdir(tool.output_dir):
                    if files.endswith(".tsv") and files != "abundance.tsv":
                        shutil.copy2(os.path.join(tool.output_dir, files), os.path.join(new_dir, files))
                        self.logger.info("文件拷贝成功！")
            self.logger.info("设置kallisto输出结果成功！")
            if self.get_list() > 1:
                self.mergekallisto_run()
            else:
                self.logger.info("样本数目小于等于2，无法进行样本间分析、差异分析、差异统计！")
        elif event['data'] == "mergekallisto":
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置mergekallisto输出结果成功！")
            self.kallisto_count_path = self.mergekallisto.output_dir + "/count.xls"
            if self.option("exp_way") == 'fpkm':
                self.kallisto_fpkm_path = self.mergekallisto.output_dir + "/kallisto.fpkm.xls"
            elif self.option("exp_way") == "tpm":
                self.kallisto_fpkm_path = self.mergekallisto.output_dir + "/true_tpm.xls"
            self.trans_fpkm = self.diff_Rexp_run(trans_count_path = self.kallisto_count_path, trans_fpkm_path = self.kallisto_fpkm_path)
        elif event['data'] == "genes_correlation" or event['data'] == 'trans_correlation':
            if not os.path.exists(self.output_dir+"/correlation"):
                os.mkdir(self.output_dir+"/correlation")
            if self.option("express_method").lower() == 'rsem':
                self.linkdir(obj.output_dir, event['data'], self.output_dir+"/correlation")
            else:
                self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置correlation输出结果成功！")
        elif event['data'] == "genes_pca" or event['data'] == 'trans_pca':
            if not os.path.exists(self.output_dir+"/pca"):
                os.mkdir(self.output_dir + "/pca")
            if self.option("express_method").lower() == 'rsem':
                self.linkdir(obj.output_dir, event['data'], self.output_dir+"/pca")
            else:
                self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置pca输出结果成功！")
        """
        elif event['data'] == 'venn_corr' or event['data'] == 'venn_genes' or event['data'] == 'venn_transcripts':
            self.logger.info("设置样本间venn图输出结果成功！")
            self.logger.info("开始进行样本间相关性评估！")
            if self.option('express_method') == 'rsem':
                if not os.path.exists(self.output_dir + '/venn'):
                    os.mkdir(self.output_dir+'/venn')
                self.linkdir(obj.output_dir, event['data'], self.output_dir+'/venn')
                self.sample_correlation(self.genes_fpkm, 'genes')
                self.sample_correlation(self.transcripts_fpkm, 'transcripts')
            else:
                self.linkdir(obj.output_dir, event['data'], self.output_dir)
                self.sample_correlation(self.fpkm)
        """

    def run(self):
        super(ExpressModule, self).run()
        if self.get_list() > 2:
            if self.option("express_method").lower() == 'rsem':
                self.on_rely([self.genes_diffRexp, self.trans_diffRexp, self.genes_corr, self.genes_pca, self.trans_corr, self.trans_pca],self.end)
            elif self.option("express_method").lower() == 'kallisto':
                self.on_rely([self.trans_diffRexp, self.trans_corr, self.trans_pca], self.end)
            elif self.option("express_method").lower() == 'featurecounts':
                self.on_rely([self.genes_diffRexp, self.genes_corr, self.genes_pca], self.end)
        else:
            if self.option("express_method").lower() == 'rsem':
                self.on_rely([self.genes_diffRexp, self.trans_diffRexp, self.genes_corr, self.trans_corr],self.end)
            elif self.option("express_method").lower() == 'kallisto':
                self.on_rely([self.trans_diffRexp, self.trans_corr], self.end)
            elif self.option("express_method").lower() == 'featurecounts':
                self.on_rely([self.genes_diffRexp, self.genes_corr], self.end)
        if self.option("express_method").lower() == "featurecounts":
            self.featurecounts_run()
        elif self.option("express_method") == "rsem":
            self.rsem_run()
        elif self.option("express_method") == "kallisto":
            self.file_get_list()
            self.transcript_abstract()
       # super(ExpressModule, self).run()

    def end(self):
        repaths=[
            [".","","表达量分析模块结果输出目录"],
            ['./express/gene_count', 'txt', '基因count值'],
            ['./express/gene_fpkm', 'txt', '基因fpkm值'],
            ['./diff/diff_count', '', '差异基因的count值'],
            ['./diff/diff_fpkm', '', '差异基因的fpkm值'],
            ['./diff/gene_file', '', '差异基因的列表'],
        ]
        regexps=[
            [r"./rsem/", "", "rsem分析结果输出目录"],
        ]
        super(ExpressModule, self).end()
