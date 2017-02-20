#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ == konghualei
# last_modify: 2016.11.11

from biocluster.module import Module
import os
import re
from biocluster.core.exceptions import OptionError
import glob
from mbio.files.sequence.file_sample import FileSampleFile
from mbio.packages.ref_rna.express.set_strand import set_strand
from mbio.packages.ref_rna.express.change_sample_name import *
from mbio.packages.ref_rna.express.gene_list_file_change import *
from mbio.packages.ref_rna.express.single_sample import *
import shutil

class ExpressModule(Module):
    def __init__(self,work_id):
        super(ExpressModule,self).__init__(work_id)
        options=[
            {"name": "fq_type", "type": "string", "default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 参考基因组的gtf文件
            {"name": "gtf_type", "type":"string","default": "ref"},  #ref，merge_cufflinks，merge_stringtie, new_transcripts, new_genes五种参数
            {"name": "sample_bam", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  # 所有样本的bam文件夹 适用于rsem htseq featureCoutns软件
            {"name": "fastq_dir", "type":"infile", "format":"sequence.fastq, sequence.fastq_dir"}, #所有样本的fastq_dir文件夹，适用于kallisto软件
            {"name": "ref_genome", "type": "string"}, # 参考基因组参数
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  #转录本的fasta 适用于kallisto软件
            {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "strand_dir", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数"forward" "reverse"
            #{"name": "feature_id", "type": "string", "default": "gene"},  # 默认计算基因的count值，可以选择exon，('transcript'仅限于featurecounts)
            {"name": "express_method", "type": "string"},  # 选择计算表达量的方法 "htseq"
            {"name": "sort_type", "type": "string", "default": "pos"},  # 按照位置排序 "name" htseq参数
            {"name": "diff_rate", "type": "float", "default": 0.1},  # edger离散值
            {"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # 对照组文件，格式同分组文件
            {"name": "edger_group", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
            {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
            {"name": "all_list", "type": "outfile", "format": "denovo_rna.express.gene_list"},  # 全部基因名称文件
            {"name": "method", "type": "string", "default": "edgeR"},  # 分析差异基因选择的方法
            {"name": "change_sample_name", "type": "bool", "default": True} #选择是否更改样本的名称
        ]
        self.add_option(options)
        self.step.add_steps("featurecounts", "diff_Rexp", "htseq", "mergehtseq", "kallisto", "mergekallisto", "pca", "corr")
        self.featurecounts=self.add_tool("ref_rna.express.featureCounts")
        self.corr = self.add_tool("denovo_rna.mapping.correlation")
        self.pca = self.add_tool("meta.beta_diversity.pca")
        self.diff_Rexp = self.add_tool("ref_rna.express.diff_Rexp")
        self.tool_lists = []
        self.samples = []
        self.sumtool = []

    def check_options(self):
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        #if not self.option("ref_gtf").is_set:
        #    raise OptionError("需要输入gtf文件")
        #if not self.option("sample_bam").is_set:
        #    raise OptionError("请传入bam的文件夹")
        #if not self.option('control_file').is_set:
        #     raise OptionError("必须设置输入文件：上下调对照组参考文件")
        #if self.option("diff_ci") >= 1 or self.option("diff_ci") <= 0:
        #    raise OptionError("显著性水平不在(0,1)范围内")
        # if self.option("diff_rate") > 1 or self.option("diff_rate") <= 0:
        #     raise OptionError("期望的差异基因比率不在(0，1]范围内")
        #samples, genes = self.option('count').get_matrix_info()
        if self.option('express_method') == 'htseq':
            os.path.splitext(os.path.basename(self.option('ref_gtf').prop['path']))[:-1]
            #self.gtf_path = check_gtf_format(gtf_path=self.option('ref_gtf').prop['path'],out_gtf='gtf')
            self.gtf_path = self.option('ref_gtf').prop['path']
        return True

    def featurecounts_run(self):
        n=0
        """发送信息给前端"""
        print "featureCounts开始计算表达量！"
        self.step.featurecounts.start()
        self.step.update()
        print self.option("ref_gtf").prop['path'],self.option("gtf_type"),self.option("strand_specific"),self.option("sample_bam").prop['path']
        print self.option('fq_type')

        tool_opt = {
            "ref_gtf": self.option("ref_gtf").prop['path'],
            "gtf_type": self.option("gtf_type"),
            "strand_specific": self.option("strand_specific"),
            "bam": self.option("sample_bam").prop['path'],
            "fq_type": self.option('fq_type')
        }
        if self.option("strand_specific"):
             tool_opt.update({
                 "strand_dir":self.option("strand_dir")
             })
        """
        tool_opt = {
            "ref_gtf": "/mnt/ilustre/users/sanger-dev/sg-users/wangzhaoyue/Eukaryote/tophat2/cufflinks/merge_123456/merged.gtf",
            "fq_type": "PE",
            "strand_specific": False,
            "gtf_type": "ref",
            "bam": "/mnt/ilustre/users/sanger-dev/sg-users/wangzhaoyue/Eukaryote/hisat2/cufflinks/featurecounts_sample_dir/samples"
        }
        if self.option("strand_specific"):
             tool_opt.update({
                 "strand_dir":self.option("strand_dir")
             })
        """
        self.featurecounts.set_options(tool_opt)
        self.featurecounts.on('end', self.set_output, 'featurecounts')
        self.featurecounts.on('end', self.set_step, {'end': self.step.featurecounts})
        if self.get_list() > 1:
            self.featurecounts.on('end', self.sample_correlation)
            self.featurecounts.on('end', self.diff_Rexp_run)
        else:
            self.logger.info("单个样本只能计算表达量，无法进行样本间相关性评估和差异分析！")
        self.featurecounts.run()
        

    def htseq_run(self):
        n=0
        self.step.htseq.start()
        self.step.update()
        tool_opt = {
               "ref_gtf": self.option("ref_gtf").prop['path'],
               "strand_specific": self.strand_specific,
               "sort_type": self.option("sort_type"),
               "gtf_type": self.option("gtf_type"),
               "feature": self.option("feature_id")
        }
        if self.option("strand_specific"):
            tool_opt.update({
               "strand_dir": self.strand_dir
            })
        s_files=os.listdir(self.option('sample_bam').prop['path'])
        for f in s_files:
            n+=1
            tool_opt.update({
               'bam': self.option('sample_bam').prop['path']+'/'+f
            })
            self.logger.info(n)
            self.htseq=self.add_tool('ref_rna.express.htseq')
            print tool_opt
            self.htseq.set_options(tool_opt)
            self.tool_lists.append(self.htseq)
            self.htseq.run()
            self.sumtool.append(self.htseq)
        if len(self.tool_lists) != 1:
            self.on_rely(self.tool_lists, self.set_output, "htseq")
            self.on_rely(self.tool_lists, self.set_step, {'end': self.step.htseq, 'start': self.step.mergehtseq})
        else:
            self.htseq.on('end', self.set_output, "htseq") #只能得到单个样本的表达量

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

    def kallisto_run(self):
        self.step.kallisto.start()
        self.step.update()
        if os.path.isdir(self.option("fastq_dir").prop['path']):
            #先只添加用户自定义，上传fasta文件，不涉及平台参考库
            self.file_get_list()
            tool_opt = {
                        "ref_genome_custom": self.option("ref_genome_custom").prop['path']
                    }
            if self.option("fq_type") =="SE":
                for single in self.samples.values():
                    tool_opt.update({
                        "fq_type": "SE",
                        "single_end_reads": os.path.join(self.option("fastq_dir").prop['path'], single)
                    })
                    self.kallisto = self.add_tool("ref_rna.express.kallisto")
                    self.kallisto.set_options(tool_opt)
                    self.tool_lists.append(self.kallisto)
                    self.kallisto.run()
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
                        self.kallisto.run()
        if len(self.tool_lists)!=1:
            self.on_rely(self.tool_lists, self.set_output, "kallisto")
            self.on_rely(self.tool_lists, self.set_step, {'end': self.step.kallisto, 'start': self.step.mergekallisto})
        else:
            self.kallisto.on('end', self.set_output, 'kallisto') #得到单个样本的表达量，无法进行下游分析
            self.logger.info("单个样本只能计算表达量，无法进行样本间相关性评估和差异分析！")

    def mergekallisto_run(self):
        self.mergekallisto = self.add_tool("ref_rna.express.mergekallisto")
        dir_path = self.output_dir + "/kallisto/"
        self.mergekallisto.set_options({
            "kallisto_output_dir": dir_path
        })
        self.mergekallisto.on('end', self.set_output, 'mergekallisto')
        self.mergekallisto.on('end', self.set_step, {'end': self.step.mergekallisto})
        self.mergekallisto.on('end', self.sample_correlation)
        self.mergekallisto.run()
        self.wait()
        self.logger.info("mergekallisto 运行结束！")
        
    def mergehtseq_run(self):
        self.mergehtseq = self.add_tool("ref_rna.express.mergehtseq")
        dir_path = self.output_dir + '/htseq'
        self.logger.info(dir_path)
        self.mergehtseq.set_options({
            "htseq_files": dir_path
        })
        self.sumtool.append(self.mergehtseq)
        self.mergehtseq.on('end', self.set_output, 'mergehtseq')
        self.mergehtseq.on('end', self.set_step, {'end': self.step.mergehtseq, 'start': self.step.diff_Rexp})
        """把差异表达分析绑定在一起"""
        self.mergehtseq.run()
        self.wait()
        self.logger.info("mergehtseq 运行结束！")

    def sample_correlation(self):
        if self.option("express_method").lower() == "featurecounts":
            fpkm_path = self.featurecounts.option("fpkm").prop['path']
        elif self.option("express_method") == "kallisto":
            fpkm_path = self.mergekallisto.option("fpkm").prop['path']
        elif self.option("express_method") == "htseq":
            pass
        if not os.path.exists(fpkm_path):
            raise Exception("没有生成对应的fpkm文件，无法进行样本间相关性评估！")
        else:
            self.logger.info(fpkm_path)
        self.corr.set_options({
            "fpkm": fpkm_path
        })
        self.pca.set_options({
            "otutable": fpkm_path
        })
        # self.corr.on("end", self.set_step, {"end", self.step.corr})
        # self.pca.on("end", self.set_step, {"end", self.step.pca})
        self.corr.on("end", self.set_output, "correlation")
        self.pca.on("end", self.set_output, "pca")
        self.corr.run()
        # self.wait()
        self.pca.run()
        # self.wait()
        # self.logger.info("sample_correlation 运行结束！")
        
    def diff_Rexp_run(self):
        self.logger.info('开始进行差异表达分析！')
        """
        if self.option("express_method").find("featurecounts") != -1:
            fpkm_count_dir = self.output_dir + "/mergefeaturecounts_express"
        elif self.option("express_method").find("htseq") != -1:
            fpkm_count_dir = self.output_dir + "/mergehtseq_express" 
        for f in os.listdir(fpkm_count_dir):
            if f.find("fpkm") != -1:
                fpkm_path=os.path.join(fpkm_count_dir, f)
                self.logger.info(fpkm_path)
            else:
                count_path=os.path.join(fpkm_count_dir, f)
                self.logger.info(count_path)
        """
        edger_group_path = self.option('edger_group').prop['path']
        count_path = self.featurecounts.output_dir + "/count.xls"
        fpkm_path = self.featurecounts.output_dir + "/fpkm_tpm.fpkm.xls"
        if self.option("change_sample_name"):
            new_count_path, sample_name_dict, edger_group_path ,new_fpkm_path= changesamplename(file_path_count=count_path, file_path_fpkm=fpkm_path, out_count="count.txt", out_fpkm="fpkm.txt",group_file=edger_group_path)
        self.logger.info(new_count_path)
        self.logger.info(new_fpkm_path)
        tool_opt = {
           "count": new_count_path,
           "fpkm": new_fpkm_path,
            "control_file": self.option('control_file').prop['path'],
            "edger_group": edger_group_path,
            "gname": "group",
            "method": self.option("method"),
            'diff_ci': self.option("diff_ci")
        }
        #if self.option("edger_group").is_set:
        #     tool_opt['edger_group'] = self.option("edger_group").prop['path']
        #     tool_opt['gname'] = self.option('gname')
        self.diff_Rexp.set_options(tool_opt)
        self.sumtool.append(self.diff_Rexp)
        self.diff_Rexp.on("end", self.set_output, 'diff_Rexp')
        self.diff_Rexp.on("end", self.set_step, {"end": self.step.diff_Rexp})
        self.diff_Rexp.run()
        self.wait()
        return sample_name_dict

    def get_list(self):
        list_path = self.option("sample_bam").prop['path']
        sample_number = os.listdir(list_path)
        return sample_number

    def linkdir(self, dirpath, dirname, output_dir):
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath,i)for i in allfiles]
        newfiles = [os.path.join(newdir,i)for i in allfiles]
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
            #self.diff_Rexp_run()
        elif event['data'] == 'htseq':
            for tool in self.tool_lists:
                self.linkdir(tool.output_dir, event['data'], self.output_dir)
            self.mergehtseq_run()
        elif event['data'] == "mergehtseq":
            self.linkdir(obj.output_dir, event['data']+"_express", self.output_dir)
            self.logger.info("设置gene count 和 fpkm 输出结果成功！")
            self.diff_Rexp_run()
        elif event['data'] == "diff_Rexp":
            file_path = self.work_dir+"/DiffRexp/output"
            self.linkdir(file_path, 'diff', self.output_dir)
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
            self.mergekallisto_run()
        elif event['data'] == "mergekallisto":
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置mergekallisto输出结果成功！")
        elif event['data'] == "correlation":
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置correlation输出结果成功！")
        elif event['data'] == "pca":
            self.linkdir(obj.output_dir, event['data'], self.output_dir)
            self.logger.info("设置pca输出结果成功！")
        # self.logger.info("end!")
        # self.logger.info("输出结果失败")
        # self.end()

    def run(self):
        super(ExpressModule, self).run()
        self.on_rely([self.corr,self.diff_Rexp,self.pca],self.end)
        if self.option("express_method").lower() == "featurecounts":
            self.featurecounts_run()
        elif self.option("express_method") == "htseq":
            self.htseq_run()
        elif self.option("express_method") == "kallisto":
            self.file_get_list()
            self.kallisto_run()

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
            [r"./featurecounts/", "", "featurecounts分析结果输出目录"],
        ]
        super(ExpressModule, self).end()

