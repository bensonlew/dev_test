# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""有参转录组基础分析，无拼接部分"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import shutil
from mbio.files.meta.otu.group_table import GroupTableFile


class RefrnaNoAssembleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        """
        self._sheet = wsheet_object
        super(RefrnaNoAssembleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # 对照组文件，格式同分组文件
            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "seq_method", "type": "string"},  # 测序手段，分为tophat测序和hisat测序    
            {"name": "gff","type": "infile", "format":"ref_rna.reads_mapping.gff"},  # gff格式文件
            {"name": "express_method", "type": "string", "default": "featurecounts"},  # 选择计算表达量的方法 "htseq","both"
            {"name": "sort_type", "type": "string", "default": "pos"},  # 按照位置排序 "name" htseq参数
           #  {"name": "dispersion", "type": "float", "default": 0.1},  # edger离散值
           #  {"name": "min_rowsum_counts", "type": "int", "default": 2},  # 离散值估计检验的最小计数值
           #  {"name": "diff_ci", "type": "float", "default": 0.05},  # 显著性水平
           #  {"name": "diff_rate", "type": "float", "default": 0.01}  # 期望的差异基因比率

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filecheck = self.add_tool("denovo_rna.filecheck.file_denovo")
        self.qc = self.add_module("ref_rna.qc.quality_control")
        self.qc_stat_before = self.add_module("ref_rna.qc.qc_stat")
        self.qc_stat_after = self.add_module("ref_rna.qc.qc_stat")
        self.mapping = self.add_module("ref_rna.mapping.rnaseq_mapping")
        self.map_qc = self.add_module("ref_rna.mapping.ref_assessment")
        self.exp = self.add_module("ref_rna.express.express")
        self.diff_exp = self.add_module("denovo_rna.express.diff_analysis")
        # self.network = self.add_module("ref_rna.ppinetwork_analysis")
        # self.tf = self.add_tool("")  # 转录因子tool
        self.step.add_steps("qcstat", "mapping", "annotation", "express", "map_stat")
        
    def check_options(self):
        """
        检查参数设置
        """
        if self.option('ref_genome') == "customer_mode":
            if not self.option("ref_genome_custom").is_set or not self.option("gff").is_set:
                raise OptionError("数据库自定义模式必须设置参考基因组和gff文件")
        if not self.option("fq_type") in ["PE","SE"]:
            raise OptionError("测序类型不正确")
        if not self.option("seq_method") in ["Tophat","Hisat"]:
            raise OptionError("请选择比对软件")
        return True
        
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
            "ref_genome_custom" : self.option("ref_genome_custom"),
            "ref_genome" : self.option("ref_genome"),
            "mapping_method" : self.option("seq_method").lower(),  # 测序手段，分为tophat测序和hisat测序
            "seq_method" : self.option("fq_type"),   # PE或SE
            "fastq_dir" : self.qc.option("sickle_dir"),
            "gff" : self.option("gff"),  
            "assemble_method" : "None"
        }
        self.mapping.set_options(opts)
        self.mapping.on("end",self.set_output,"mapping")
        self.mapping.run()

    def run_map_assess(self):
        if self.option("ref_genome") == "customer_mode":
            self.option("gff").gff_to_bed()
            bed_path = self.option("gff").prop["path"] + ".bed"
        else:
            bed_path = ""
        opts ={
            "bed" : bed_path,
            "bam" : self.mapping.option("bam_output")     
        }
        self.map_qc.set_options(opts)
        self.map_qc.on("end",self.set_output,"map_qc")
        self.map_qc.run()
    
    def run_exp(self):
        if self.option("ref_genome") == "customer_mode":
            self.option("gff").gff_to_gtf()
            gtf_path = self.option("gff").prop["path"] + ".gtf"
        else:
            gtf_path = ""
        self.logger.info(self.mapping.option("bam_output").prop["path"])
        opts = {
            "fq_type" : self.option("fq_type"),
            "ref_gtf" : gtf_path,
            "gtf_type" : "ref",
            "express_method" : "featurecounts",
            "sort_type" : "pos",
            "sample_bam" : self.mapping.option("bam_output"),
            "strand_specific" : "None",
            "control_file" : self.option("control_file"),
            "edger_group" : self.option("group_table")
        }
        self.exp.set_options(opts)
        self.exp.on("end",self.set_output,"exp")
        self.exp.run()
    """    
    def run_exp_diff(self):
        if self.exp_stat.diff_gene:
            exp_diff_opts = {
                'diff_fpkm': self.exp_stat.option('diff_fpkm'),
                'analysis': self.option('exp_analysis')
            }
            if 'network' in self.option('exp_analysis'):
                exp_diff_opts.update({'gene_file': self.exp_stat.option('gene_file')})
            elif 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'kegg_path': self.annotation.option('kegg_path'),
                    'diff_list_dir': self.exp_stat.option('diff_list_dir')
                })
            elif 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'go_list': self.annotation.option('go_list'),
                    'diff_list_dir': self.exp_stat.option('diff_list_dir'),
                    'all_list': self.exp_stat.option('all_list'),
                    'go_level_2': self.annotation.option('go_level_2')
                })
            self.exp_diff.set_options(exp_diff_opts)
            self.exp_diff.on('end', self.set_output, 'exp_diff')
            self.exp_diff.run()
            self.final_tools.append(self.exp_diff)
        else:
            self.logger.info('输入文件数据量过小，没有检测到差异基因，差异基因相关分析将忽略')
    """
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
        if event['data'] == 'exp':
            self.move2outputdir(obj.output_dir, 'express')
            self.logger.info('express analysis are done')
        if event['data'] == 'exp_diff':
            self.move2outputdir(obj.output_dir, 'express_diff')
            self.logger.info("express diff")
        
    def run(self):
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_qc_stat, False)
        self.qc.on('end', self.run_qc_stat, True)
        self.qc.on('end',self.run_mapping)
        # self.mapping.on("end",self.run_map_assess)
        self.mapping.on("end",self.run_exp)
        self.run_filecheck()
        super(RefrnaNoAssembleWorkflow, self).run()
        
    def end(self):
        super(RefrnaNoAssembleWorkflow, self).end()
