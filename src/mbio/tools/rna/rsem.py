#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ : konghualei 
# last modify: 20170301

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from mbio.packages.ref_rna.express.single_sample import *
from mbio.files.sequence.fastq import FastqFile
import shutil
import os
import re
import subprocess

class RsemAgent(Agent):
    """调用rsem软件计算基因表达量(对转录本计算表达量), bowtie2方法建索引、比对"""
    def __init__(self, parent):
        super(RsemAgent, self).__init__(parent)
        options = [
            {"name": "fq_type", "type": "string"}, #PE OR SE
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 转录本fasta文件
            {"name": "fq_l", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹  不压缩的fq文件
            {"name": "fq_r", "type": "infile", "format": "sequence.fastq"},  # PE测序，包含所有样本的左端fq文件的文件夹
            {"name": "fq_s", "type": "infile", "format": "sequence.fastq"},  # SE测序，包含所有样本的fq文件的文件夹
            # {"name": "bam", "type": "infile", "format": "align.bwa.bam, ref_rna.assembly.bam_dir"}, # bam文件输入
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf, ref_rna.reads_mapping.gff" }, # gtf/gff文件
            #{"name": "gtf_type", "type": "string", "default": "ref"}, # "ref" "merged_stringtie" "merged_cufflinks"
            {"name": "sample_name", "type":"string"}, # 样本名称
            {"name": "cpu", "type": "int", "default": 8},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}, # 设置内存
            {"name": "only_bowtie_build", "type": "bool", "default": False},  # 为true时该tool只建索引
            {"name": "bowtie_build_rsem", "type": "bool", "default": False},  # 为true时该tool需要建索引
        ]
        self.add_option(options)
        self.step.add_steps("rsem")
        self.on("start", self.stepstart)
        self.on("end", self.stepfinish)
    
    def stepstart(self):
        self.step.rsem.start()
        self.step.update()
    
    def stepfinish(self):
        self.step.rsem.finish()
        self.step.update()
    
    def check_options(self):
        """重写参数检测函数"""
        self.logger.info("输出ref_gtf格式")
        self.logger.info(self.option('ref_gtf').format)
        if not self.option("ref_fa").is_set:
            raise OptionError("请输入参考基因组的fa文件")
        if self.option("fq_type") not in ['PE', 'SE']:
            raise OptionError("请输入单端或双端!")
        if not self.option("ref_gtf").is_set:
            raise OptionError("请输入参考基因组gtf文件!")
    
    def set_resource(self):
        self._cpu = 8
        self._memory = '100G'
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        super(RsemAgent, self).end()
        
class RsemTool(Tool):
    def __init__(self, config):
        super(RsemTool, self).__init__(config)
        # self.version = "1.0.1"
        #self.rsem_path =self.config.SOFTWARE_DIR + '/bioinfo/rna/RSEM-1.2.31/bin/'
        self.rsem_path ='bioinfo/rna/RSEM-1.2.31/bin/'
        self.bowtie_path = self.config.SOFTWARE_DIR + '/bioinfo/align/bowtie2-2.2.9/'
        #self.star_build_path = self.config.SOFTWARE_DIR + "/bioinfo/rna/star-2.5/bin/Linux_x86_64/"
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.set_environ(PATH=self.rsem_path)
        self.set_environ(PATH=self.bowtie_path)
    
    def rsem_build(self):
        self.rsem_index_path = os.path.join(self.work_dir, "rsem_index")
        gtf_format = os.path.basename(self.option('ref_gtf').prop['path'])
        gene2transcript = os.path.join(self.work_dir, 'gene2transcript')
        if re.search(r"gtf$",gtf_format):
            gtf(self.option('ref_gtf').prop['path'], gene2transcript)
            self.logger.info("对gtf文件提取gene_transcript成功！")
        elif re.search(r'gff$|gff3$', gtf_format):
            gff3(infile = self.option('ref_gtf').prop['path'], outfile = gene2transcript, out_path=self.work_dir)
            self.logger.info("对gtf文件提取gene_transcript成功！")
        if os.path.exists(gene2transcript):
            self.logger.info("建立gene_id和transcript_id对应关系成功！")
        if self.option("ref_fa").is_set:
            cmd = self.rsem_path + "rsem-prepare-reference -p 8 --transcript-to-gene-map {} {} {} --bowtie2 --bowtie2-path {} ".format(gene2transcript, self.option("ref_fa").prop['path'],\
                   self.rsem_index_path, self.bowtie_path)
        """
        if self.option("ref_gtf").is_set and self.option("ref_fa").is_set:  #根据gtf/gff、fasta建索引
            if gtf_format.endswith('gff3'):
                cmd = self.rsem_path + "rsem-prepare-reference -p 8 --gff3 {} {} --bowtie2 --bowtie2-path {} {}".format(self.option("ref_gtf").prop['path'],\
                    self.option("ref_fa").prop['path'], self.bowtie_path, self.rsem_index_path)
            else:
                cmd = self.rsem_path + "rsem-prepare-reference -p 8 --gtf {} {} --bowtie2 --bowtie2-path {} {}".format(self.option("ref_gtf").prop['path'],\
                    self.option("ref_fa").prop['path'], self.bowtie_path, self.rsem_index_path)
        else:
            raise Exception("{}和{}文件不存在！".format(self.option("ref_gtf").prop['path'], self.option("ref_fa").prop['path']))
        """
        self.logger.info(cmd)
        bowtie2_cmd = self.add_command("bowtie2_build", cmd).run()
        self.wait()
        if bowtie2_cmd.return_code == 0:
            self.logger.info("%s运行完成" % bowtie2_cmd)
        else:
            self.set_error("%s运行出错!" % bowtie2_cmd)
    
    def rsem_run(self):
        data = FastqFile()
        if self.option("fq_type") == "PE":
            cmd = self.rsem_path + "rsem-calculate-expression --paired-end -p 8 {} {} {} {} --bowtie2 --bowtie2-path {}".format(self.option("fq_l").prop['path'],\
               self.option("fq_r").prop['path'], self.rsem_index_path , self.output_dir+"/"+self.option("sample_name"), self.bowtie_path)
        elif self.option("fq_type") == "SE":
            cmd = self.rsem_path + "rsem-calculate-expression -p 8 {} {} {} --bowtie2 --bowtie2-path {}".format(self.option("fq_s").prop['path'],\
                   self.rsem_index_path , self.output_dir+"/"+self.option("sample_name"), self.bowtie_path)
        self.logger.info(cmd)
        exp_cmd = self.add_command("express_run", cmd).run()
        self.wait()
        if exp_cmd.return_code == 0:
            self.logger.info("{}运行成功！".format(exp_cmd))
        else:
            self.set_error("{}运行失败".format(exp_cmd))

    def set_output(self):
        for files in os.listdir(self.work_dir):
            if files.endswith('isoforms.results'):
                shutil.copy2(os.path.join(self.work_dir, files), self.output_dir+"/"+files)
            elif files.endswith('genes.results'):
                shutil.copy2(os.path.join(self.work_dir, files), self.output_dir+"/"+files)
        for _files in os.listdir(self.output_dir):
            file_path = os.path.join(self.output_dir, _files)
            os.system(""" sed -i "s/gene://g" %s """ % (file_path))
            os.system(""" sed -i "s/transcript://g" %s """ % (file_path))
        self.logger.info("RSEM表达量结果设置成功!")
    
    def run(self):
        super(RsemTool, self).run()
        if self.option("only_bowtie_build") == True:
            self.rsem_build()
            self.logger.info("rsem建立索引成功！")
        else:
            if self.option("bowtie_build_rsem") == False:
                self.rsem_build()
                self.rsem_run()
                self.logger.info("rsem计算表达量成功！")
            else:
                self.rsem_run()
        self.set_output()
        self.end()