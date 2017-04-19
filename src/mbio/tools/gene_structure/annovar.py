#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import subprocess
import shutil
from mbio.packages.ref_rna.gene_structure.snp_anno import snp_anno
import json


class AnnovarAgent(Agent):
    """
    Annovar:用对处理vcf格式文件/注释突变信息
    version 1.0
    author: qindanhua
    last_modify: 2016.12.30
    """

    def __init__(self, parent):
        super(AnnovarAgent, self).__init__(parent)
        options = [
            {"name": "ref_genome", "type": "string"},  # 参考基因组类型
            {"name": "input_file", "type": "infile", "format": "ref_rna.gene_structure.vcf"},  # 输入文件
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 输入文件
        ]
        self.add_option(options)
        self.step.add_steps('annovar')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.annovar.start()
        self.step.update()

    def step_end(self):
        self.step.annovar.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("input_file").is_set:
            raise OptionError("请输入VCF格式文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = '20G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./snp_anno.xls", "xls", "snp注释结果表"]
        ])
        # print self.get_upload_files()
        super(AnnovarAgent, self).end()


class AnnovarTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(AnnovarTool, self).__init__(config)
        self.perl_path = "program/perl/perls/perl-5.24.0/bin/"
        self.perl_full_path = self.config.SOFTWARE_DIR + "/program/perl/perls/perl-5.24.0/bin/"
        self.annovar_path = self.config.SOFTWARE_DIR + "/bioinfo/gene-structure/annovar/"
        self.gtfToGenePred_path = "/bioinfo/gene-structure/annovar/"
        self.ref_fasta = ''
        self.ref_gtf = ''

    def get_genome(self):
        if self.option("ref_genome") == "customer_mode":
            self.ref_fasta = self.option("ref_fasta").prop["path"]
            self.ref_gtf = self.option("ref_gtf").prop["path"]
        else:
            ref_genome_json = self.config.SOFTWARE_DIR + "/database/refGenome/scripts/ref_genome.json"
            with open(ref_genome_json, "r") as f:
                ref_dict = json.loads(f.read())
                self.ref_fasta = ref_dict[self.option("ref_genome")]["ref_genome"]
                self.ref_gtf = ref_dict[self.option("ref_genome")]["gtf"]

    def gtf_to_genepred(self):
        cmd = "{}gtfToGenePred -genePredExt {} {}.genes_refGene.tmp.txt"\
            .format(self.gtfToGenePred_path, self.ref_gtf, self.option("ref_genome"))
        command = self.add_command("gtftogenepred", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行gtfToGenePred完成!")
            # print "awk \'{print 1\"\\t\"$0}\'"
            self.logger.info("awk \'{print 1\"\\t\"$0}\'" + " {}.genes_refGene.tmp.txt  > {}_refGene.txt"
                             .format(self.option("ref_genome"), self.option("ref_genome")))
            try:
                subprocess.check_output("awk \'{print 1\"\\t\"$0}\'" + " {}.genes_refGene.tmp.txt  > {}_refGene.txt"
                                        .format(self.option("ref_genome"), self.option("ref_genome")), shell=True)
                self.logger.info("提取gtfToGenePred结果信息完成")
                return True
            except subprocess.CalledProcessError:
                self.logger.info("提取gtfToGenePred结果信息出错")
                return False
        else:
            self.set_error("运行gtfToGenePred出错！")
            raise Exception("运行gtfToGenePred出错！")

    def retrieve_seq_from_fasta(self):
        cmd = "{}perl {}retrieve_seq_from_fasta.pl -format refGene -seqfile {} --outfile {}_refGeneMrna.fa " \
              "{}_refGene.txt".format(self.perl_path, self.annovar_path, self.ref_fasta,
                                      self.option("ref_genome"), self.option("ref_genome"))
        command = self.add_command("retrieve_seq_from_fasta", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0 or None:
            self.logger.info("运行retrieve_seq_from_fasta完成!")
            if os.path.exists("./geneomedb"):
                shutil.rmtree("./geneomedb")
            os.system("mkdir geneomedb")
            os.system("mv {}.genes_refGene.tmp.txt {}_refGene.txt {}_refGeneMrna.fa ./geneomedb/"
                      .format(self.option("ref_genome"), self.option("ref_genome"), self.option("ref_genome")))
        else:
            self.set_error("运行retrieve_seq_from_fasta出错！")
            raise Exception("运行retrieve_seq_from_fasta出错！")

    def convert2annovar(self):
        cmd = "{}perl {}convert2annovar.pl -format vcf4 {} > clean.snp.avinput"\
            .format(self.perl_full_path, self.annovar_path, self.option("input_file").prop["path"])
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("提取convert2annovar结果信息完成")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("提取convert2annovar结果信息出错")
            return False
        # command = self.add_command("convert2annovar", cmd)
        # command.run()
        # self.wait(command)
        # if command.return_code == 0:
        #     self.logger.info("运行convert2annovar完成!")
        # else:
        #     self.set_error("运行convert2annovar出错！")
        #     raise Exception("运行convert2annovar出错！")

    def annotate_variation(self):
        cmd = "{}perl {}annotate_variation.pl -buildver {} -dbtype refGene clean.snp.avinput ./geneomedb"\
            .format(self.perl_path, self.annovar_path, self.option("ref_genome"), self.option("input_file"))
        command = self.add_command("annotate_variation", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行annotate_variation完成!")
        else:
            self.set_error("运行annotate_variation出错！")
            raise Exception("运行annotate_variation出错！")

    def set_output(self):
        self.logger.info("snp annotation")
        snp_anno("./clean.snp.avinput.variant_function", "./clean.snp.avinput.exonic_variant_function", "./snp_anno.xls")
        self.logger.info("snp annotation done")
        self.logger.info("set ouptput")
        self.logger.info(self.output_dir + "/snp_anno.xls")
        if os.path.exists(self.output_dir + "/snp_anno.xls"):
            os.remove(self.output_dir + "/snp_anno.xls")
        os.link(self.work_dir + "/snp_anno.xls", self.output_dir + "/snp_anno.xls")
        self.logger.info("set ouptput done")

    def run(self):
        super(AnnovarTool, self).run()
        self.get_genome()
        self.gtf_to_genepred()
        self.retrieve_seq_from_fasta()
        self.convert2annovar()
        self.annotate_variation()
        self.set_output()
        self.end()
