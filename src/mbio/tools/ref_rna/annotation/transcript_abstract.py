# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

import os
import shutil
import re
import json
from biocluster.core.exceptions import OptionError
from biocluster.agent import Agent
from biocluster.tool import Tool


class TranscriptAbstractAgent(Agent):
    """
    提取参考基因组最长序列，作为基因注释的输入文件
    author: zengjing
    last_modify: 2016.09.22
    """
    def __init__(self, parent):
        super(TranscriptAbstractAgent, self).__init__(parent)
        options = [
            {"name": "ref_genome", "type": "string"},                                             # 参考基因组参数，若为customer_mode时，客户传入参考基因组文件，否则选择平台上的
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},          # 参考基因组fasta文件
            {"name": "ref_genome_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 参考基因组gtf文件
            {"name": "ref_genome_gff", "type": "infile", "format": "ref_rna.reads_mapping.gff"},  # 参考基因组gff文件
            {"name": "query", "type": "outfile", "format": "sequence.fasta"},                     # 输出做注释的转录本序列
            {"name": "gene_file", "type": "outfile", "format": "denovo_rna.express.gene_list"}    # 输出最长转录本
        ]
        self.add_option(options)
        self.step.add_steps("Transcript")
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.Transcript.start()
        self.step.update()

    def step_end(self):
        self.step.Transcript.finish()
        self.step.update()

    def check_option(self):
        if not self.option("ref_genome").is_set:
            raise OptionError("请设置参考基因组参数")
        if not self.option("ref_genome_custom").is_set:
            raise OptionError("请设置参考基因组custom文件")
        if self.option("ref_genome_gtf").is_set or self.option("ref_genome_gff").is_set:
            pass
        else:
            raise OptionError("请设置参考基因组gtf文件或gff文件")

    def set_resource(self):
        self._cpu = 10
        self._memory = '10G'

    def end(self):
        super(TranscriptAbstractAgent, self).end()

class TranscriptAbstractTool(Tool):
    def __init__(self, config):
        super(TranscriptAbstractTool, self).__init__(config)
        self.gffread_path = "bioinfo/rna/cufflinks-2.2.1/"
        self.long_path = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/"
        self.python_path = "program/Python/bin/"

    def run_gffread(self):
        if self.option("ref_genome") == "customer_mode":
            fasta = self.option("ref_genome_custom").prop["path"]
            if self.option("ref_genome_gtf").is_set:
                gff = self.option("ref_genome_gtf").prop["path"]
            else:
                gff = self.option("ref_genome_gff").prop["path"]
        else:
            with open("/mnt/ilustre/users/sanger-dev/app/database/refGenome/scripts/ref_genome.json", "r") as a:
                dict = json.loads(a.read())
                fasta = dict[self.option("ref_genome_custom")]["fasta"]
                gff = dict[self.option("ref_genome")]["gtf"]
        cmd = "{}gffread {} -g {} -w exons.fa".format(self.gffread_path, gff, fasta)
        self.logger.info("开始运行cufflinks的gffread，合成、提取exons")
        command = self.add_command("gffread", cmd)
        command.run()
        self.wait()
        output1 = os.path.join(self.work_dir, "exons.fa")
        self.option('query', output1)

    def run_long_transcript(self):
        exon_path = os.path.join(self.work_dir, "exons.fa")
        cmd = "{}python {}annotation_longest.py -i {}".format(self.python_path, self.long_path, exon_path)
        self.logger.info("提取最长序列")
        command = self.add_command("the_longest", cmd)
        command.run()
        self.wait()
        output1 = os.path.join(self.work_dir, "exons.fa")
        shutil.move(output1, "../TranscriptAbstract/output/")
        output2 = os.path.join(self.work_dir, "the_longest_exons.fa")
        shutil.move(output2, "../TranscriptAbstract/output/")
        self.option('query', self.work_dir + '/output/exons.fa')

    def get_gene_list(self):
        # output_path = self.option("query").prop["path"]
        output_path = self.work_dir + "/output/the_longest_exons.fa"
        gene_list_path = self.work_dir + '/output/gene_list.txt'
        gene_lists = []
        with open(output_path, 'rb') as f, open(gene_list_path, 'wb') as w:
            lines = f.readlines()
            for line in lines:
                m = re.match(r">(.+) gene=(.+)", line)
                if m:
                    trans_name = m.group(1)
                    if trans_name not in gene_lists:
                        w.write(trans_name + '\n')
                        gene_lists.append(trans_name)
                else:
                    n = re.match(r">(.+) transcript:(.+)", line)
                    if n:
                        trans_name = n.group(1)
                        if trans_name not in gene_lists:
                            w.write(trans_name + '\n')
                            gene_lists.append(trans_name)
        self.option('gene_file', gene_list_path)

    def run(self):
        super(TranscriptAbstractTool, self).run()
        self.run_gffread()
        self.run_long_transcript()
        self.get_gene_list()
        self.end()
