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
            {"name": "ref_genome", "type": "string"},                                    # 参考基因组参数，若为customer_mode时，客户传入参考基因组文件，否则选择平台上的
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"}, # 参考基因组fasta文件
            {"name": "ref_genome_gff", "type": "infile", "format": "ref_rna.gff"}        # 参考基因组gff文件
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
        if not self.option("ref_genome_gff").is_set:
            raise OptionError("请设置参考基因组gff文件") 
        else:
            pass
    
    def set_resource(self):
        self._cpu = 10
        self._memory = '10G'

    def end(self):
        super(TranscriptAbstractAgent, self).end()

class TranscriptAbstractTool(Tool):
    def __init__(self, config):
        super(TranscriptAbstractTool, self).__init__(config) 
        self.gffread_path = "bioinfo/rna/cufflinks-2.2.1/"
        self.long_path = "/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/"
        self.python_path = "program/Python/bin/"

    def run_gffread(self):
        if self.option("ref_genome") == "customer_mode":
            fasta = self.option("ref_genome_custom").prop["path"]
            gff = self.option("ref_genome_gff").prop["path"]
        else:
            with open("/mnt/ilustre/users/sanger-dev/app/database/refGenome/ref_genome.json", "r") as a:
                dict = json.loads(a.read())
                fasta = dict[self.option("ref_genome_custom")]["fasta"]
                gff = dict[self.option("ref_genome")]["gff3"]
        cmd = "{}gffread {} -g {} -w exon.fa".format(self.gffread_path, gff, fasta)
        self.logger.info("开始运行cufflinks的gffread，合成、提取exon")
        command = self.add_command("gffread", cmd)
        command.run()
        self.wait()   
        
    def run_long_transcript(self):
        exon_path = os.path.join(self.work_dir, "exon.fa")
        cmd = "{}python {}annotation_longest.py -i {}".format(self.python_path, self.long_path, exon_path) 
        self.logger.info("提取最长序列")
        command = self.add_command("the_longest", cmd)
        command.run()
        self.wait()
        output1 = os.path.join(self.work_dir, "exon.fa")
        shutil.move(output1, "../TranscriptAbstract/output/")
        output2 = os.path.join(self.work_dir, "output.fa")
        shutil.move(output2, "../TranscriptAbstract/output/")
                
    def run(self):
        super(TranscriptAbstractTool, self).run()
        self.run_gffread()
        self.run_long_transcript()
        self.end()
