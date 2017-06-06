# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from __future__ import division
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import subprocess

class GenomeStructureAgent(Agent):
    """
    获得染色体上基因数量、GC含量信息
    """
    def __init__(self, parent):
        super(GenomeStructureAgent, self).__init__(parent)
        options = [
            {"name": "in_fasta", "type": "infile", "format": "sequence.fasta"},
            {"name": "in_gff", "type": "infile", "format": "gene_structure.gff3"},
            {"name": "in_gtf", "type": "infile", "format":"gene_structure.gtf"}
        ]
        self.add_option(options)
        self.step.add_steps("genome_structure")
        self.on('start', self.start_genome_structure)
        self.on("end", self.end_genome_structure)

    def start_genome_structure(self):
        self.step.genome_structure.start()
        self.step.update()

    def end_genome_structure(self):
        self.step.genome_structure.finish()
        self.step.update()

    def check_options(self):
        if not self.option("in_fasta").is_set:
            raise OptionError("参数in_fasta不能为空")

    def set_resource(self):
        self._cpu = 4
        self._memory = "4G"


class GenomeStructureTool(Tool):
    def __init__(self, config):
        super(GenomeStructureTool, self).__init__(config)
        self.samtools ="/bioinfo/align/samtools-1.3.1/samtools"
        self.bedtools = self.config.SOFTWARE_DIR + "/bioinfo/rna/bedtools2-master/bin/bedtools"
        self.scripts = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/tabletools_add.pl"

    def cmd1(self):
        new_path = self.work_dir + "/" + os.path.basename(self.option("in_fasta").prop["path"])
        if os.path.exists(new_path):
            os.remove(new_path)
        os.link(self.option("in_fasta").prop["path"], new_path)
        cmd = "{} faidx {}".format(self.samtools, os.path.basename(self.option("in_fasta").prop["path"]))
        command = self.add_command("faidx_cmd", cmd)
        self.logger.info("samtools开始运行")
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("samtools运行完成")
            self.cmd2()
        else:
            command.rerun()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("samtools运行完成")
                self.cmd2()
            else:
                raise Exception("samtools运行出错")

    def cmd2(self):
        cmd = "less -S {}".format(os.path.basename(self.option("in_fasta").prop["path"]) + ".fai")
        cmd += " | awk -F '\\t' -vOFS='\\t' '{print $1,0,$2}' | sort -k1,1 | "
        cmd += "{} nuc -fi {} -bed - | cut -f1,5,12 | ".format(self.bedtools, self.option("in_fasta").prop["path"])
        cmd += """awk -F '\\t' -vOFS='\\t' 'BEGIN{print "#Chr", "Seq_Len","Pct_GC"} NR > 1 {print $1,$3,int($2*10000+0.5)/100"%"}' > ref_genome.stat.xls"""
        self.logger.info("第二步开始运行,命令为{}".format(cmd))
        subprocess.check_output(cmd, shell=True)
        self.logger.info("第二步运行完成")
        self.cmd3()

    def cmd3(self):
        if self.option("in_gff").is_set:
            new_path = self.work_dir + "/" + os.path.basename(self.option("in_gff").prop["path"])
        else:
            new_path = self.work_dir + "/" + os.path.basename(self.option("in_gtf").prop["path"])
        if os.path.exists(new_path):
            os.remove(new_path)
        if self.option("in_gff").is_set:
            os.link(self.option("in_gff").prop["path"], new_path)
        else:
            os.link(self.option("in_gtf").prop["path"], new_path)
        cmd = "less -S {} | grep -v '^#' | awk -F '\\t' '$3 == \"gene\"' | cut -f1,9 | " \
              "cut -d ';' -f1,2 | sort -k1,1 | {} groupby -g 1 -c 1 -o count ".format(new_path, self.bedtools)
        cmd += "| awk -F '\\t' -vOFS='\\t' 'BEGIN{print \"# Chr\",\"Gene_Number\"}1' > ref_genome.gee.stat.xls"
        self.logger.info("第三步开始运行,命令为{}".format(cmd))
        subprocess.check_output(cmd, shell=True)
        self.logger.info("第三步运行完成")
        self.cmd4()

    def cmd4(self):
        cmd = "perl {} -i ref_genome.gee.stat.xls -t ref_genome.stat.xls -n 1 " \
              "> ref_genome.gene.stat.xls".format(self.scripts)
        self.logger.info("第四步开始运行,命令为{}".format(cmd))
        subprocess.check_output(cmd, shell=True)
        self.logger.info("第四步运行完成")
        self.end()

    def run(self):
        super(GenomeStructureTool, self).run()
        self.cmd1()
