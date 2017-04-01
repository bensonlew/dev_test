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
            {"name": "in_gff", "type": "infile", "format": "ref_rna.reads_mapping.gff"},
            {"name": "in_gtf", "type": "infile", "format":"ref_rna.reads_mapping.gtf"}
        ]
        self.add_option(options)
        self.step.add_steps("sample_extract")
        self.on('start', self.start_sample_extract)
        self.on("end", self.end_sample_extract)

    def start_sample_extract(self):
        self.step.sample_extract.start()
        self.step.update()

    def end_sample_extract(self):
        self.step.sample_extract.finish()
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

    def cmd2(self):
        new_path = self.work_dir + "/" + os.path.basename(self.option("in_gff").prop["path"])
        if os.path.exists(new_path):
            os.remove(new_path)
        os.link(self.option("in_gff").prop["path"], new_path)
        cmd = "less -S {} | grep -v '^#' | awk -F '\t' '$3 == \"gene\"' | cut -f1,9 | " \
              "cut -d ';' -f1,2 | sort -k1,1 | {} groupby -g 1 -c 1 -o count ".format(new_path, self.bedtools)
        cmd += "| awk -F '\t' -vOFS='\t' 'BEGIN{print \"# Chr\",\"Gene_Number\"}1' > ref_genome.gee.stat.xls"
        self.logger.info("第二步开始运行")
        subprocess.check_output(cmd, shell=True)
        self.logger.info("第二步运行完成")
        self.end()

    def run(self):
        super(GenomeStructureTool, self).run()
        self.cmd1()
