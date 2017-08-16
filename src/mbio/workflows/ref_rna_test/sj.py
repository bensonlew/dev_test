# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""sj star建索引工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
import os
import json
import shutil
import re


class SjWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(SjWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "genome_structure_file", "type": "infile", "format": "gene_structure.gff3"},
            {"name": "ref_genome", "type": "string", "default": "customer_mode"},  # 参考基因组
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq_dir"},  # Fastq文件夹
            {"name": "group_table", "type": "infile", "format": "sample.group_table"},  # 分组文件
            {"name": "control_file", "type": "infile", "format": "sample.control_table"}

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.file_check = self.add_tool("rna.filecheck_ref")
        self.star_index = self.add_tool("align.star_index")
        self.json_path = self.config.SOFTWARE_DIR + "/database/refGenome/scripts/ref_genome.json"
        self.json_dict = self.get_json()
        if self.option("ref_genome") != "customer_mode":
            self.ref_genome = self.json_dict[self.option("ref_genome")]["ref_genome"]
            self.option("ref_genome_custom", self.ref_genome)
            self.taxon_id = self.json_dict[self.option("ref_genome")]["taxon_id"]
            self.anno_path = self.json_dict[self.option("ref_genome")]["anno_path"]
            self.logger.info(self.anno_path)
        else:
            self.ref_genome = self.option("ref_genome_custom")
            self.taxon_id = ""
        self.gff = ""
        if self.option("ref_genome") == "customer_mode":
            if self.option('genome_structure_file').format == "gene_structure.gff3":
                self.gff = self.option('genome_structure_file').prop["path"]
        else:
            self.gff = self.json_dict[self.option("ref_genome")]["gff"]

    def check_options(self):
        """
        检查选项
        """
        return True

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def get_json(self):
        f = open(self.json_path, "r")
        json_dict = json.loads(f.read())
        return json_dict

    def run_filecheck(self):
        opts = {
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': "PE",
            'control_file': self.option('control_file'),
            "ref_genome_custom": self.option("ref_genome_custom")
        }
        if self.gff != "":
            opts.update({
                "gff": self.gff
            })
        else:
            opts.update({
                "in_gtf": self.option('genome_structure_file').prop["path"]
            })
        if self.option('group_table').is_set:
            opts.update({'group_table': self.option('group_table')})
        self.file_check.set_options(opts)
        self.file_check.run()

    def run_star_index(self):
        opts = {
            "ref_genome": self.option("ref_genome"),
            "ref_gtf": self.file_check.option("gtf"),
            "seq_method": "PE"
        }
        self.star_index.set_options(opts)
        self.star_index.on("end", self.set_output)
        self.star_index.run()

    def set_output(self):
        base_path = os.path.split(self.ref_genome)[0]
        index_path = os.path.join(base_path, "ref_star_index1")
        if not os.path.exists(index_path):
            os.mkdir(base_path + "/ref_star_index1")
        else:
            for file in os.listdir(base_path + "/ref_star_index1/"):
                file_path = os.path.join(base_path + "/ref_star_index1/", file)
                os.remove(file_path)
        for file in os.listdir(self.work_dir + "/ref_star_index1"):
            file_path = os.path.join(self.work_dir + "/ref_star_index1", file)
            os.link(file_path, base_path + "/ref_star_index1/")
        self.end()

    def run(self):
        self.file_check.on("end", self.run_star_index)
        self.run_filecheck()
        super(SjWorkflow, self).run()

    def end(self):
        super(SjWorkflow, self).end()