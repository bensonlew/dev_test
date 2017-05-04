#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ == shijin

import os
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class ParallelAnnoModule(Module):
    """
    如果比对产生的xml文件过大，将比对过程中未合并的小xml进行注释并产生upload_table文件，合并upload_table之后再进行注释
    version 1.0
    """
    def __init__(self, work_id):
        super(ParallelAnnoModule, self).__init__(work_id)
        options = [
            {"name": "nr_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "kegg_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "string_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},
            {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},
            {"name": "upper_limit", "type": "int", "default": 10},  # xml被拆为10份
            {"name": "out_cog", "type": "outfile", "format": "annotation.upload.anno_upload"},
            {"name": "out_kegg", "type": "outfile", "format": "annotation.upload.anno_upload"},
            {"name": "out_go", "type": "outfile", "format": "annotation.upload.anno_upload"}
        ]
        self.add_option(options)
        self.xml_list = []
        self.change_list = []
        self.anno_list = []

    def check_options(self):
        """
        检查参数
        """
        return True

    def run(self):
        super(ParallelAnnoModule, self).run()
        self.get_list()
        # if self.option("blast_method") == "diamond":
        self.run_anno()

    def get_list(self):
        nr_tmp = self.option("nr_align_dir").prop["path"]
        kegg_tmp = self.option("kegg_align_dir").prop["path"]
        string_tmp = self.option("string_align_dir").prop["path"]
        for i in range(self.option("upper_limit")):
            mark = "_" + str(i + 1) + "_"
            file_tmp = []
            for file in os.listdir(nr_tmp):
                if file.find(mark) != -1 and not file.endswith("_new"):
                    file = os.path.join(nr_tmp, file)
                    file_tmp.append(file)
            for file in os.listdir(string_tmp):
                if file.find(mark) != -1 and not file.endswith("_new"):
                    file = os.path.join(string_tmp, file)
                    file_tmp.append(file)
            for file in os.listdir(kegg_tmp):
                if file.find(mark) != -1 and not file.endswith("_new"):
                    file = os.path.join(kegg_tmp, file)
                    file_tmp.append(file)
            self.xml_list.append(file_tmp)
            self.logger.info(str(i) + "\t" + str(file_tmp))

    def run_anno(self):
        for i in range(self.option("upper_limit")):
            lst = self.xml_list[i]
            opts = {
                "blast_nr_xml": lst[0],
                "blast_string_xml": lst[1],
                "blast_kegg_xml": lst[2],
                "gene_file": self.option("gene_file"),
                "ref_genome_gtf": self.option("ref_genome_gtf"),
                'go_annot': True,
                'nr_annot': True
            }
            anno_module = self.add_module("annotation.ref_annotation")
            anno_module.set_options(opts)
            self.anno_list.append(anno_module)
        if len(self.anno_list) != 1:
            self.on_rely(self.anno_list, self.cat_output)
        else:
            self.anno_list[0].on("end", self.cat_output)
        for anno_mod in self.anno_list:
            anno_mod.run()
    
    def cat_output(self):
        for mod in self.anno_list:
            transcript_kegg_path = mod.kegg_annot.output_dir + "/kegg_table.xls"
            transcript_go_path = mod.go_annot.output_dir + "/blast2go.annot"
            transcript_cog_path = mod.string_cog.work_dir + "/cog_list.xls"
            gene_kegg_path = mod.option("gene_kegg_table").prop["path"]
            gene_go_path = mod.anno_stat.work_dir + "/go_stat/gene_blast2go.annot"
            gene_cog_path = mod.anno_stat.work_dir + "/go_stat/gene_cog_list.xls"
            self.add2kegg_table(transcript_kegg_path, self.output_dir + "/transcript_kegg.list", "transcript")
            self.add2kegg_table(gene_kegg_path, self.output_dir + "/gene_kegg.list", "gene")
            self.add2go_table(transcript_go_path, self.output_dir + "/transcript_go.list", "transcript")
            self.add2go_table(gene_go_path, self.output_dir + "/gene_go.list", "gene")
        self.end()

    @staticmethod
    def add2kegg_table(_kegg_table, table, type_):
        file = open(table, "a")
        with open(_kegg_table, "r") as r:
            r.readline()
            for line in r:
                tmp = line.split("\t")
                t_id = tmp[0]
                ko_id = tmp[1]
                string = t_id + "\t" + type_ + "\t" + ko_id + "\n"
                file.write(string)
        file.close()

    @staticmethod
    def add2go_table(_go_table, table, type_):
        file = open(table, "a")
        with open(_go_table, "r") as r:
            r.readline()
            for line in r:
                tmp = line.split("\t")
                t_id = tmp[0]
                ko_id = tmp[1]
                string = t_id + "\t" + type_ + "\t" + ko_id + "\n"
                file.write(string)
        file.close()
