#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ == shijin

import os
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class AnnoUploadModule(Module):
    """
    根据kegg、go的upload_table文件和cog的table文件
    version 1.0
    """
    def __init__(self, work_id):
        super(AnnoUploadModule, self).__init__(work_id)
        options = [
            {"name": "nr_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "kegg_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "string_align_dir", "type": "infile", "format": "align.blast.blast_xml_dir"},
            {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},
            {"name": "upper_limit", "type": "int", "default": 10},  # xml被拆为10份
            {"name": "out_cog", "type": "outfile", "format": "annotation.cog.cog_table"},
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
        super(AnnoUploadModule, self).run()
        self.get_list()
        # if self.option("blast_method") == "diamond":
        self.run_anno()

    def get_list(self):
        nr_tmp = self.option("nr_align_dir").prop["path"]
        kegg_tmp = self.option("kegg_align_dir").prop["path"]
        string_tmp = self.option("string_align_dir").prop["path"]
        for i in range(self.option("upper_limint")):
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

    def run_anno(self):
        for i in range(self.option("upper_limit")):
            lst = self.xml_list[i]
            opts = {
                "blast_nr_xml": lst[0],
                "blast_kegg_xml": lst[1],
                "blast_string_xml": lst[2],
                "gene_file": self.option("gene_file"),
                'go_annot': True,
                'nr_annot': True
            }
            anno_module = self.add_module("annotation.ref_annotation")
            anno_module.set_options(opts)
            self.anno_list.append(anno_module)
        if len(self.anno_list) != 1:
            self.on_rely(self.anno_list, self.end)
        else:
            self.anno_list[0].on("end", self.end)
