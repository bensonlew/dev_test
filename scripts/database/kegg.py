#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zengjing"

import re
import os
import gridfs
import datetime
from biocluster.config import Config


class kegg(object):
    def __init__(self):
        self.ko_ids = []
        self.pathway_ids = []
        self.mongodb = Config().biodb_mongo_client.sanger_biodb

    def ko_split(self, ko, ko_path):
        """提出ko里的ko_id, ko_name, ko_definition, pathway_ids, enzyme_ids, module_ids，共20917个K"""
        with open(ko, "rb") as f, open(ko_path, "wb") as w:
            w.write("ko_id\tko_name\tpathway_ids\tenzyme_ids\tmodule_ids\tko_definition\n")
            lines = f.readlines()
            split = []
            split.append(0)
            for i in range(len(lines)):
                m = re.match(r"///.*$", lines[i])
                if m:
                    split.append(i)
            for j in range(len(split)-1):
                start = split[j]
                end = split[j+1]
                pathway_ids, module_ids, enzyme_ids = [], [], []
                ko_id, ko_name, ko_definition = '', '', ''
                for k in range(start, end):
                    line = lines[k]
                    entry = re.match(r"ENTRY\s*(K\d*)\s.*(KO).*$", line)
                    name = re.match(r"NAME\s*(\S.*)$", line)
                    definition = re.match(r"DEFINITION\s*(\S.*)$", line)
                    pathway = re.match(r".*(ko\d{5})\s*(\w.*)$", line)
                    brite = re.match(r"BRITE.*$", line)
                    enzyme = re.match(r"\s*(\d.*.\d.*.\d.*..+)\s{2}\S.*$", line)
                    module = re.match(r"MODULE\s*(M\d*)\s*.*$", line)
                    if entry:
                        ko_id = entry.group(1)
                    if name:
                        ko_name = name.group(1)
                    if definition:
                        ko_definition = definition.group(1)
                    if pathway:
                        pathway_id = pathway.group(1)
                        pathway_ids.append(pathway_id)
                    if enzyme:
                        enzyme_id = enzyme.group(1)
                        enzyme_ids.append(enzyme_id)
                    if module:
                        module_id = module.group(1)
                        module_ids.append(module_id)
                w.write(ko_id + "\t" + ko_name + "\t" + ';'.join(pathway_ids) + "\t" + ';'.join(enzyme_ids) + "\t" + ';'.join(module_ids) + "\t" + ko_definition + "\n")

    def pathway_category(self, pathway, pathway_path):
        """提出pathway里的pathway_id, pathway_category"""
        with open(pathway, "rb") as f, open(pathway_path, "wb") as w:
            w.write("pathway_id\tcategory1\tcategory2\tcategory3\n")
            lines = f.readlines()
            for line in lines:
                first = re.match(r"#(.*)$", line)
                second = re.match(r"##(.*)$", line)
                thrid = re.match(r"(\d{5})\s*(\S.*)$", line)
                if first and not second:
                    pathway1 = first.group(1)
                if second:
                    pathway2 = second.group(1)
                if thrid:
                    pathway_id = thrid.group(1)
                    pathway3 = thrid.group(2)
                    w.write("ko" + pathway_id + "\t" + pathway1 + "\t" + pathway2 + "\t" + pathway3 + "\n")

    def module_category(self, module, module_path):
        """提出module里的module_id, module_category"""
        with open(module, "rb") as f, open(module_path, "wb") as w:
            w.write("module_id\tcategory1\tcategory2\tcategory3\tcategoey4\n")
            lines = f.readlines()
            split = []
            split.append(0)
            for i in range(len(lines)):
                m = re.match(r"///.*$", lines[i])
                if m:
                    split.append(i)
            for j in range(len(split)-1):
                start = split[j]
                end = split[j+1]
                module_id = None
                for k in range(start, end):
                    line = lines[k]
                    m = re.match(r"ENTRY\s*(M\d*)\s.*Module$", line)
                    n = re.match(r"NAME\s*(\S.*)$", line)
                    c = re.match(r"CLASS\s*(\S.*);\s(.*);\s(.*)$", line)
                    if m:
                        module_id = m.group(1)
                    if n:
                        category4 = n.group(1)
                    if c:
                        category1 = c.group(1)
                        category2 = c.group(2)
                        category3 = c.group(3)
                if module_id:
                    w.write(module_id + "\t" + category1 + "\t" + category2 + "\t" + category3 + "\t" + category4 + "\n")

    def enzyme_category(self, enzyme, enzyme_path):
        """提出enzyme里的enzyme_id, enzyme_category"""
        with open(enzyme, "rb") as f, open(enzyme_path, "wb") as w:
            w.write("enzyme_id\tcategory1\tcategory2\tcategory3\tcategory4\n")
            lines = f.readlines()
            for i in range(len(lines)-2):
                line = lines[i]
                entry = re.match(r"ENTRY\s*EC\s*(\S*).*$", line)
                name = re.match(r"NAME\s*(\S.*)$", line)
                class1 = re.match(r"CLASS\s*(\S.*);$", line)
                if entry:
                    enzyme_id = entry.group(1)
                if name:
                    enzyme4 = name.group(1)
                    m = re.match(r"(.*);$", enzyme4)
                    if m:
                        enzyme4 = m.group(1)
                if class1:
                    enzyme1 = class1.group(1)
                    class2 = re.match(r"\s*(\S.*);$", lines[i+1])
                    if class2:
                        enzyme2 = class2.group(1)
                        class3 = re.match(r"\s*(\S.*)$", lines[i+2])
                        if class3:
                            enzyme3 = class3.group(1)
                        else:
                            enzyme3 = ''
                    else:
                        enzyme2 = ''
                    w.write(enzyme_id + "\t" + enzyme1 + "\t" + enzyme2 + "\t" + enzyme3 + "\t" + enzyme4 + "\n")

    def add_kegg_gene(self, kegg_gene):
        """往mongo导入kegg_gene"""
        with open(kegg_gene, "rb") as f:
            lines = f.readlines()
            data_list = []
            for line in lines:
                m = re.match(r"ko:(K\d{5})\s*(.*:.*)$", line)
                if m:
                    ko_id = m.group(1)
                    # if ko_id not in self.ko_ids:
                    #     self.ko_ids.append(ko_id)
                    gene_id = m.group(2)
                    insert_data = {
                        "koid": ko_id,
                        "gene_id": gene_id
                    }
                    data_list.append(insert_data)
            try:
                collection = self.mongodb["kegg_gene_test"]
                collection.insert_many(data_list)
            except:
                raise Exception("导入kegg_gene_new出错")

    def add_kegg_pathway_png(self, pathway_png, ko_xml, n_ko_xml):
        """往mongo里导入kegg_pathway_png"""
        data_list = []
        fs = gridfs.GridFS(self.mongodb)
        for i in os.listdir(pathway_png):
            m = re.match(r"(.+).png", i)
            if m:
                pathway_id = m.group(1)
                # if pathway_id not in self.pathway_ids:
                #     self.pathway_ids.append(pathway_id)
                png_dir = pathway_png + "/" + pathway_id + ".png"
                png_id = fs.put(open(png_dir, "rb"))
                xml_dir = ko_xml + "/" + pathway_id + ".xml"
                n_xml_dir = n_ko_xml + "/" + pathway_id + ".xml"
                if os.path.exists(xml_dir):
                    xml_id = fs.put(open(xml_dir, "rb"))
                else:
                    xml_id = fs.put(open(n_xml_dir, "rb"))
                insert_data = {
                    "pathway_id": pathway_id,
                    "pathway_ko_kgml": xml_id,
                    "pathway_ko_png": png_id
                }
                data_list.append(insert_data)
        try:
            collection = self.mongodb["kegg_pathway_png_test"]
            collection.insert_many(data_list)
        except:
            raise Exception("导入kegg_pathway_png_new出错")

    def add_kegg_ko(self, ko_path, pathway_path, enzyme_path, module_path):
        """往mongo里导入kegg_ko"""
        with open(ko_path, "rb") as k, open(pathway_path, "rb") as p, open(enzyme_path, "rb") as e, open(module_path, "rb") as m:
            k_lines = k.readlines()
            p_lines = p.readlines()
            e_lines = e.readlines()
            m_lines = m.readlines()
            data_list = []
            for k in k_lines[1:]:
                k = k.strip().split("\t")
                ko_id = k[0]
                # if ko_id not in self.ko_ids:
                #     print "此koid:{}不在kegg_gene里".format(ko_id)
                ko_name = k[1]
                try:
                    ko_definition = k[5]
                except:
                    ko_definition = ""
                try:
                    pathway_ids = k[2]
                except:
                    pathway_ids = []
                try:
                    enzyme_ids = k[3]
                except:
                    enzyme_ids = []
                try:
                    module_ids = k[4]
                except:
                    module_ids = []
                pathway_categories, enzyme_categories, module_categories = [], [], []
                if pathway_ids:
                    pathway_ids = k[2].split(";")
                    for pathway_id in pathway_ids:
                        # if pathway_id not in self.pathway_ids:
                        #     print "此pathway_id：{}不在kegg_pathway_png里".format(pathway_id)
                        pathway_category = []
                        for p in p_lines[1:]:
                            p = p.strip().split("\t")
                            if pathway_id == p[0]:
                                pathway_category.append(p[1])
                                pathway_category.append(p[2])
                                pathway_category.append(p[3])
                        if pathway_category:
                            pathway_categories.append(pathway_category)
                        else:
                            print ko_id + "\t" + pathway_id + "没有找到pathway_id的category"
                if enzyme_ids:
                    enzyme_ids = k[3].split(";")
                    for enzyme_id in enzyme_ids:
                        enzyme_category = []
                        for e in e_lines[1:]:
                            e = e.strip().split("\t")
                            if enzyme_id == e[0]:
                                enzyme_category.append(e[1])
                                enzyme_category.append(e[2])
                                enzyme_category.append(e[3])
                                enzyme_category.append(e[4])
                        if enzyme_category:
                            enzyme_categories.append(enzyme_category)
                        else:
                            print ko_id + "\t" + enzyme_id + "没有找到enzyme_id的category"
                if module_ids:
                    module_ids = k[4].split(";")
                    for module_id in module_ids:
                        module_category = []
                        for m in m_lines[1:]:
                            m = m.strip().split("\t")
                            if module_id == m[0]:
                                module_category.append(m[1])
                                module_category.append(m[2])
                                module_category.append(m[3])
                                module_category.append(m[4])
                        if module_category:
                            module_categories.append(module_category)
                        else:
                            print ko_id + "\t" + module_id + "没有找到module_id的category"
                insert_data = {
                    "ko_id": ko_id,
                    "ko_name": ko_name,
                    "ko_desc": ko_definition,
                    "pathway_id": pathway_ids,
                    "pathway_category": enzyme_categories,
                    "enzyme_id": enzyme_ids,
                    "enzyme_category": enzyme_categories,
                    "module_id": module_ids,
                    "module_category": module_categories,
                    "create_ts": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                data_list.append(insert_data)
            try:
                collection = self.mongodb["kegg_ko_test"]
                collection.insert_many(data_list)
            except:
                raise Exception("导入kegg_ko_new出错")

if __name__ == "__main__":
    a = kegg()
    ko = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/genes/ko/ko"
    ko_path = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/files/ko.txt"
    pathway = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/pathway/pathway.list"
    pathway_path = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/files/pathway.txt"
    module = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/module/module"
    module_path = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/files/module.txt"
    enzyme = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/ligand/enzyme/enzyme"
    enzyme_path = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/files/enzyme.txt"
    a.ko_split(ko, ko_path)
    a.pathway_category(pathway, pathway_path)
    a.module_category(module, module_path)
    a.enzyme_category(enzyme, enzyme_path)
    kegg_gene = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/genes/ko/ko_genes.list"
    ko_xml = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/xml/kgml/metabolic/ko"
    n_ko_xml = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/xml/kgml/non-metabolic/ko"
    pathway_png = "/mnt/ilustre/users/sanger-dev/app/database/KEGG/kegg_2017-05-01/kegg/pathway/ko"
    a.add_kegg_gene(kegg_gene)
    a.add_kegg_pathway_png(pathway_png, ko_xml, n_ko_xml)
    a.add_kegg_ko(ko_path, pathway_path, enzyme_path, module_path)
