# -*- coding: utf-8 -*-
# _author_ = "zengjing"
import collections
import re
import os
import subprocess
from biocluster.config import Config


class Transcript(object):
    def __init__(self):
        self.name = ''
        self.gene_id = ''
        self.gene_name = ''
        self.length = ''
        self.cog = ''
        self.nog = ''
        self.cog_ids = ''
        self.nog_ids = ''
        self.go = ''
        self.ko_id = ''
        self.ko_name = ''
        self.pathway = ''
        self.pfam = ''
        self.pfam_evalue = ''
        self.nr = ''
        self.swissprot = ''


class RefAnnoQuery(object):
    def __init__(self):
        self.stat_info = {}
        self.cog_string = Config().biodb_mongo_client.sanger_biodb.COG
        self.cog_string_v9 = Config().biodb_mongo_client.sanger_biodb.COG_V9
        self.kegg_ko = Config().biodb_mongo_client.sanger_biodb.kegg_ko_v1
        self.go = Config().biodb_mongo_client.sanger_biodb.GO
        self.gloabl = ["map01100", "map01110", "map01120", "map01130", "map01200", "map01210", "map01212", "map01230", "map01220"]

    def get_anno_stat(self, outpath, cog_list=None, gos_list=None, org_kegg=None, anno_type="transcript"):
        self.get_cog(cog_list=cog_list)
        self.get_go(gos_list=gos_list)
        self.get_kegg(org_kegg=org_kegg)
        with open(outpath, "w") as w:
            if anno_type == "transcript":
                head = 'transcript\tgene_id\tgene_name\tlength\tcog\tnog\tcog_description\tnog_description\tKO_id\tKO_name\tpaths\tpfam\tgo\tnr\tswissprot\n'
                w.write(head)
                for name in self.stat_info:
                    w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].name, self.stat_info[name].gene_id, self.stat_info[name].gene_name,
                            self.stat_info[name].length, self.stat_info[name].cog, self.stat_info[name].nog, self.stat_info[name].cog_ids, self.stat_info[name].nog_ids,
                            self.stat_info[name].ko_id, self.stat_info[name].ko_name, self.stat_info[name].pathway, self.stat_info[name].pfam,
                            self.stat_info[name].go, self.stat_info[name].nr, self.stat_info[name].swissprot))
            if anno_type == "gene":
                head = 'gene_id\tgene_name\tcog\tnog\tcog_description\tnog_description\tKO_id\tKO_name\tpaths\tpfam\tgo\tnr\tswissprot\n'
                w.write(head)
                for name in self.stat_info:
                    w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].name, self.stat_info[name].gene_name,
                            self.stat_info[name].cog, self.stat_info[name].nog, self.stat_info[name].cog_ids, self.stat_info[name].nog_ids,
                            self.stat_info[name].ko_id, self.stat_info[name].ko_name, self.stat_info[name].pathway, self.stat_info[name].pfam,
                            self.stat_info[name].go, self.stat_info[name].nr, self.stat_info[name].swissprot))

    def get_cog(self, cog_list):
        """找到转录本ID对应的cogID、nogID、kogID及功能分类和描述"""
        with open(cog_list, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                cog = line[1]
                nog = line[2]
                if query_name in self.stat_info:
                    self.stat_info[query_name].cog = self.get_cog_group_categories(cog)[0]
                    self.stat_info[query_name].nog = self.get_cog_group_categories(nog)[0]
                    self.stat_info[query_name].cog_ids = self.get_cog_group_categories(cog)[1]
                    self.stat_info[query_name].nog_ids = self.get_cog_group_categories(nog)[1]
                else:
                    query = Transcript()
                    query.name = query_name
                    query.cog = self.get_cog_group_categories(cog)[0]
                    query.nog = self.get_cog_group_categories(nog)[0]
                    query.cog_ids = self.get_cog_group_categories(cog)[1]
                    query.nog_ids = self.get_cog_group_categories(nog)[1]
                    self.stat_info[query_name] = query

    def get_cog_group_categories(self, group):
        """找到cog/nog/kogID对应的功能分类及功能分类描述、cog描述"""
        group = group.split(";")
        funs, ids = [], []
        for item in group:
            if item:
                result = self.cog_string.find_one({'cog_id': item})
                if result:
                    group = result["cog_categories"]
                    group_des = result["categories_description"]
                    cog_des = result["cog_description"]
                    cog_fun = item + "(" + group + ":" + group_des + ")"
                    cog_id = item + "(" + cog_des + ")"
                    funs.append(cog_fun)
                    ids.append(cog_id)
        funs = "; ".join(funs)
        ids = "; ".join(ids)
        return funs, ids

    def get_go(self, gos_list):
        """找到转录本ID对应的goID及term、term_type"""
        with open(gos_list, 'rb') as r:
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                go = line[1].split(";")
                gos = []
                for i in go:
                    result = self.go.find_one({"go_id": i})
                    if result:
                        item = i + "(" + result["ontology"] + ":" + result["name"] + ")"
                        gos.append(item)
                gos = "; ".join(gos)
                if query_name in self.stat_info:
                    self.stat_info[query_name].go = gos
                else:
                    query = Transcript()
                    query.name = query_name
                    query.go = gos
                    self.stat_info[query_name] = query

    def get_kegg(self, org_kegg):
        "找到转录本ID对应的KO、KO_name、Pathway、Pathway_definition"
        with open(org_kegg, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                ko_id = line[1]
                result = self.kegg_ko.find_one({"ko_id": ko_id})
                if result:
                    ko_name = result["ko_name"]
                else:
                    ko_name = ''
                    print "没找到ko_id {}".fromat(ko_id)
                pathway = []
                try:
                    pathways = line[2].split(";")
                except:
                    pathways = []
                    print "{} 没有pathway".format(query_name)
                for p in pathways:
                    m = re.match(r".+(\d{5})", p)
                    if m:
                        map_id = "map" + m.group(1)
                        if map_id not in self.gloabl:
                            pid = re.sub("map", "ko", map_id)
                            result = self.kegg_ko.find_one({"pathway_id": {"$in": [pid]}})
                            if result:
                                pids = result["pathway_id"]
                                for index, i in enumerate(pids):
                                    if i == pid:
                                        category = result["pathway_category"][index]
                                        definition = category[2]
                                        item = map_id + "(" + definition + ")"
                                        pathway.append(item)
                            else:
                                print "{} 没有在mongo中找到该pathway".format(pid)
                pathway = "; ".join(pathway)
                if query_name in self.stat_info:
                    self.stat_info[query_name].ko_id = ko_id
                    self.stat_info[query_name].ko_name = ko_name
                    self.stat_info[query_name].pathway = pathway
                else:
                    query = Transcript()
                    query.name = query_name
                    query.ko_id = ko_id
                    query.ko_name = ko_name
                    query.pathway = pathway
                    self.stat_info[query_name] = query

    # def get_gene_name(self, )

if __name__ == "__main__":
    Transcript()
    test = RefAnnoQuery()
    outpath = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/stat/ref_anno.xls"
    cog_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/cog/mmu_cog.txt"
    gos_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/go/mmu_go.txt"
    org_kegg = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/kegg/hsa_kegg.txt"
    test.get_anno_stat(outpath=outpath, cog_list=cog_list, gos_list=gos_list, org_kegg=org_kegg, anno_type="gene")
