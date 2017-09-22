# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import regex
import re
from biocluster.config import Config
import sys


class Transcript(object):
    def __init__(self):
        self.name = ''
        self.gene_id = ''
        self.gene_name = ''
        self.length = ''
        self.nr = ''
        self.swissprot = ''
        self.cog = ''
        self.nog = ''
        self.cog_ids = ''
        self.nog_ids = ''
        self.go = ''
        self.ko_id = ''
        self.ko_name = ''
        self.pathway = ''
        self.pfam = []


class AllAnnoStat(object):
    def __init__(self):
        self.stat_info = {}
        self.gene_names = {}
        self.cog_string = Config().biodb_mongo_client.sanger_biodb.COG_V9
        self.kegg_ko = Config().biodb_mongo_client.sanger_biodb.kegg_ko
        self.go = Config().biodb_mongo_client.sanger_biodb.GO
        self.gloabl = ["map01100", "map01110", "map01120", "map01130", "map01200", "map01210", "map01212", "map01230", "map01220"]

    def get_anno_stat(self, tran_outpath, gene_outpath, new_gtf_path, ref_gtf_path, length_path, gene_file, cog_list=None, kegg_table=None, gos_list=None, blast_nr_table=None, blast_swissprot_table=None, pfam_domain=None):
        """
        传入各个数据库的部分注释结果文件，统计功能注释信息表（即应注释查询模块的功能注释信息表）
        tran_outpath：输出结果路径：转录本功能注释信息表的文件路径；gene_outpath：基因功能注释信息表的文件路径
        new_gtf_path：新转录本的gtf文件，提取转录本对应的基因ID
        ref_gtf_path: 参考基因的gtf文件，提取基因ID对应的基因名称
        cog_list：string2cog注释tool统计得到的cog_list.xls,提取cog/nog及对应的功能分类信息
        kegg_table：kegg_annotation注释tool统计得到的kegg_table.xls
        gos_list：go_annotation注释tool统计得到的query_gos.list
        blast_nr_table：blast比对nr库得到的结果文件(blast输出文件格式为6：table)
        blast_swissprot_table: blast比对swissprot库得到的结果文件（blast输出文件格式为6：table）
        pfam_domain: orf预测的结果pfam_domain
        length_path:注释转录本序列长度
        """
        self.get_gene(new_gtf_path=new_gtf_path, ref_gtf_path=ref_gtf_path)
        if blast_nr_table:
            self.get_nr(blast_nr_table=blast_nr_table)
        if blast_swissprot_table:
            self.get_swissprot(blast_swissprot_table=blast_swissprot_table)
        if pfam_domain:
            self.get_pfam(pfam_domain=pfam_domain)
        if gos_list:
            self.get_go(gos_list=gos_list)
        if kegg_table:
            self.get_kegg(kegg_table=kegg_table)
        if cog_list:
            self.get_cog(cog_list=cog_list)
        if length_path:
            self.get_length(length_path=length_path)
        with open(tran_outpath, 'wb') as w:
            head = 'transcript\tgene_id\tgene_name\tlength\tcog\tnog\tcog_description\tnog_description\tKO_id\tKO_name\tpaths\tpfam\tgo\tnr\tswissprot\n'
            w.write(head)
            for name in self.stat_info:
                try:
                    gene_name = self.gene_names[self.stat_info[name].gene_id]
                except:
                    gene_name = ''
                w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].name, self.stat_info[name].gene_id, gene_name,
                        self.stat_info[name].length, self.stat_info[name].cog, self.stat_info[name].nog, self.stat_info[name].cog_ids, self.stat_info[name].nog_ids,
                        self.stat_info[name].ko_id, self.stat_info[name].ko_name, self.stat_info[name].pathway, '; '.join(self.stat_info[name].pfam),
                        self.stat_info[name].go, self.stat_info[name].nr, self.stat_info[name].swissprot))
        gene_list = []
        for line in open(gene_file):
            item = line.strip().split("\t")
            gene_list.append(item[0])
        with open(gene_outpath, 'wb') as w:
            head = 'gene_id\tgene_name\tcog\tnog\tcog_description\tnog_description\tKO_id\tKO_name\tpaths\tpfam\tgo\tnr\tswissprot\n'
            w.write(head)
            for name in gene_list:
                try:
                    gene_name = self.gene_names[self.stat_info[name].gene_id]
                except:
                    gene_name = ''
                w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].gene_id, gene_name,
                        self.stat_info[name].cog, self.stat_info[name].nog, self.stat_info[name].cog_ids, self.stat_info[name].nog_ids,
                        self.stat_info[name].ko_id, self.stat_info[name].ko_name, self.stat_info[name].pathway, '; '.join(self.stat_info[name].pfam),
                        self.stat_info[name].go, self.stat_info[name].nr, self.stat_info[name].swissprot))

    def get_gene(self, new_gtf_path, ref_gtf_path):
        """找到转录本ID对应的基因ID及基因名称"""
        gene_ids = []
        if new_gtf_path:
            gtf_path = new_gtf_path
        else:
            gtf_path = ref_gtf_path
        for line in open(gtf_path):
            content_m = regex.match(
                r'^([^#]\S*?)\t+((\S+)\t+){7}(.*;)*((transcript_id|gene_id)\s+?\"(\S+?)\");.*((transcript_id|gene_id)\s+?\"(\S+?)\");(.*;)*$',
                line.strip())
            if content_m:
                if 'transcript_id' in content_m.captures(6):
                    query_name = content_m.captures(7)[0]
                    gene_id = content_m.captures(10)[0]
                else:
                    query_name = content_m.captures(10)[0]
                    gene_id = content_m.captures(7)[0]
                if query_name not in self.stat_info:
                    query = Transcript()
                    query.name = query_name
                    query.gene_id = gene_id
                    self.stat_info[query_name] = query
                    gene_ids.append(gene_id)
        gene_ids = list(set(gene_ids))
        if ref_gtf_path:
            gtf_path = ref_gtf_path
        else:
            gtf_path = new_gtf_path
        for line in open(gtf_path):
            m = re.match(r".+gene_id \"(.+?)\"; .*gene_name \"(.+?)\";.*$", line)
            if m:
                gene_id = m.group(1)
                gene_name = m.group(2)
                if gene_id in gene_ids:
                    self.gene_names[gene_id] = gene_name

    def get_kegg(self, kegg_table):
        "找到转录本ID对应的KO、KO_name、Pathway、Pathway_definition"
        with open(kegg_table, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                ko_id = line[1]
                ko_name = line[2]
                pathways = line[4].split(";")
                pathway = []
                for map_id in pathways:
                    try:
                        if map_id not in self.gloabl:
                            pid = re.sub("map", "ko", map_id)
                            result = self.kegg_ko.find_one({"pathway_id": {"$in": [pid]}})
                            pids = result["pathway_id"]
                            for index, i in enumerate(pids):
                                if i == pid:
                                    category = result["pathway_category"][index]
                                    definition = category[2]
                                    item = map_id + "(" + definition + ")"
                                    pathway.append(item)
                    except:
                        print "{}在该物种中没有pathway".format(query_name)
                pathway = "; ".join(pathway)
                if query_name in self.stat_info:
                    self.stat_info[query_name].ko_id = ko_id
                    self.stat_info[query_name].ko_name = ko_name
                    self.stat_info[query_name].pathway = pathway

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

    def get_cog(self, cog_list):
        """找到转录本ID对应的cogID、nogID、kogID及功能分类和描述"""
        with open(cog_list, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                cog = line[1]
                nog = line[2]
                if query_name in self.stat_info:
                    self.stat_info[query_name].cog = self.get_cog_group_categories(cog)[0]
                    self.stat_info[query_name].nog = self.get_cog_group_categories(nog)[0]
                    self.stat_info[query_name].cog_ids = self.get_cog_group_categories(cog)[1]
                    self.stat_info[query_name].nog_ids = self.get_cog_group_categories(nog)[1]

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

    def get_nr(self, blast_nr_table):
        """找到转录本ID对应NR库的最佳hit_name和描述"""
        with open(blast_nr_table, 'rb') as f:
            f.readline()
            flag = None
            for line in f:
                line = line.strip('\n').split('\t')
                query_name = line[5]
                if flag == query_name:
                    pass
                else:
                    nr_hit_name = line[10]
                    nr_description = line[-1]
                    nr = nr_hit_name + "(" + nr_description + ")"
                    flag = query_name
                    if query_name in self.stat_info:
                        self.stat_info[query_name].nr = nr
                        self.stat_info[query_name].length = line[6]

    def get_swissprot(self, blast_swissprot_table):
        """找到转录本ID对应swissprot库的最佳hit_name及描述"""
        with open(blast_swissprot_table, 'rb') as f:
            f.readline()
            flag = None
            for line in f:
                line = line.strip('\n').split('\t')
                query_name = line[5]
                if flag == query_name:
                    pass
                else:
                    swissprot_hit_name = line[10]
                    swissprot_description = line[-1]
                    swissprot = swissprot_hit_name + "(" + swissprot_description + ")"
                    flag = query_name
                    if query_name in self.stat_info:
                        self.stat_info[query_name].swissprot = swissprot
                        self.stat_info[query_name].length = line[6]

    def get_pfam(self, pfam_domain):
        """找到转录本ID对应的最佳pfamID及domain、domain_description"""
        with open(pfam_domain, "rb") as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                query_name = line[0]
                pfam_id = line[2]
                domain = line[3]
                domain_description = line[4]
                pfam = pfam_id + "(" + domain + ":" + domain_description + ")"
                if query_name in self.stat_info:
                    if pfam not in self.stat_info[query_name].pfam:
                        pfams = self.stat_info[query_name].pfam
                        pfams.append(pfam)
                        self.stat_info[query_name].pfam = pfams

    def get_length(self, length_path):
        """每个转录本id对应的序列长度"""
        for line in open(length_path, "rb"):
            line = line.strip().split(" ")
            tran_id = line[1]
            length = line[0]
            if tran_id in self.stat_info:
                self.stat_info[tran_id].length = length

if __name__ == '__main__':
    Transcript()
    if sys.argv[3] == 'None':
        sys.argv[3] = None
    if sys.argv[7] == 'None':
        sys.argv[7] = None
    if sys.argv[8] == 'None':
        sys.argv[8] = None
    if sys.argv[9] == 'None':
        sys.argv[9] = None
    if sys.argv[10] == 'None':
        sys.argv[10] = None
    if sys.argv[11] == 'None':
        sys.argv[11] = None
    if sys.argv[12] == 'None':
        sys.argv[12] = None
    AllAnnoStat().get_anno_stat(tran_outpath=sys.argv[1], gene_outpath=sys.argv[2], new_gtf_path=sys.argv[3], ref_gtf_path=sys.argv[4], length_path=sys.argv[5], gene_file=sys.argv[6], cog_list=sys.argv[7], kegg_table=sys.argv[8], gos_list=sys.argv[9], blast_nr_table=sys.argv[10], blast_swissprot_table=sys.argv[11], pfam_domain=sys.argv[12])
