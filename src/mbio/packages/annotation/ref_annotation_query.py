# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
# from mbio.packages.annotation.transcript_gene import transcript_gene
import regex
import re


class Transcript(object):
    def __init__(self):
        self.name = ''
        self.gene_id = ''
        self.gene_name = ''
        self.length = ''
        self.nr_taxon = ''
        self.nr_hit_name = []
        self.nr_description = []
        self.swissprot_hit_name = []
        self.swissprot_description = []
        self.cog = ''
        self.nog = ''
        self.kog = ''
        self.go = ''
        self.ko_id = ''
        self.ko_name = ''
        self.pathway = ''
        self.pfam_id = []
        self.domain = []


class AllAnnoStat(object):
    def __init__(self):
        self.stat_info = {}

    def get_anno_stat(self, outpath, gtf_path=None, cog_list=None, kegg_table=None, gos_list=None, blast_nr_table=None, blast_swissprot_table=None, pfam_domain=None):
        """
        传入各个数据库的部分注释结果文件，统计功能注释信息表（即应注释查询模块的功能注释信息表）
        outpath：输出结果路径：功能注释信息表的文件路径
        gtf_path：gtf文件，提取对应的基因ID和基因名称
        cog_list：string2cog注释tool统计得到的query_cog_list.xls
        kegg_table：kegg_annotation注释tool统计得到的kegg_table.xls
        gos_list：go_annotation注释tool统计得到的query_gos.list
        blast_nr_table：blast比对nr库得到的结果文件（blast输出文件格式为6：table）
        nr_taxons：denovo_anno_stat统计tool得到的结果文件nr_taxons.xls
        blast_swissprot_table: blast比对swissprot库得到的结果文件（blast输出文件格式为6：table）
        pfam_domain: orf预测的结果pfam_domain
        """
        if gos_list:
            self.get_go(gos_list=gos_list)
        if kegg_table:
            self.get_kegg(kegg_table=kegg_table)
        if cog_list:
            self.get_cog(cog_list=cog_list)
        if blast_nr_table:
            self.get_nr(blast_nr_table=blast_nr_table)
        if blast_swissprot_table:
            self.get_swissprot(blast_swissprot_table=blast_swissprot_table)
        if pfam_domain:
            self.get_pfam(pfam_domain=pfam_domain)
        if gtf_path:
            self.get_gene(gtf_path=gtf_path)
        with open(outpath, 'wb') as w:
            head = 'transcript\tgene_id\tgene_name\tlength\tCOG\tNOG\tKOG\tKO_id\tKO_name\tpaths\tpfam_ids\tdomains\tgo_ids\tnr_hit_name\tswissprot_hit_name\n'
            w.write(head)
            for name in self.stat_info:
                w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].name, self.stat_info[name].gene_id, self.stat_info[name].gene_name,
                        self.stat_info[name].length, self.stat_info[name].cog, self.stat_info[name].nog, self.stat_info[name].kog, self.stat_info[name].ko_id, self.stat_info[name].ko_name,
                        self.stat_info[name].pathway, ';'.join(self.stat_info[name].pfam_id), ';'.join(self.stat_info[name].domain),self.stat_info[name].go, ';'.join(self.stat_info[name].nr_hit_name),
                        ';'.join(self.stat_info[name].swissprot_hit_name)))

    def get_gene(self, gtf_path):
        """找到转录本ID对应的基因ID及基因名称"""
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
                if query_name in self.stat_info:
                    self.stat_info[query_name].gene_id = gene_id
            m = re.match(r".+transcript_id \"(.+?)\";.*gene_name \"(.+?)\";.*$", line)
            if m:
                query_name = m.group(1)
                gene_name = m.group(2)
                if query_name in self.stat_info:
                    self.stat_info[query_name].gene_name = gene_name

    def get_kegg(self, kegg_table):
        with open(kegg_table, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                ko_id = line[1]
                ko_name = line[2]
                pathway = line[4]
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

    def get_go(self, gos_list):
        with open(gos_list, 'rb') as r:
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                go = line[1]
                if query_name in self.stat_info:
                    self.stat_info[query_name].go = go
                else:
                    query = Transcript()
                    query.name = query_name
                    query.go = go
                    self.stat_info[query_name] = query

    def get_cog(self, cog_list):
        with open(cog_list, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                cog = line[1]
                nog = line[2]
                kog = line[3]
                if query_name in self.stat_info:
                    self.stat_info[query_name].cog = cog
                    self.stat_info[query_name].nog = nog
                    self.stat_info[query_name].kog = kog
                else:
                    query = Transcript()
                    query.name = query_name
                    query.cog = cog
                    query.nog = nog
                    query.kog = kog
                    self.stat_info[query_name] = query

    def get_nr(self, blast_nr_table):
        """
        """
        with open(blast_nr_table, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip('\n').split('\t')
                query_name = line[5]
                if query_name in self.stat_info:
                    if line[10] not in self.stat_info[query_name].nr_hit_name:
                        self.stat_info[query_name].nr_hit_name.append(line[10])
                        self.stat_info[query_name].nr_description.append(line[-1])
                        self.stat_info[query_name].length = line[6]
                else:
                    query = Transcript()
                    query.name = query_name
                    nr_hit_name = []
                    nr_hit_name.append(line[10])
                    query.nr_hit_name = nr_hit_name
                    nr_desc = []
                    nr_desc.append(line[-1])
                    query.nr_description= nr_desc
                    query.length = line[6]
                    self.stat_info[query_name] = query

    def get_swissprot(self, blast_swissprot_table):
        """
        """
        with open(blast_swissprot_table, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip('\n').split('\t')
                query_name = line[5]
                if query_name in self.stat_info:
                    if line[10] not in self.stat_info[query_name].swissprot_hit_name:
                        self.stat_info[query_name].swissprot_hit_name.append(line[10])
                        self.stat_info[query_name].swissprot_description.append(line[-1])
                        self.stat_info[query_name].length = line[6]
                else:
                    query = Transcript()
                    query.name = query_name
                    sw_hit_name = []
                    sw_hit_name.append(line[10])
                    query.swissprot_hit_name = sw_hit_name
                    sw_desc = []
                    sw_desc.append(line[-1])
                    query.swissprot_description = sw_desc
                    query.length = line[6]
                    self.stat_info[query_name] = query

    def get_pfam(self, pfam_domain):
        """
        """
        with open(pfam_domain, "rb") as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                query_name = line[0]
                if query_name in self.stat_info:
                    if line[2] not in self.stat_info[query_name].pfam_id:
                        self.stat_info[query_name].pfam_id.append(line[2])
                        self.stat_info[query_name].domain.append(line[3])
                else:
                    query = Transcript()
                    query.name = query_name
                    pfam_ids, domains = [], []
                    pfam_ids.append(line[2])
                    domains.append(line[3])
                    query.pfam_id = pfam_ids
                    query.domain = domains
                    self.stat_info[query_name] = query
