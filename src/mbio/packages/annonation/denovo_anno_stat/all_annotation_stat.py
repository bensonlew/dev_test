# -*- coding: utf-8 -*-
# __author__ = 'qiuping'


class Transcript(object):
    def __init__(self):
        self.name = ''
        self.gene_name = ''
        self.nr_taxon = ''
        self.nr_hit_name = ''
        self.cogs = ''
        self.go = ''
        self.ko_id = ''
        self.ko_name = ''
        self.pathway = ''


class AllAnnoStat(object):
    def __init__(self):
        self.stat_info = {}

    def get_anno_stat(self, outpath, gene_list=None, cog_list=None, kegg_table=None, gos_list=None, blast_nr_table=None, nr_taxons=None):
        """
        传入各个数据库的部分注释结果文件，统计功能注释信息表（即应注释查询模块的功能注释信息表）
        outpath：输出结果路径：功能注释信息表的文件路径
        gene_list：装有基因序列名称的列表[g1,g2,...,g]
        cog_list：string2cog注释tool统计得到的query_cog_list.xls
        kegg_table：kegg_annotation注释tool统计得到的kegg_table.xls
        gos_list：go_annotation注释tool统计得到的query_gos.list
        blast_nr_table：blast比对nr库得到的结果文件（blast输出文件格式为6：table）
        nr_taxons：denovo_anno_stat统计tool得到的结果文件nr_taxons.xls
        """
        if gos_list:
            self.get_go(gos_list=gos_list, gene_list=gene_list)
        if kegg_table:
            self.get_kegg(kegg_table=kegg_table, gene_list=gene_list)
        if cog_list:
            self.get_cog(cog_list=cog_list, gene_list=gene_list)
        if blast_nr_table and nr_taxons:
            self.get_nr(blast_nr_table=blast_nr_table, nr_taxons=nr_taxons, gene_list=gene_list)
        with open(outpath, 'wb') as w:
            head = 'transcript\tgene\tnr_hit_name\tnr_taxon\tcog_id\tgo_id\tko_id\tko_name\tkegg_pathway\n'
            w.write(head)
            for name in self.stat_info:
                w.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.stat_info[name].name, self.stat_info[name].gene_name, self.stat_info[name].nr_hit_name, self.stat_info[name].nr_taxon, self.stat_info[name].cogs, self.stat_info[name].go, self.stat_info[name].ko_id, self.stat_info[name].ko_name, self.stat_info[name].pathway))

    def get_kegg(self, kegg_table, gene_list):
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
                    if query_name in gene_list:
                        query.gene_name = query_name.split('_i')[0]
                    self.stat_info[query_name] = query

    def get_go(self, gos_list, gene_list):
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
                    if query_name in gene_list:
                        query.gene_name = query_name.split('_i')[0]
                    self.stat_info[query_name] = query

    def get_cog(self, cog_list, gene_list):
        with open(cog_list, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                cog = line[1:3]
                cog.remove('')
                if len(cog) == 1:
                    cogs = cog[0]
                else:
                    cogs = ','.join(cog)
                if query_name in self.stat_info:
                    self.stat_info[query_name].cogs = cogs
                else:
                    query = Transcript()
                    query.name = query_name
                    query.cogs = cogs
                    if query_name in gene_list:
                        query.gene_name = query_name.split('_i')[0]
                    self.stat_info[query_name] = query

    def get_nr(self, blast_nr_table, nr_taxons, gene_list):
        """
        """
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
                    if query_name in self.stat_info:
                        self.stat_info[query_name].nr_hit_name = nr_hit_name
                    else:
                        query = Transcript()
                        query.name = query_name
                        query.nr_hit_name = nr_hit_name
                        if query_name in gene_list:
                            query.gene_name = query_name.split('_i')[0]
                        self.stat_info[query_name] = query
        with open(nr_taxons, 'rb') as r:
            for line in r:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                taxon = line[1]
                if query_name in self.stat_info:
                    self.stat_info[query_name].nr_taxon = taxon

# gene_list = []
# with open('/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/rna_anno/gene_list.txt', 'rb') as f:
#     for line in f:
#         gene_list.append(line.strip('\n'))
# test = AllAnnoStat()
# test.get_anno_stat(outpath='/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/rna_anno/all_anno_stat.xls', gene_list=gene_list, cog_list='/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/rna_anno/cog_anno/cog_list.xls', kegg_table='/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/rna_anno/kegg_anno/kegg_table.xls', gos_list='/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/rna_anno/go_anno/query_gos.list', blast_nr_table='/mnt/ilustre/users/sanger-dev/sg-users/hesheng/test/test_file/blast_result/Trinity_vs_nr.xmltmp.xls', nr_taxons='/mnt/ilustre/users/sanger-dev/workspace/20161025/Single_denovo_anno_stat/DenovoAnnoStat/output/ncbi_taxonomy/query_taxons.xls')
