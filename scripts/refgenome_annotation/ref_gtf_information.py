# -*- coding: utf-8 -*-
# _author_ = "zengjing"
import collections
import re
import regex
import os
import gridfs
import subprocess
from biocluster.config import Config
from itertools import islice


class GtfFilt(object):
    """
    通过gtf对文件注释进行筛选,提取transcrpt_id对应的基因ID，转录长度，基因名称信息
    """
    def __init__(self):
        self.tran_genes = {}
        self.gene_names = {}
        self.tran_lengths = {}
        self.tran_ids = []
        self.gene_ids = []

    def get_gtf_information(self, gtf_path, biomart_path):
        self.tran_genes, self.gene_names, self.tran_lengths = {}, {}, {}
        for line in open(gtf_path):
            content_m = regex.match(
                r'^([^#]\S*?)\t+((\S+)\t+){7}(.*;)*((transcript_id|gene_id)\s+?\"(\S+?)\");.*((transcript_id|gene_id)\s+?\"(\S+?)\");(.*;)*$',
                line.strip())
            if content_m:
                if 'transcript_id' in content_m.captures(6):
                    tran_id = content_m.captures(7)[0]
                    gene_id = content_m.captures(10)[0]
                else:
                    tran_id = content_m.captures(10)[0]
                    gene_id = content_m.captures(7)[0]
                if tran_id not in self.tran_genes:
                    self.tran_genes[tran_id] = gene_id
                self.tran_ids.append(tran_id)
                self.gene_ids.append(gene_id)
            m = re.match(r".+gene_id \"(.+?)\"; .*gene_name \"(.+?)\";.*$", line)
            if m:
                gene_id = m.group(1)
                name = m.group(2)
                self.gene_names[gene_id] = name
        self.tran_ids = list(set(self.tran_ids))
        self.gene_ids = list(set(self.gene_ids))
        print len(self.tran_ids)
        print len(self.gene_ids)
        # for line in open(biomart_path):
        #     item = line.strip().split("\t")
        #     try:
        #         tran_id = item[1]
        #     except:
        #         pass
        #     try:
        #         length = item[-3]
        #     except:
        #         pass
        #     self.tran_lengths[tran_id] = length
        # test = open("tran_information.list", "w")
        # for tran_id in self.tran_genes:
        #     gene_id = self.tran_genes[tran_id]
        #     length = self.tran_lengths[tran_id]
        #     try:
        #         name = self.gene_names[gene_id]
        #     except:
        #         name = "\t"
        #     test.write(tran_id + "\t" + length + "\t" + gene_id + "\t" + name + "\n")

    def kegg_filt(self, kegg_db, outdir):
        gene_kos, gene_maps = {}, {}
        gene_ids = []
        with open(kegg_db, "r") as f, open(outdir + "/kegg_trans.list", "w") as w1, open(outdir + "/kegg_genes.list", "w") as w2:
            lines = f.readlines()
            for line in lines:
                if line.startswith("#"):
                    pass
                else:
                    item = line.strip().split("\t")
                    gene_id = item[0]
                    tran_id = item[1]
                    gene_ids.append(gene_id)
                    try:
                        koid = item[3]
                        if gene_id not in gene_kos:
                            gene_kos[gene_id] = []
                        gene_kos[gene_id].append(koid)
                    except:
                        koid = '\t'
                    try:
                        map_ids = ';'.join(["map" + i for i in item[4].split(";")])
                        if gene_id not in gene_maps:
                            gene_maps[gene_id] = []
                        for i in item[4].split(";"):
                            gene_maps[gene_id].append("map" + i)
                    except:
                        map_ids = '\t'
                    if tran_id in self.tran_ids:
                        w1.write(tran_id + "\t" + koid + "\t" + map_ids + "\n")
                    else:
                        print tran_id + " 该转录本不在gtf内"
            gene_ids = list(set(gene_ids))
            for gene_id in gene_ids:
                if gene_id in self.gene_ids:
                    try:
                        koids = ';'.join(list(set(gene_kos[gene_id])))
                    except:
                        koids = '\t'
                    try:
                        map_ids = ';'.join(list(set(gene_maps[gene_id])))
                    except:
                        map_ids = '\t'
                    w2.write(gene_id + "\t" + koids + "\t" + map_ids + "\n")

    def go_filt(self, go_db, outdir):
        gene_ids = []
        gene_gos = {}
        with open(go_db, "r") as f, open(outdir + "/go_trans.list", "w") as w1, open(outdir + "/go_genes.list", "w") as w2:
            lines = f.readlines()
            for line in lines:
                if line.startswith("#"):
                    pass
                else:
                    item = line.strip().split("\t")
                    gene_id = item[0]
                    tran_id = item[1]
                    gene_ids.append(gene_id)
                    if gene_id not in gene_gos:
                        gene_gos[gene_id] = []
                    for i in item[2].split(";"):
                        gene_gos[gene_id].append(i)
                    if tran_id in self.tran_ids:
                        w1.write(tran_id + "\t" + item[2] + "\n")
                    else:
                        print tran_id + " 该转录本没在gtf内"
            gene_ids = list(set(gene_ids))
            for gene_id in gene_ids:
                gos = ';'.join(list(set(gene_gos[gene_id])))
                if gene_id in self.gene_ids:
                    w2.write(gene_id + "\t" + gos + "\n")
                else:
                    print gene_id + " 该基因没有在gtf内"

    def cog_filt(self, cog_db, outdir):
        gene_ids = []
        gene_cogs, gene_nogs = {}, {}
        with open(cog_db, "r") as f, open(outdir + "/cog_trans.list", "w") as w1, open(outdir + "/cog_genes.list", "w") as w2:
            lines = f.readlines()
            for line in lines:
                if line.startswith("#"):
                    pass
                else:
                    item = line.strip().split("\t")
                    gene_id = item[0]
                    tran_id = item[1]
                    try:
                        cogs = item[3].split(";")
                    except:
                        cogs = []
                    try:
                        nogs = item[4].split(";")
                    except:
                        nogs = []
                    if gene_id not in gene_ids:
                        gene_ids.append(gene_id)
                        gene_cogs[gene_id] = []
                        gene_nogs[gene_id] = []
                    for i in cogs:
                        gene_cogs[gene_id].append(i)
                    for j in nogs:
                        gene_nogs[gene_id].append(j)
                    if tran_id in self.tran_ids:
                        w1.write(tran_id + "\t" + ';'.join(cogs) + "\t" + ';'.join(nogs) + "\n")
                    else:
                        print tran_id + " 该转录本没有在gtf内"
            for gene_id in gene_ids:
                if gene_id in self.gene_ids:
                    cogs = ';'.join(list(set(gene_cogs[gene_id])))
                    nogs = ';'.join(list(set(gene_nogs[gene_id])))
                    w2.write(gene_id + "\t" + cogs + "\t" + nogs + "\n")



if __name__ == "__main__":
    a = GtfFilt()
    gtf_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/gtf/Arabidopsis_thaliana.TAIR10.36.gtf"
    biomart_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/biomart/Arabidopsis_thaliana.TAIR10.biomart.txt"
    a.get_gtf_information(gtf_path, biomart_path)
    # kegg_db = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/KEGG/Arabidopsis_thaliana.pathway"
    # outdir = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome"
    # # a.kegg_filt(kegg_db, outdir)
    # go_db = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/GO/Arabidopsis_thaliana.TAIR10.36.gene2go"
    # a.go_filt(go_db, outdir)
    # cog_db = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/COG/Arabidopsis_thaliana.gene2cog.xls"
    # a.cog_filt(cog_db, outdir)
