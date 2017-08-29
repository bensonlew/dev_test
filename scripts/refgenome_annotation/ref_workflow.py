# -*- coding: utf-8 -*-
# _author_ = "zengjing"
import collections
import re
import regex
import os
import json
import gridfs
import subprocess
from biocluster.config import Config


class RefAnnotation(object):
    """
    通过gtf对文件注释进行筛选,提取transcrpt_id对应的基因ID，转录长度，基因名称信息
    """
    def __init__(self):
        self.tran_genes = {}
        self.gene_names = {}
        self.tran_lengths = {}
        self.tran_ids = []
        self.gene_ids = []
        self.mongodb = Config().biodb_mongo_client.sanger_biodb
        # self.mongodb = Config().mongo_client.sanger_biodb
        self.cog_string = self.mongodb.COG_String
        self.cog = self.mongodb.COG
        self.gene_coll = self.mongodb.kegg_gene_v1
        self.ko_coll = self.mongodb.kegg_ko_v1
        self.png_coll = self.mongodb.kegg_pathway_png_v1
        self.go = self.mongodb.GO
        self.go_script = "/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/scripts/goAnnot.py"
        self.map_path = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/script/map4.r"
        self.r_path = "/mnt/ilustre/users/sanger-dev/app/program/R-3.3.3/bin/Rscript"
        self.gloabl = ["map01100", "map01110", "map01120", "map01130", "map01200", "map01210", "map01212", "map01230", "map01220"]

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
        for line in open(biomart_path):
            item = line.strip().split("\t")
            try:
                tran_id = item[1]
            except:
                pass
            try:
                length = item[-3]
            except:
                pass
            self.tran_lengths[tran_id] = length

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

    def cog_annotation(self, org_cog, cog_summary, cog_table):
        """
        参考基因组cog注释
        org_cog：物种id对应的cog_id、nog_id; cog_summary：cog注释结果文件
        """
        self.func_type = {
            'INFORMATION STORAGE AND PROCESSING': sorted(['J', 'A', 'K', 'L', 'B']),
            'CELLULAR PROCESSES AND SIGNALING': sorted(['D', 'Y', 'V', 'T', 'M', 'N', 'Z', 'W', 'U', 'O']),
            'METABOLISM': sorted(['C', 'G', 'E', 'F', 'H', 'I', 'P', 'Q']),
            'POORLY CHARACTERIZED': sorted(['R', 'S']),
        }
        self.func_decs = {
            'J': 'Translation, ribosomal structure and biogenesis',
            'A': 'RNA processing and modification', 'K': 'Transcription',
            'L': 'Replication, recombination and repair',
            'B': 'Chromatin structure and dynamics',
            'D': 'Cell cycle control, cell division, chromosome partitioning',
            'Y': 'Nuclear structure', 'V': 'Defense mechanisms', 'T': 'Signal transduction mechanisms',
            'M': 'Cell wall/membrane/envelope biogenesis',
            'N': 'Cell motility', 'Z': 'Cytoskeleton', 'W': 'Extracellular structures',
            'U': 'Intracellular trafficking, secretion, and vesicular transport',
            'O': 'Posttranslational modification, protein turnover, chaperones',
            'C': 'Energy production and conversion', 'G': 'Carbohydrate transport and metabolism',
            'E': 'Amino acid transport and metabolism', 'F': 'Nucleotide transport and metabolism',
            'H': 'Coenzyme transport and metabolism', 'I': 'Lipid transport and metabolism',
            'P': 'Inorganic ion transport and metabolism',
            'Q': 'Secondary metabolites biosynthesis, transport and catabolism',
            'R': 'General function prediction only', 'S': 'Function unknown'
        }
        no_ids, total_ids = [], []
        with open(org_cog, "r") as f, open(cog_summary, "w") as w, open(cog_table, "w") as w1:
            w1.write('#Query_name\tQuery_length\tHsp_start_of_query\tHsp_end_of_query\tHsp_strand_of_query\tHit_name\tHit_description\tHit_length\tHsp_start_of_hit\tHsp_end_of_hit\tCOG/NOG_group\tCOG/NOG_group_description\tCOG/NOG_group_categoriesr\tCOG/NOG_region_start\tCOG/NOG_region_end\tCoverage_of_COG/NOG_region\tIdentities_of_COG/NOG_region\tPositives_Identities_of_COG/NOG_region\n')
            lines = f.readlines()
            fun_seqs = {"COG": {}, "NOG": {}}
            for line in lines[1:]:
                item = line.strip().split("\t")
                query_id = item[0]
                try:
                    cog_ids = item[1].split(";")
                    for cog_id in cog_ids:
                        results = self.cog.find({"cog_id": cog_id})
                        if results.count() > 0:
                            for result in results:
                                group = result["cog_categories"]
                                cog_des = result["cog_description"]
                                w1.write(query_id + "\tNone\tNone\tNone\tNone\tNone\tNone\tNone\tNone\tNone\t" + cog_id + "\t" + cog_des + "\t" + group + "\tNone\tNone\tNone\tNone\tNone" + "\n")
                                if group not in fun_seqs['COG']:
                                    fun_seqs['COG'][group] = []
                                fun_seqs['COG'][group].append(query_id)
                                total_ids.append(query_id)
                        else:
                            no_ids.append(cog_id)
                except:
                    pass
                try:
                    nog_ids = item[2].split(";")
                    for nog_id in nog_ids:
                        results = self.cog.find({"cog_id": nog_id})
                        if results.count() > 0:
                            for result in results:
                                group = result["cog_categories"]
                                cog_des = result["cog_description"]
                                w1.write(query_id + "\tNone\tNone\tNone\tNone\tNone\tNone\tNone\tNone\tNone\t" + nog_id + "\t" + cog_des + "\t" + group + "\tNone\tNone\tNone\tNone\tNone" + "\n")
                                if group not in fun_seqs['NOG']:
                                    fun_seqs['NOG'][group] = []
                                fun_seqs['NOG'][group].append(query_id)
                                total_ids.append(query_id)
                        else:
                            no_ids.append(nog_id)
                except:
                    pass
            no_ids = list(set(no_ids))
            print len(no_ids)
            total_ids = list(set(total_ids))
            w.write('#Total seqs with COG/KOG/NOGs:{}'.format(str(len(total_ids))))
            w.write('#Type\tfunctional_categories\tCOG\tNOG\tCOG list\tNOG list\n')
            for first in self.func_type:
                for g in self.func_type[first]:
                    second = '[' + g + ']' + self.func_decs[g]
                    try:
                        cog_seqs = list(set(fun_seqs["COG"][g]))
                    except:
                        cog_seqs = []
                    try:
                        nog_seqs = list(set(fun_seqs["NOG"][g]))
                    except:
                        nog_seqs = []
                    w.write(first + "\t" + second + "\t" + str(len(cog_seqs)) + "\t" + str(len(nog_seqs)) + "\t" + ';'.join(cog_seqs) + "\t" + ';'.join(nog_seqs) + "\n")

    def go_annotation(self, go_list, out_dir):
        """
        参考基因组go注释
        go_list: 物种对应的go_id,同一id对应的多个go_id间用分号隔开；out_dir:输出文件路径
        需投递到指定队列：BLAST2GO
        """
        cmd = 'python %s %s %s %s %s' % (self.go_script, go_list, 'localhost', 'biocluster102', 'sanger-dev-123')  # 10.100.203.193
        print cmd
        try:
            subprocess.check_output(cmd, shell=True)
            print "运行goAnnot.py完成"
        except subprocess.CalledProcessError:
            print "运行goAnnot.py出错"

    def get_kegg_table(self, org_kegg, table_path):
        """
        org_kegg：物种kegg注释文件，ensembl_id\tKO\tpathway\n
        table_path: kegg_table.xls
        """
        kegg_table = open(table_path, "w")
        kegg_table.write("#Query\tKO_ID (Gene id)\tKO_name (Gene name)\tHyperlink\tPaths\n")
        for line in open(org_kegg, "rb"):
            item = line.strip().split("\t")
            query_id = item[0]
            koids = item[1]
            try:
                org_paths = item[2].split(";")
                map_paths = []
                for map_id in org_paths:
                    if map_id not in self.gloabl:
                        map_paths.append(map_id)
            except:
                map_paths = []
            map_paths = list(set(map_paths))
            link = 'http://www.genome.jp/dbget-bin/www_bget?ko:' + koids
            ko_names = []
            try:
                ko_ids = koids.split(";")
                for ko_id in ko_ids:
                    result = self.ko_coll.find_one({"ko_id": ko_id})
                    if result:
                        ko_names.append(result['ko_name'])
            except:
                koid = koids
                result = self.ko_coll.find_one({"ko_id": koid})
                if result:
                    ko_names.append(result['ko_name'])
            kegg_table.write(query_id + "\t" + koids + "\t" + ';'.join(ko_names) + "\t" + link + "\t" + ';'.join(map_paths) + "\n")

    def get_payhway_table(self, org_kegg, pathway_path, link_bgcolor, png_bgcolor, pathwaydir, layerfile, image_magick):
        """
        org_kegg：物种kegg注释文件，ensembl_id\tKO\tpathway\n
        pathway_path: pahtway_table.xls
        link_bgcolor: 链接背景颜色(ref:yellow), png_bgcolor：静态图背景颜色
        pathwaydir: 通路图文件夹
        """
        path_table = open(pathway_path, "w")
        path_table.write("Pathway\tFirst Category\tSecond Category\tPathway_definition\tnum_of_seqs\tgene_list\tpathway_imagename\tHyperlink\n")
        if not os.path.exists(pathwaydir):
            os.makedirs(pathwaydir)
        paths = []
        path_koids, path_seqs = {}, {}
        d, ko = {}, {}
        for line in open(org_kegg, "rb"):
            item = line.strip().split("\t")
            query_id = item[0]
            ko_ids = item[1].split(";")
            try:
                org_paths = item[2].split(";")
                for map_id in org_paths:
                    if map_id not in self.gloabl:
                        if map_id not in paths:
                            path_koids[map_id] = []
                            path_seqs[map_id] = []
                            paths.append(map_id)
                        for ko_id in ko_ids:
                            path_koids[map_id].append(ko_id)
                        path_seqs[map_id].append(query_id)
            except:
                pass
        for map_id in paths:
            ko_ids = list(set(path_koids[map_id]))
            seqs = list(set(path_seqs[map_id]))
            pid = re.sub("map", "ko", map_id)
            result = self.ko_coll.find_one({"pathway_id": {"$in": [pid]}})
            if result:
                pids = result["pathway_id"]
                layer = False
                for index, i in enumerate(pids):
                    if i == pid:
                        category = result["pathway_category"][index]
                        layer_1st = category[0]
                        layer_2nd = category[1]
                        definition = category[2]
                        layer = True
                if layer:
                    if d.has_key(layer_1st):
                        if d[layer_1st].has_key(layer_2nd):
                            for s in seqs:
                                d[layer_1st][layer_2nd].append(s)
                        else:
                            d[layer_1st][layer_2nd] = seqs
                    else:
                        d[layer_1st] = {}
                        d[layer_1st][layer_2nd] = seqs
            else:
                print "mongo里没有找到pathway_id {}".format(pid)
            path_image = map_id + ".png"
            ko_color = []
            for ko_id in ko_ids:
                ko_color.append(ko_id + "%09" + link_bgcolor)
            link = 'http://www.genome.jp/dbget-bin/show_pathway?' + map_id + '/' + '/'.join(ko_color)
            path_table.write(map_id+ "\t" + layer_1st + "\t" + layer_2nd + "\t" + definition + "\t"\
                             + str(len(seqs)) + "\t" + ';'.join(seqs) + "\t" + path_image + "\t" + link + "\n")
            fgcolor = "NA"
            kos_path = os.path.join(os.getcwd(), "KOs.txt")
            with open(kos_path, "w") as w:
                w.write("#KO\tbg\tfg\n")
                for k in ko_ids:
                    w.write(k + "\t" + png_bgcolor + "\t" + fgcolor + "\n")
            png_path = pathwaydir + '/' + map_id + ".png"
            pdf_path = pathwaydir + '/' + map_id + ".pdf"
            self.get_pic(map_id, kos_path, png_path)
            if image_magick:
                cmd = image_magick + ' -flatten -quality 100 -density 130 -background white ' + png_path + ' ' + pdf_path
                try:
                    subprocess.check_output(cmd, shell=True)
                except subprocess.CalledProcessError:
                    print '图片格式pdf转png出错'
        with open(layerfile, "w+") as k:
            for i in d:
                for j in d[i]:
                    seqs = list(set(d[i][j]))
                    line = i + "\t" + j + "\t" + str(len(seqs)) + "\t" + ';'.join(seqs) + "\n"
                    k.write(line)

    def get_pic(self, path, kos_path, png_path):
        """
        画通路图
        """
        fs = gridfs.GridFS(self.mongodb)
        pid = re.sub("map", "ko", path)
        with open("pathway.kgml", "w+") as k, open("pathway.png", "w+") as p:
            result = self.png_coll.find_one({"pathway_id": pid})
            if result:
                kgml_id = result['pathway_ko_kgml']
                png_id = result['pathway_map_png']
                k.write(fs.get(kgml_id).read())
                p.write(fs.get(png_id).read())
        cmd = "{} {} {} {} {} {} {}".format(self.r_path, self.map_path, path, kos_path, png_path, "pathway.kgml", "pathway.png")
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            print "{}画图出错".format(path)
            os.system("cp {} {}".format("pathway.png", png_path))

    def kegg_annotation(self, org_kegg, table_path, link_bgcolor, png_bgcolor, pathway_path, pathwaydir, layerfile, image_magick):
        """
        参考基因组kegg注释

        """
        self.get_kegg_table(org_kegg, table_path)
        self.get_payhway_table(org_kegg, pathway_path, link_bgcolor, png_bgcolor, pathwaydir, layerfile, image_magick)

    def cog_anno_stat(self, cog_summary, cog_venn):
        query_ids = []
        with open(cog_summary, "r") as f, open(cog_venn, "w") as w:
            lines = f.readlines()
            for line in lines[1:]:
                item = line.strip().split("\t")
                try:
                    query_cog = item[4].split(";")
                    for q in query_cog:
                        query_ids.append(q)
                except:
                    pass
                try:
                    query_nog = item[5].split(";")
                    for q in query_nog:
                        query_ids.append(q)
                except:
                    pass
            query_ids = list(set(query_ids))
            for q in query_ids:
                w.write(q + "\n")
        return query_ids

    def go_anno_stat(self, gos_list, go_venn):
        query_ids = []
        with open(gos_list, "r") as f, open(go_venn, "w") as w:
            lines = f.readlines()
            for line in lines:
                item = line.strip().split("\t")
                query_ids.append(item[0])
            query_ids = list(set(query_ids))
            for q in query_ids:
                w.write(q + "\n")
        return query_ids

    def kegg_anno_stat(self, kegg_table, kegg_venn):
        query_ids = []
        with open(kegg_table, "r") as f, open(kegg_venn, "w") as w:
            lines = f.readlines()
            for line in lines[1:]:
                item = line.strip().split("\t")
                query_ids.append(item[0])
            query_ids = list(set(query_ids))
            for q in query_ids:
                w.write(q + "\n")
        return query_ids

    def ref_anno_stat(self, cog_summary, gene_cog_summary, gos_list, gene_gos_list, kegg_table, gene_kegg_table, venn_dir, all_stat):
        cog_venn = venn_dir + "/cog_venn.txt"
        gene_cog_venn = venn_dir + "/gene_cog_venn.txt"
        kegg_venn = venn_dir + "/kegg_venn.txt"
        gene_kegg_venn = venn_dir + "/gene_kegg_venn.txt"
        go_venn = venn_dir + "/go_venn.txt"
        gene_go_venn = venn_dir + "/gene_go_venn.txt"
        cog_ids = self.cog_anno_stat(cog_summary, cog_venn)
        go_ids = self.go_anno_stat(gos_list, go_venn)
        kegg_ids = self.kegg_anno_stat(kegg_table, kegg_venn)
        gene_cog_ids = self.cog_anno_stat(gene_cog_summary, gene_cog_venn)
        gene_go_ids = self.go_anno_stat(gene_gos_list, gene_go_venn)
        gene_kegg_ids = self.kegg_anno_stat(gene_kegg_table, gene_kegg_venn)
        anno_ids, gene_anno_ids = [], []
        for i in cog_ids:
            anno_ids.append(i)
        for i in go_ids:
            anno_ids.append(i)
        for i in kegg_ids:
            anno_ids.append(i)
        for i in gene_cog_ids:
            gene_anno_ids.append(i)
        for i in gene_go_ids:
            gene_anno_ids.append(i)
        for i in gene_kegg_ids:
            gene_anno_ids.append(i)
        anno_ids = list(set(anno_ids))
        gene_anno_ids = list(set(gene_anno_ids))
        total_count = 54013
        gene_total_count = 32833
        with open(all_stat, "w") as w:
            w.write("type\ttranscripts\tgenes\ttranscripts_percent\tgenes_percent\n")
            w.write("cog\t" + str(len(cog_ids)) + "\t" + str(len(gene_cog_ids)) + "\t" + str(round(float(len(cog_ids))/total_count, 4)) + "\t" + str(round(float(len(gene_cog_ids))/gene_total_count, 4)) + "\n")
            w.write("go\t" + str(len(go_ids)) + "\t" + str(len(gene_go_ids)) + "\t" + str(round(float(len(go_ids))/total_count, 4)) + "\t" + str(round(float(len(gene_go_ids))/total_count, 4)) + "\n")
            w.write("kegg\t" + str(len(kegg_ids)) + "\t" + str(len(gene_kegg_ids)) + "\t" + str(round(float(len(kegg_ids))/total_count, 4)) + "\t" + str(round(float(len(gene_kegg_ids))/total_count, 4)) + "\n")
            w.write("total_anno\t" + str(len(anno_ids)) + "\t" + str(len(gene_anno_ids)) + "\t" + str(round(float(len(anno_ids))/total_count, 4)) + "\t" + str(round(float(len(gene_anno_ids))/total_count, 4)) + "\n")
            w.write("total\t" + str(total_count) + "\t" + str(gene_total_count) + "\t1\t1" + "\n")

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

    def get_anno_stat(self, outpath, gtf_path, biomart_path, cog_list=None, gos_list=None, org_kegg=None, anno_type="transcript"):
        self.get_gtf_information(gtf_path=gtf_path, biomart_path=biomart_path, anno_type=anno_type)
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

    def get_gtf_information(self, gtf_path, biomart_path, anno_type):
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
                if anno_type == "transcript":
                    if tran_id not in self.stat_info:
                        query = Transcript()
                        query.name = tran_id
                        query.gene_id = gene_id
                        self.stat_info[tran_id] = query
                else:
                    if gene_id not in self.stat_info:
                        query = Transcript()
                        query.name = gene_id
                        query.gene_id = gene_id
                        self.stat_info[gene_id] = query
            if anno_type == "transcript":
                m = re.match(r".+transcript_id \"(.+?)\"; .*gene_name \"(.+?)\";.*$", line)
                if m:
                    tran_id = m.group(1)
                    name = m.group(2)
                    self.stat_info[tran_id].gene_name = name
            else:
                m = re.match(r".+gene_id \"(.+?)\"; .*gene_name \"(.+?)\";.*$", line)
                if m:
                    gene_id = m.group(1)
                    name = m.group(2)
                    if gene_id in self.stat_info:
                        self.stat_info[gene_id].gene_name = name
                    else:
                        query = Transcript()
                        query.name = gene_id
                        query.gene_id = gene_id
                        self.stat_info[gene_id] = query
        for line in open(biomart_path):
            item = line.strip().split("\t")
            try:
                tran_id = item[1]
                length = item[-3]
                if anno_type == "transcript":
                    if tran_id in self.stat_info:
                        self.stat_info[tran_id].length = length
            except:
                pass

    def get_cog(self, cog_list):
        """找到转录本ID对应的cogID、nogID、kogID及功能分类和描述"""
        with open(cog_list, 'rb') as f:
            lines = f.readlines()
            for line in lines:
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

    def get_kegg(self, org_kegg):
        "找到转录本ID对应的KO、KO_name、Pathway、Pathway_definition"
        with open(org_kegg, 'rb') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip('\n').split('\t')
                query_name = line[0]
                koids = line[1]
                ko_names = []
                try:
                    ko_ids = koids.split(";")
                    for ko_id in ko_ids:
                        result = self.kegg_ko.find_one({"ko_id": ko_id})
                        if result:
                            ko_names.append(result['ko_name'])
                except:
                    koid = koids
                    result = self.kegg_ko.find_one({"ko_id": koid})
                    if result:
                        ko_names.append(result['ko_name'])
                try:
                    org_paths = item[2].split(";")
                    map_paths = []
                    for map_id in org_paths:
                        if map_id not in self.gloabl:
                            map_paths.append(map_id)
                except:
                    map_paths = []
                map_paths = list(set(map_paths))
                pathway = []
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
                if query_name in self.stat_info:
                    self.stat_info[query_name].ko_id = ko_id
                    self.stat_info[query_name].ko_name = ';'.join(ko_names)
                    self.stat_info[query_name].pathway = "; ".join(pathway)
if __name__ == "__main__":
    # test = RefAnnotation()
    image_magick = "/mnt/ilustre/users/sanger-dev/app/program/ImageMagick/bin/convert"
    png_bgcolor = "#FFFF00"  # 黄色
    link_bgcolor = "yellow"
    db_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/"
    ref_json = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/annot_species.complete.json"
    # ref_json = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/ath.json"
    # ref_json = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/script/ath.json"
    f = open(ref_json, "r")
    json_dict = json.loads(f.read())
    # out = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/taxonomy"
    for taxon in json_dict:
        test = RefAnnotation()
        gtf_path = db_path + json_dict[taxon]["gtf"]
        biomart_path = db_path + json_dict[taxon]["bio_mart_annot"]
        kegg_db = db_path + json_dict[taxon]["kegg"]
        go_db = db_path + json_dict[taxon]["go"]
        cog_db = db_path + json_dict[taxon]["cog"]
        # outdir = out + '/' + taxon + '/Annotation'
        test.get_gtf_information(gtf_path, biomart_path)
        outdir = db_path + json_dict[taxon]["gtf"].split("gtf")[0] + 'Annotation'
        try:
            json_dict[taxon]["anno_path"] = outdir
        except:
            print "设置注释路径出错"
        json_dict[taxon] = json.dumps(json_dict[taxon], sort_keys=True, separators=(',', ':'))
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        print outdir
        test.kegg_filt(kegg_db, outdir)
        test.go_filt(go_db, outdir)
        test.cog_filt(cog_db, outdir)
        org_kegg = outdir + "/kegg_trans.list"
        kegg_path = outdir + "/kegg"
        if not os.path.exists(kegg_path):
            os.makedirs(kegg_path)
        kegg_table = kegg_path + "/kegg_table.xls"
        pathway_path = kegg_path + "/pathway_table.xls"
        pathwaydir = kegg_path + "/pathways"
        layerfile = kegg_path + "/kegg_layer.xls"
        test.kegg_annotation(org_kegg, kegg_table, link_bgcolor, png_bgcolor, pathway_path, pathwaydir, layerfile, image_magick)
        gene_org_kegg = outdir + "/kegg_genes.list"
        gene_kegg_path = outdir + "/anno_stat/kegg_stat"
        if not os.path.exists(gene_kegg_path):
            os.makedirs(gene_kegg_path)
        gene_kegg_table = gene_kegg_path + "/gene_kegg_table.xls"
        pathway_path = gene_kegg_path + "/gene_pathway_table.xls"
        pathwaydir = gene_kegg_path + "/gene_pathway"
        layerfile = gene_kegg_path + "/gene_kegg_layer.xls"
        test.kegg_annotation(gene_org_kegg, gene_kegg_table, link_bgcolor, png_bgcolor, pathway_path, pathwaydir, layerfile, image_magick)

        org_cog = outdir + "/cog_trans.list"
        cog_path = outdir + "/cog"
        if not os.path.exists(cog_path):
            os.makedirs(cog_path)
        gene_cog_path = outdir + "/anno_stat/cog_stat"
        if not os.path.exists(gene_cog_path):
            os.makedirs(gene_cog_path)
        cog_summary =cog_path + "/cog_summary.xls"
        cog_table =
        test.cog_annotation(org_cog, cog_summary, cog_table)
        gene_org_cog = outdir + "/cog_genes.list"
        gene_cog_summary = gene_cog_path + "/gene_cog_summary.xls"
        test.cog_annotation(gene_org_cog, gene_cog_summary, gene_cog_table)

        go_path = outdir + "/go"
        if not os.path.exists(go_path):
            os.makedirs(go_path)
        gene_go_path = outdir + "/anno_stat/go_stat"
        if not os.path.exists(gene_go_path):
            os.makedirs(gene_go_path)
        go_list = outdir + "/go_trans.list"
        test.go_annotation(go_list, go_path)
        outfiles = ['go1234level_statistics.xls', 'go123level_statistics.xls', 'go12level_statistics.xls']
        for item in outfiles:
            linkfile = go_path + '/' + item
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.system("cp " + os.getcwd() + '/' + item + " " + linkfile)
        os.system("cp " + go_list + " " + go_path + "/query_gos.list")
        go_list = outdir + "/go_genes.list"
        test.go_annotation(go_list, gene_go_path)
        for item in outfiles:
            linkfile = gene_go_path + '/' + item
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.system("cp " + os.getcwd() + '/' + item + " " + linkfile)
        os.system("cp " + go_list + " " + gene_go_path + "/gene_gos.list")
        gos_list = go_path + "/query_gos.list"
        gene_gos_list = gene_go_path + "/gene_gos.list"

        venn_dir = outdir + "/anno_stat/venn"
        if not os.path.exists(venn_dir):
            os.makedirs(venn_dir)
        all_stat = outdir + "/anno_stat/all_annotation_statistics.xls"
        test.ref_anno_stat(cog_summary, gene_cog_summary, gos_list, gene_gos_list, kegg_table, gene_kegg_table, venn_dir, all_stat)

        Transcript()
        query = RefAnnoQuery()
        outpath = outdir + "/anno_stat/trans_anno_detail.xls"
        query.get_anno_stat(outpath=outpath, gtf_path=gtf_path, biomart_path=biomart_path, cog_list=org_cog, gos_list=gos_list, org_kegg=org_kegg, anno_type="transcript")
        Transcript()
        query = RefAnnoQuery()
        outpath = outdir + "/anno_stat/genes_anno_detail.xls"
        query.get_anno_stat(outpath=outpath, gtf_path=gtf_path, biomart_path=biomart_path, cog_list=gene_org_cog, gos_list=gene_gos_list, org_kegg=gene_org_kegg, anno_type="gene")

    with open("test_zj.json", "w") as f:
        json.dump(json_dict, f)
