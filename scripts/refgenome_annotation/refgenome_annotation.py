# -*- coding: utf-8 -*-
# _author_ = "zengjing"
import collections
import re
import regex
import os
import gridfs
import subprocess
import json
from biocluster.config import Config
from itertools import islice


class GtfFilt(object):
    """
    通过gtf对文件注释进行筛选,提取transcrpt_id对应的基因ID，转录长度，基因名称信息
    """
    def get_gtf_information(self, gtf_path, biomart_path):
        tran_gene, gene_name, tran_length = {}, {}, {}
        tran_ids, gene_ids = [], []
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
                if tran_id not in tran_gene:
                    tran_gene[tran_id] = gene_id
                tran_ids.append(tran_id)
                gene_ids.append(gene_id)
            m = re.match(r".+gene_id \"(.+?)\"; .*gene_name \"(.+?)\";.*$", line)
            if m:
                gene_id = m.group(1)
                name = m.group(2)
                gene_name[gene_id] = name
        tran_ids = list(set(tran_ids))
        gene_ids = list(set(gene_ids))
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
            tran_length[tran_id] = length
        test = open("tran_information.list", "w")
        for tran_id in tran_gene:
            gene_id = tran_gene[tran_id]
            length = tran_length[tran_id]
            try:
                name = gene_name[gene_id]
            except:
                name = "\t"
            test.write(tran_id + "\t" + length + "\t" + gene_id + "\t" + name + "\n")


class RefgenomeAnnotation(object):
    """
    参考基因组kegg、cog、go注释
    """
    def __init__(self):
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

    def cog_annotation(self, org_cog, cog_summary):
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
        no_ids = []
        with open(org_cog, "r") as f, open(cog_summary, "w") as w:
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
                                if group not in fun_seqs['COG']:
                                    fun_seqs['COG'][group] = []
                                fun_seqs['COG'][group].append(query_id)
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
                                if group not in fun_seqs['NOG']:
                                    fun_seqs['NOG'][group] = []
                                fun_seqs['NOG'][group].append(query_id)
                        else:
                            no_ids.append(nog_id)
                except:
                    pass
            no_ids = list(set(no_ids))
            print no_ids
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
                for org_path in org_paths:
                    m = re.match(r".+(\d{5})", org_path)
                    if m:
                        map_id = "map" + m.group(1)
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
                    result = self.ko_coll.find_one({"ko_id": koid})
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
                for org_path in org_paths:
                    m = re.match(r".+(\d{5})", org_path)
                    if m:
                        map_id = "map" + m.group(1)
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


class AnnotationStat(object):
    """
    注释统计：对cog_summary.xls/gos.list/kegg_table.xls进行统计
    """
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


if __name__ == "__main__":
    test = RefgenomeAnnotation()
    image_magick = "/mnt/ilustre/users/sanger-dev/app/program/ImageMagick/bin/convert"

    # org_cog = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/cog_genes.list"
    # cog_summary = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/GeneAnnotation/COG/refgene_cog_statistics.xls"
    # test.cog_annotation(org_cog, cog_summary)
    # org_cog = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/cog_trans.list"
    # cog_summary = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/TransAnnotion/COG/reftrans_cog_statistics.xls"
    # test.cog_annotation(org_cog, cog_summary)

    # go_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/go_trans.list"
    # out_dir = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/TransAnnotion/GO/"
    # test.go_annotation(go_list, out_dir)
    # outfiles = ['go1234level_statistics.xls', 'go123level_statistics.xls', 'go12level_statistics.xls']
    # for item in outfiles:
    #     linkfile = out_dir + '/reftrans' + item
    #     if os.path.exists(linkfile):
    #         os.remove(linkfile)
    #     os.system("cp " + os.getcwd() + '/' + item + " " + linkfile)
    # os.system("cp " + go_list + " " + out_dir + "/reftrans_gos.list")
    # go_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/go_genes.list"
    # out_dir = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/GeneAnnotation/GO/"
    # test.go_annotation(go_list, out_dir)
    # for item in outfiles:
    #     linkfile = out_dir + '/refgene' + item
    #     if os.path.exists(linkfile):
    #         os.remove(linkfile)
    #     os.system("cp " + os.getcwd() + '/' + item + " " + linkfile)
    # os.system("cp " + go_list + " " + out_dir + "/refgene_gos.list")

    # png_bgcolor = "#FFFF00" # 黄色
    # link_bgcolor = "yellow"
    # org_kegg = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/kegg_trans.list"
    # table_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/kegg/kegg_table.xls"
    # pathway_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/kegg/pathway_table.xls"
    # pathwaydir = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/kegg/pathways"
    # layerfile = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/kegg/kegg_layer.xls"
    # test.kegg_annotation(org_kegg, table_path, link_bgcolor, png_bgcolor, pathway_path, pathwaydir, layerfile, image_magick)
    # org_kegg = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/kegg_genes.list"
    # table_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/anno_stat/kegg_stat/gene_kegg_table.xls"
    # pathway_path = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/anno_stat/kegg_stat/gene_pathway_table.xls"
    # pathwaydir = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/anno_stat/kegg_stat/gene_pathway"
    # layerfile = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/plants/Arabidopsis_thaliana/Ensemble_release_36/Annotation/anno_stat/kegg_stat/gene_kegg_layer.xls"
    # test.kegg_annotation(org_kegg, table_path, link_bgcolor, png_bgcolor, pathway_path, pathwaydir, layerfile, image_magick)

    # stat = AnnotationStat()
    # cog_summary = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/TransAnnotion/COG/reftrans_cog_statistics.xls"
    # gene_cog_summary = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/GeneAnnotation/COG/refgene_cog_statistics.xls"
    # gos_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/TransAnnotion/GO/reftrans_gos.list"
    # gene_gos_list = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/GeneAnnotation/GO/refgene_gos.list"
    # kegg_table = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/TransAnnotion/KEGG/reftrans_kegg_table.xls"
    # gene_kegg_table = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/GeneAnnotation/KEGG/refgene_kegg_table.xls"
    # venn_dir = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/venn"
    # all_stat = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna1/ref_genome/Annotation/ref_all_annot_statistics.xls"
    # stat.ref_anno_stat(cog_summary, gene_cog_summary, gos_list, gene_gos_list, kegg_table, gene_kegg_table, venn_dir, all_stat)


    ref_json = "/mnt/ilustre/users/sanger-dev/app/database/Genome_DB_finish/annot_species.json"
    f = open(ref_json, "r")
    json_dict = json.loads(f.read())
    for taxon in json_dict:
        print taxon
