# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.workflow import Workflow
from collections import defaultdict
import os
import glob
from itertools import chain
import subprocess
from mbio.packages.denovo_rna.express.kegg_regulate import KeggRegulate


class GenesetKeggWorkflow(Workflow):
    """
    基因集功能分类分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(GenesetKeggWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "geneset_kegg", "type": "string"},
            {"name": "kegg_table", "type": "infile", "format": "annotation.kegg.kegg_table"},
            {"name": "geneset_type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_table_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "geneset_id", "type": "string"},
            {"name": "kegg_pics", "type": "string"}

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.r_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin/Rscript"
        self.map_path = self.config.SOFTWARE_DIR + "/bioinfo/annotation/scripts/map3.r"
        self.db_path = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/ref_rna/ref_anno/script/database/"
        self.image_magick = self.config.SOFTWARE_DIR + "/program/ImageMagick/bin/convert"
        # self.group_spname = dict()

    def run(self):
        self.start_listener()
        self.fire("start")
        self.get_kegg_table()
        self.set_db()
        # super(GenesetClassWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_geneset = self.api.ref_rna_geneset
        self.logger.info("wooooooooorkflowinfoooooooo")
        output_file = self.output_dir + '/kegg_stat.xls'
        pathway_file = self.output_dir + '/pathways'
        api_geneset.add_kegg_regulate_detail(self.option("main_table_id"), output_file)
        api_geneset.add_kegg_regulate_pathway(pathway_file, self.option("main_table_id"))
        # os.link(output_file, self.output_dir + "/" + os.path.basename(output_file))
        print(output_file)
        self.end()

    def get_kegg_table(self):
        kegg = KeggRegulate()
        ko_genes, path_ko = self.option('kegg_table').get_pathway_koid()
        geneset_ko = defaultdict(set)
        regulate_gene = {}
        with open(self.option("geneset_kegg"), "r") as f:
            for line in f:
                line = line.strip().split("\t")
                regulate_gene[line[0]] = line[1].split(",")
        # path_kos = set(chain(*path_ko.values()))
        # for ko in path_kos:
        #     genes = ko_genes[ko]
        #     for gene in genes:
        #         for gn in regulate_gene:
        #             if gene in regulate_gene[gn]:
        #                 geneset_ko[gn].add(ko)
        pathways = self.output_dir + '/pathways'
        if not os.path.exists(pathways):
            os.mkdir(pathways)
        # kos_path = self.get_ko(ko_genes=ko_genes,catgory=regulate_gene,out_dir=self.output_dir)
        # self.logger.info(ko_genes)
        kegg.get_regulate_table(ko_gene=ko_genes, path_ko=path_ko, regulate_gene=regulate_gene, output= self.output_dir + '/kegg_stat.xls')
        self.get_ko(ko_genes=ko_genes, catgory=regulate_gene, kegg_regulate=self.output_dir + '/kegg_stat.xls', out_dir=self.output_dir)
        self.get_pics(ko_genes=ko_genes, path_ko=path_ko, kos_path=self.output_dir + "/ko", out_dir=self.output_dir)
        # pathways = self.output_dir + '/pathways'
        # if not os.path.exists(pathways):
        #     os.mkdir(pathways)
        # # self.logger.info(ko_genes)
        # new_path_ko = kegg.get_regulate_table(ko_gene=ko_genes, path_ko=path_ko, regulate_gene=regulate_gene, output= self.output_dir + '/kegg_stat.xls')
        # kegg.get_pictrue(path_ko=new_path_ko, out_dir=pathways, regulate_dict=geneset_ko,
        #                  image_magick=self.config.SOFTWARE_DIR + "/bioinfo/plot/imageMagick/bin/convert")  # 颜色
        # kegg.get_pictrue(path_ko=path_ko, out_dir=pathways)

    def get_ko(self, ko_genes, catgory, kegg_regulate, out_dir):
        # kos = ko_genes.keys()
        # genes = set(chain(*ko_genes.values()))
        # gene_list = list(genes)
        # color_dict = {}
        # for gene in gene_list:
        #     color_dict[gene] = []
        if not os.path.exists(out_dir + "/ko"):
            os.mkdir(out_dir + "/ko")
        f = open(kegg_regulate, "r")
        f.readline()
        for line in f:
            tmp = line.strip().split("\t")
            path = tmp[0]
            gene_num1 = tmp[2]
            if len(catgory) == 1:
                if gene_num1 and not gene_num1 == "0" and not gene_num1.startswith("http"):
                    gene1_list_tmp = [x.split("(")[1] if not ")" in x else x.strip(")").split("(")[1] for x in tmp[3].split(");")]
                    for gene in gene1_list_tmp:
                        if gene.find(";") != 1:
                            gene1_list.append(gene)
                        else:
                            gene1_list.extend(gene.split(";"))
                else:
                    gene1_list = []
                gene2_list = []
            else:
                if gene_num1 and not gene_num1 == "0" and not gene_num1.startswith("http"):
                    gene1_list_tmp = [x.split("(")[1] if not ")" in x else x.strip(")").split("(")[1] for x in tmp[3].split(");")]
                    for gene in gene1_list_tmp:
                        if gene.find(";") != 1:
                            gene1_list.append(gene)
                        else:
                            gene1_list.extend(gene.split(";"))
                else:
                    gene1_list = []
                gene_num2 = tmp[4]
                gene2_list = []
                if gene_num2 and not gene_num2.startswith("http") and not gene_num2 == "0":
                    # gene2_list = [x.split("(")[1][:-1] for x in tmp[5].split(");")]
                    # self.logger.info(gene2_list)
                    gene2_list_tmp = [x.split("(")[1] if not ")" in x else x.strip(")").split("(")[1] for x in tmp[5].split(");")]
                    for gene in gene2_list_tmp:
                        if gene.find(";") != 1:
                            gene2_list.append(gene)
                        else:
                            gene2_list.extend(gene.split(";"))
            gene_list = []
            gene_list.extend(gene1_list)
            # if gene2_list:
            gene_list.extend(gene2_list)
            gene_list_unrepeat = list(set(gene_list))
            color_dict = {}
            for gene in gene_list_unrepeat:
                color_dict[gene] = []
                if len(catgory) == 1:
                    if gene in gene1_list:
                        color_dict[gene].append("#00ffff")  # 兰色
                elif len(catgory) ==2:
                    if gene in gene1_list:
                        color_dict[gene].append("#00ffff")  # 兰色
                    if gene in gene1_list:
                        color_dict[gene].append("#a5682a")  # 棕色
                else:
                    pass
            with open(out_dir + "/ko/" + path, "w") as fw:
                fw.write("#KO\tbg\tfg\n")
                for key in color_dict.keys():
                    if len(color_dict[key]) != 0:
                        str_ = key + "\tNA\t" + ",".join(color_dict[key]) + "\n"
                    else:
                        str_ = key + "\tNA\t" + "NA" + "\n"
                    fw.write(str_)
        # ko_fg = {}
        # for ko in kos:
        #     ko_fg[ko] = []
        #     for gene in ko_genes[ko]:
        #         ko_fg[ko].extend(color_dict[gene])
        #         ko_fg[ko] = list(set(ko_fg[ko]))
        # with open(out_dir + "/kos", "w") as fw:
        #     fw.write("#KO\tbg\tfg\n")
        #     for ko in ko_fg.keys():
        #         if len(ko_fg[ko]) != 0:
        #             fw.write(ko + "\tNA\t" + ",".join(ko_fg[ko]) + "\n")
        #         else:
        #             fw.write(ko + "\tNA\t" + "NA" + "\n")
        self.logger.info("ko文件生成完毕")
        return


    def get_pics(self, ko_genes, path_ko, kos_path, out_dir):
        path_list = path_ko.keys()
        for path in path_list:
            ko_path = kos_path + "/" + path
            cmd = "{} {} {} {} {} {}".format(self.r_path, self.map_path, path, ko_path, out_dir + "/pathways/" + path + ".pdf", self.db_path)
            self.logger.info(cmd)
            try:
                subprocess.check_output(cmd, shell=True)
                png_path = out_dir + "/pathways/" + path + ".png"
                cmd = self.image_magick + ' -flatten -quality 100 -density 130 -background white ' + out_dir + "/pathways/" + path + ".pdf" + ' '  + png_path
                self.logger.info(cmd)
                subprocess.check_output(cmd, shell=True)
            except:
                self.logger.info("{}画图出错".format(path))
                try:
                    db_png_path = self.db_path + path + ".png"
                    self.logger.info(db_png_path)
                    os.link(db_png_path, out_dir + "/pathways/" + path + ".png")
                    cmd = self.image_magick + ' -flatten -quality 100 -density 130 -background white ' + db_png_path + ' ' + out_dir + "/pathways/" + path + ".pdf"
                    self.logger.info(cmd)
                    subprocess.check_output(cmd, shell=True)
                except:
                    self.logger.info('图片格式png转pdf出错')



    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "基因集KEGG功能分类结果目录"],
            # ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        # print self.get_upload_files()
        self.set_end()
        self.fire('end')
        self._upload_result()
        self._import_report_data()
        self.step.finish()
        self.step.update()
        self.logger.info("运行结束!")
        self._save_report_data()
        # super(GenesetClassWorkflow, self).end()

