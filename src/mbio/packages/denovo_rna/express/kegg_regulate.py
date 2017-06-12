# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from Bio.KEGG.KGML import KGML_parser
from Bio.Graphics.KGML_vis import KGMLCanvas
from biocluster.config import Config
import gridfs
import re
import os


class KeggRegulate(object):
    def __init__(self):
        self.mong_db = Config().biodb_mongo_client.sanger_biodb
        # self.pathway_pic = Config().SOFTWARE_DIR + '/database/KEGG/pathway_map2/'

    def get_kgml_and_png(self, pathway_id, kgml_path, png_path):
        collection = self.mong_db['kegg_pathway_png']
        result = collection.find_one({'pathway_id': pathway_id})
        if result:
            kgml_id = result['pathway_ko_kgml']
            png_id = result['pathway_ko_png']
            with open(kgml_path, 'wb') as k, open(png_path, 'wb') as p:
                fs = gridfs.GridFS(self.mong_db)
                k.write(fs.get(kgml_id).read())
                p.write(fs.get(png_id).read())
            return True
        else:
            return False

    def get_regulate_table(self, ko_gene, path_ko, regulate_gene, output):
        """
        生成kegg调控统计表
        ko_gene:ko对应的gene信息:{'ko1': [gene1,gene2], ...,'ko2': [gene1,gene2]}
        path_ko:path对应的ko信息:{'pathway1': [ko1,ko2], ...,'pathway2': [ko1,ko2]}
        output:输出结果的路径
        regulate_dict：gene调控信息:{'up': [gene1,gene2], 'down': [gene1,gene2]}
        """
        colors = ['red', 'yellow', 'blue', "green", 'purple', 'pink']
        with open(output, 'wb') as w:
            # w.write('Pathway_id\tKo_ids\tup_numbers\tdown_numbers\tup_genes\tdown_genes\n')
            # modified by qindanhua add 7 line 支持两个以上的基因集统计
            genelist_names = regulate_gene.keys()
            w.write('Pathway_id\tKo_ids\t')
            for gn in genelist_names:
                w.write("{}_numbers\t{}_genes\t".format(gn, gn))
            w.write("\n")

            for path in path_ko:
                ko_ids = set(path_ko[path])
                # up_genes = []
                # down_genes = []
                write_dict = {}
                for gn in genelist_names:
                    write_dict[gn] = []
                for ko in ko_ids:
                    # print ko
                    genes = set(ko_gene[ko])
                    for gn in genelist_names:
                        geneset = set(regulate_gene[gn])
                        same_gene = genes & geneset
                        # print same_genes
                        if len(same_gene) > 0:
                            # print same_gene
                            for sg in same_gene:
                                write_dict[gn].append('{}({})'.format(sg, ko))
                # print write_dict
                count = 0
                link = 'http://www.genome.jp/kegg-bin/show_pathway?' + path
                # print link
                for gn in write_dict:
                    count += len(write_dict[gn])
                if count > 0:
                    w.write('{}\t{}\t'.format(path, ";".join(ko_ids)))
                    for n, gn in enumerate(write_dict):
                        geneko = write_dict[gn]
                        w.write("{}\t{}\t".format(len(write_dict[gn]), ";".join(write_dict[gn])))
                        for gk in geneko:
                            gko = gk.split('(')[-1][:-1]
                            # print gko
                            color_gk = '/' + gko + '%09' + colors[n]
                            link += color_gk
                            # print link
                        # print link
                    w.write('{}'.format(link))
                    w.write("\n")

    def get_pictrue(self, path_ko, out_dir, regulate_dict=None):
            """
            传入path_ko统计信息，生成pathway绘图文件夹
            path_ko：path对应的ko信息:{'pathway': [ko1,ko2], ...,'pathway': [ko1,ko2]}
            out_dir:输出结果的目录
            regulate_dict：ko调控信息:{'up': [ko1,ko2], 'up_down': [ko1,ko2],'down': [ko1,ko2]}
            """
            colors = ['#EEEE00', '#CC0000', '#388E3C', "#EE6AA7", '#9B30FF', '#7B68EE']
            for path in path_ko:
                koid = path_ko[path]
                l = {}
                kgml_path = out_dir + '/kgml_tmp.kgml'
                png_path = out_dir + '/{}.png'.format(path)
                result = self.get_kgml_and_png(pathway_id=path, kgml_path=kgml_path, png_path=png_path)
                if result:
                    pathway = KGML_parser.read(open(kgml_path))
                    pathway.image = png_path
                    for ko in koid:
                        for degree in pathway.entries.values():
                            if re.search(ko, degree.name):
                                l[degree.id] = ko
                    if not regulate_dict == None:
                        for theid in l:
                            for graphic in pathway.entries[theid].graphics:
                                # modified by qindanhua 20170602 适应基因集的修改，输入的字典名称根据基因集名臣变化，不限制于上下调基因
                                for n, gs in enumerate(regulate_dict):
                                    if l[theid] in regulate_dict[gs]:
                                        graphic.fgcolor = colors[n]
                                # if l[theid] in regulate_dict['up_down']:
                                #     graphic.fgcolor = '#EEEE00'
                                # elif l[theid] in regulate_dict['up']:
                                #     graphic.fgcolor = '#CC0000'
                                # elif l[theid] in regulate_dict['down']:
                                #     graphic.fgcolor = '#388E3C'
                                # else:
                                #     print theid
                    canvas = KGMLCanvas(pathway, import_imagemap=True)
                    canvas.draw(out_dir + '/' + path + '.pdf')
                    os.remove(kgml_path)
