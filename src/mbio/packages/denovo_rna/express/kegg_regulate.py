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
        with open(output, 'wb') as w:
            w.write('Pathway_id\tKo_ids\tup_numbers\tdown_numbers\tup_genes\tdown_genes\n')
            for path in path_ko:
                ko_ids = set(path_ko[path])
                up_genes = []
                down_genes = []
                for ko in ko_ids:
                    genes = ko_gene[ko]
                    for g in genes:
                        if g in regulate_gene['up']:
                            up_genes.append('{}({})'.format(g, ko))
                        if g in regulate_gene['down']:
                            down_genes.append('{}({})'.format(g, ko))
                w.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(path, ';'.join(ko_ids), ';'.join(up_genes), ';'.join(down_genes), len(up_genes), len(down_genes)))

    def get_pictrue(self, path_ko, out_dir, regulate_dict=None):
            """
            传入path_ko统计信息，生成pathway绘图文件夹
            path_ko：path对应的ko信息:{'pathway': [ko1,ko2], ...,'pathway': [ko1,ko2]}
            out_dir:输出结果的目录
            regulate_dict：ko调控信息:{'up': [ko1,ko2], 'up_down': [ko1,ko2],'down': [ko1,ko2]}
            """
            for path in path_ko:
                koid = path_ko[path]
                l = {}
                kgml_path = out_dir + '/kgml_tmp.kgml'
                png_path = out_dir + '/png_tmp.png'
                result = self.get_kgml_and_png(pathway_id=path, kgml_path=kgml_path, png_path=png_path)
                if result:
                    pathway = KGML_parser.read(open(kgml_path))
                    pathway.image = png_path
                    for ko in koid:
                        for degree in pathway.entries.values():
                            if re.search(ko, degree.name):
                                l[degree.id] = ko
                    for theid in l:
                        for graphic in pathway.entries[theid].graphics:
                            if l[theid] in regulate_dict['up_down']:
                                graphic.fgcolor = '#EEEE00'
                            elif l[theid] in regulate_dict['up']:
                                graphic.fgcolor = '#CC0000'
                            elif l[theid] in regulate_dict['down']:
                                graphic.fgcolor = '#388E3C'
                            else:
                                print theid
                    canvas = KGMLCanvas(pathway, import_imagemap=True)
                    canvas.draw(out_dir + '/' + path + '.pdf')
                    os.remove(kgml_path)
                    os.remove(png_path)
