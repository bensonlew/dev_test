# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
# last_modify:20161205

import os
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import json
import re
import gridfs
import glob


class RefRnaGeneset(Base):
    def __init__(self, bind_object):
        super(RefRnaGeneset, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @report_check
    def add_main_table(self, collection_name, params, name):
        """
        添加主表的导表函数
        :param collection_name: 主表的collection名字
        :param params: 主表的参数
        :param name: 主表的名字
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "status": "end",
            "name": name,
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": json.dumps(params, sort_keys=True, separators=(',', ':'))
        }

        collection = self.db[collection_name]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def add_geneset_cog_detail(self, geneset_cog_table, geneset_cog_id):
        """
        cog详情表导表函数
        :param geneset_cog_table:cog结果表
        :param geneset_cog_id:主表ID
        :return:
        """
        data_list = []
        geneset_name = []
        with open(geneset_cog_table, 'r') as f:
            first_line = f.readline().strip("\n").split("\t")
            print first_line
            # print f.next().split("\t")

            for gn in first_line[2:]:
                if "list" in gn or "LIST" in gn:
                    continue
                elif not gn[:-4] in geneset_name:
                    geneset_name.append(gn[:-4])
            self.bind_object.logger.error(geneset_name)
            for line in f:
                line = line.strip().split("\t")
                data = {
                    'geneset_cog_id': ObjectId(geneset_cog_id),
                    'type': line[0],
                    'function_categories': line[1]
                }
                for n, gn in enumerate(geneset_name):
                    data[gn + "_cog"] = int(line[6*n+2])
                    data[gn + "_nog"] = int(line[6*n+3])
                    data[gn + "_kog"] = int(line[6*n+4])
                    if data[gn + "_cog"] == 0:
                        data[gn + "_cog_list"] = ""
                        data[gn + "_cog_str"] = ""
                    else:
                        data[gn + "_cog_list"] = line[6*n+5].split(";")
                        data[gn + "_cog_str"] = line[6*n+5]
                    if data[gn + "_nog"] == 0:
                        data[gn + "_nog_str"] = ""
                        data[gn + "_nog_list"] = ""
                    else:
                        data[gn + "_nog_str"] = line[6*n+6]
                        data[gn + "_nog_list"] = line[6*n+6].split(";")
                    if data[gn + "_kog"] == 0:
                        data[gn + "_kog_list"] = ""
                        data[gn + "_kog_str"] = ""
                    else:
                        data[gn + "_kog_list"] = line[6*n+7].split(";")
                        data[gn + "_kog_str"] = line[6*n+7]
                data_list.append(data)
        try:
            collection = self.db['sg_geneset_cog_class_detail']
            main_collection = self.db['sg_geneset_cog_class']
            collection.insert_many(data_list)
            main_collection.update({"_id": ObjectId(geneset_cog_id)}, {"$set": {"table_columns": geneset_name, "status": "end"}})
            self.bind_object.logger.error(geneset_name)
        except Exception, e:
            self.bind_object.logger.error("导入cog表格：%s出错:%s" % (geneset_cog_table, e))
        else:
            self.bind_object.logger.error("导入cog表格：%s成功!" % (geneset_cog_table))

    @report_check
    def add_go_enrich_detail(self, go_enrich_id, go_enrich_dir):
        """
        GO富集详情导表函数
        :param go_enrich_id: 主表ID
        :param go_enrich_dir: 结果文件（不是文件夹）
        :return:
        """
        if not isinstance(go_enrich_id, ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(go_enrich_dir):
            raise Exception('{}所指定的路径不存在。请检查！'.format(go_enrich_dir))
        data_list = []
        with open(go_enrich_dir, 'r') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('go_enrich_id', go_enrich_id),
                    ('go_id', line[0]),
                    ('go_type', line[1]),
                    ('enrichment', line[2]),
                    ('discription', line[3]),
                    ('ratio_in_study', line[4]),
                    ('ratio_in_pop', line[5]),
                    ('p_uncorrected', float(line[6])),
                    ('p_corrected', float(line[9])),
                    ('depth', int(line[7])),
                    ('study_count', int(line[4].split("/")[0])),
                    ('pop_count', int(line[5].split("/")[0])),
                    ('gene_list', line[-1]),
                    ('gene_str', line[-1].split(";"))
                ]
                # if float(line[8]):
                #     m = re.match(r"(.+)/(.+)", line[5])
                #     pop_count = int(m.group(1))
                #     line[6] = float(line[6])
                #     line[7] = int(line[7])
                #     line[8] = int(line[8])
                #     line[9] = float(line[9])
                #     line[10] = float(line[10])
                #     line[11] = float(line[11])
                #     line[12] = float(line[12])
                #     data = [
                #         ('go_enrich_id', go_enrich_id),
                #         ('go_id', line[0]),
                #         ('go_type', line[1]),
                #         ('enrichment', line[2]),
                #         ('discription', line[3]),
                #         ('ratio_in_study', line[4]),
                #         ('ratio_in_pop', line[5]),
                #         ('p_uncorrected', line[6]),
                #         ('depth', line[7]),
                #         ('study_count', line[8]),
                #         ('pop_count', pop_count),
                #         ('gene_list', line[13]),
                #     ]
                #     try:
                #         data += [('p_bonferroni', line[9])]
                #     except:
                #         data += [('p_bonferroni', '')]
                #     try:
                #         data += [('p_sidak', line[10])]
                #     except:
                #         data += [('p_sidak', '')]
                #     try:
                #         data += [('p_holm', line[11])]
                #     except:
                #         data += [('p_holm', '')]
                #     try:
                #         data += [('p_fdr', line[12])]
                #     except:
                #         data += [('p_fdr', '')]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_geneset_go_enrich_detail']
            collection.insert_many(data_list)
            # main_collection = self.db['sg_geneset_go_enrich']
            # main_collection.update({"_id": ObjectId(go_enrich_id)}, {"$set": {"status": "end"}})
        except Exception, e:
            print("导入go富集信息：%s出错:%s" % (go_enrich_dir, e))
        else:
            print("导入go富集信息：%s成功!" % (go_enrich_dir))

    @report_check
    def update_directed_graph(self, go_enrich_id, go_graph_dir):
        collection = self.db['sg_geneset_go_enrich']
        fs = gridfs.GridFS(self.db)
        gra = fs.put(open(go_graph_dir, 'rb'))
        try:
            collection.update({"_id": ObjectId(go_enrich_id)}, {"$set": {'go_directed_graph': gra}})
        except Exception, e:
            print("导入%s信息出错：%s" % (go_graph_dir, e))
        else:
            print("导入%s信息成功！" % (go_graph_dir))

    @report_check
    def add_kegg_enrich_detail(self, enrich_id, kegg_enrich_table, geneset_list_path, all_list_path):
        """
        KEGG富集详情表导表函数
        :param enrich_id: 主表id
        :param kegg_enrich_table: 结果表
        :return:
        """
        if not isinstance(enrich_id, ObjectId):
            if isinstance(enrich_id, types.StringTypes):
                enrich_id = ObjectId(enrich_id)
            else:
                raise Exception('kegg_enrich_id必须为ObjectId对象或其对应的字符串!')
        if not os.path.exists(kegg_enrich_table):
            raise Exception('kegg_enrich_table所指定的路径:{}不存在，请检查！'.format(kegg_enrich_table))
        data_list = []
        geneset_length = len(open(geneset_list_path, "r").readlines())
        all_list_length = len(open(all_list_path, "r").readlines())
        with open(kegg_enrich_table, 'rb') as r:
            for line in r:
                if re.match(r'\w', line):
                    line = line.strip('\n').split('\t')
                    insert_data = {
                        'kegg_enrich_id': enrich_id,
                        'term': line[0],
                        'database': line[1],
                        'id': line[2],
                        'study_number': int(line[3]),
                        'backgroud_number': int(line[4]),
                        'ratio_in_study': line[3] + "/" + str(geneset_length),
                        'ratio_in_pop': line[4] + "/" + str(all_list_length),
                        'pvalue': round(float(line[5]), 4),
                        'corrected_pvalue': round(float(line[-3]), 4) if not line[-3] == "None" else "None",
                        'gene_lists': line[-2],
                        'hyperlink': line[-1]
                    }
                    data_list.append(insert_data)
            if data_list:
                try:
                    collection = self.db['sg_geneset_kegg_enrich_detail']
                    collection.insert_many(data_list)
                    # main_collection = self.db['sg_geneset_kegg_enrich']
                    # main_collection.update({"_id": ObjectId(enrich_id)}, {"$set": {"status": "end"}})
                except Exception, e:
                    self.bind_object.logger.error("导入kegg富集统计表：%s信息出错:%s" % (kegg_enrich_table, e))
                else:
                    self.bind_object.logger.info("导入kegg富集统计表:%s信息成功!" % kegg_enrich_table)
            else:
                coll = self.db['sg_geneset_kegg_enrich']
                coll.update({'_id': enrich_id}, {'$set': {'desc': 'no_result'}})
                self.bind_object.logger.info("kegg富集统计表没结果：" % kegg_enrich_table)

    @report_check
    def add_go_regulate_detail(self, go_regulate_dir, go_regulate_id):
        """
        :param go_regulate_id: 主表ID
        :param go_regulate_dir: GO上下调结果
        :return:
        """
        data_list = []
        # geneset_name = []
        if not isinstance(go_regulate_id, ObjectId):
            if isinstance(go_regulate_id, types.StringTypes):
                go_regulate_id = ObjectId(go_regulate_id)
            else:
                raise Exception('go_enrich_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(go_regulate_dir):
            raise Exception('{}所指定的路径不存在，请检查！'.format(go_regulate_dir))
        with open(go_regulate_dir, 'r') as f:
            first_line = f.readline().strip().split("\t")
            doc_keys = set([l.split(" ")[0] for l in first_line[3:]])
            geneset_name = doc_keys
            # print first_line[3:]
            # print f.next()
            print doc_keys
            for line in f:
                line = line.strip().split('\t')
                data = {
                    'go_regulate_id': go_regulate_id,
                    'go_type': line[0],
                    'go': line[1],
                    'go_id': line[2]
                }
                # print line[3]
                for n, dk in enumerate(doc_keys):
                    # print n+3
                    line4 = line[4+n*3].split("(")
                    data["{}_num".format(dk)] = int(line[3+n*3])
                    data["{}_percent".format(dk)] = float(line4[0])
                    # data["{}_percent_str".format(dk)] = line4[1][:-1] if len(line4[1][:-1]) > 1 else "0"
                    data["{}_str".format(dk)] = line[5+n*3]
                    data["{}_genes".format(dk)] = line[5+n*3].split(";")
                    if len(line4) > 1:
                        data["{}_percent_str".format(dk)] = line4[1][:-1]
                    else:
                        data["{}_percent_str".format(dk)] = 0
                data_list.append(data)
            try:
                collection = self.db['sg_geneset_go_class_detail']
                main_collection = self.db['sg_geneset_go_class']
                collection.insert_many(data_list)
                main_collection.update({"_id": ObjectId(go_regulate_id)}, {"$set": {"table_columns": list(geneset_name)}})
                self.bind_object.logger.info("llllllll")
                self.bind_object.logger.info(geneset_name)
                self.bind_object.logger.info(ObjectId(go_regulate_id))
            except Exception, e:
                self.bind_object.logger.info("导入go调控信息：%s出错:%s" % (go_regulate_dir, e))
            else:
                self.bind_object.logger.info("导入go调控信息：%s成功!" % (go_regulate_dir))

    @report_check
    def add_kegg_regulate_pathway(self, pathway_dir, regulate_id):
        """

        :param regulate_id: 主表id
        :param pathway_dir:~/output/pathway 结果图片文件夹
        :return:
        """
        if not isinstance(regulate_id, ObjectId):
            if isinstance(regulate_id, types.StringTypes):
                regulate_id = ObjectId(regulate_id)
            else:
                raise Exception('kegg_regulate_id必须为ObjectId对象或其对应的字符串!')
        if not os.path.exists(pathway_dir):
            raise Exception('pathway_dir所指定的路径:{}不存在，请检查！'.format(pathway_dir))
        data_list = []
        png_files = glob.glob("{}/*.png".format(pathway_dir))
        pdf_files = glob.glob("{}/*.pdf".format(pathway_dir))
        fs = gridfs.GridFS(self.db)
        for f in png_files:
            # png_id = fs.put(open(os.path.join(pathway_dir, f), 'rb'))
            f_name = os.path.basename(f).split(".")[0]
            png_id = fs.put(open(f, 'rb'))
            pdf_id = fs.put(open(os.path.join(pathway_dir, f_name + ".pdf"), 'rb'))
            insert_data = {
                'kegg_id': regulate_id,
                'pathway_png': png_id,
                'pathway_pdf': pdf_id,
                'pathway_id': f_name
            }
            data_list.append(insert_data)
        try:
            collection = self.db['sg_geneset_kegg_class_pathway']
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.info("导入kegg调控pathway：%s信息出错:%s" % (pathway_dir, e))
        else:
            self.bind_object.logger.info("导入kegg调控pathway:%s信息成功!" % pathway_dir)

    @report_check
    def add_kegg_regulate_detail(self, regulate_id, kegg_regulate_table):
        """

        :param regulate_id: 主表ID
        :param kegg_regulate_table: kegg_stat.xls统计结果文件
        :return:
        """
        main_collection = self.db['sg_geneset_kegg_class']
        kegg_main = self.db['sg_annotation_kegg']
        kegg_level_coll = self.db['sg_annotation_kegg_level']
        if not isinstance(regulate_id, ObjectId):
            if isinstance(regulate_id, types.StringTypes):
                regulate_id = ObjectId(regulate_id)
            else:
                raise Exception('kegg_regulate_id必须为ObjectId对象或其对应的字符串!')
        if not os.path.exists(kegg_regulate_table):
            raise Exception('kegg_regulate_table所指定的路径:{}不存在，请检查！'.format(kegg_regulate_table))
        task_id = main_collection.find_one({"_id": regulate_id})['task_id']
        kegg_main_id = kegg_main.find_one({"task_id": task_id})['_id']
        kegg_result = kegg_level_coll.find({"kegg_id": kegg_main_id})
        print kegg_main_id
        path_def = {}
        for kr in kegg_result:
            # print kr['pathway_definition']
            path_def[kr['pathway_id'].split(":")[-1]] = kr['pathway_definition']
        # print path_def
        # print task_id
        data_list = []
        with open(kegg_regulate_table, 'rb') as r:
            first_line = r.readline().strip().split("\t")[2:]
            # print r.next()
            genesets_name = []
            for fl in first_line:
                if "numbers" in fl:
                    genesets_name.append(fl[:-8])
            print genesets_name
            # print first_line
            for line in r:
                line = line.strip('\n').split('\t')
                insert_data = {
                    'kegg_id': regulate_id,
                    'pathway_id': line[0],
                    'ko_ids': line[1],
                    'pathway_definition': path_def[line[0]],
                    'link': line[-1]
                }
                # print path_def[line[0]]
                for n, gn in enumerate(genesets_name):
                    gene_list = re.findall(r"(.*?)\(.*?\);", line[3+2*n])
                    insert_data["{}_geneko".format(gn)] = line[3+2*n]
                    insert_data["{}_numbers".format(gn)] = line[2+2*n]
                    insert_data["{}_genes".format(gn)] = gene_list
                    insert_data["{}_str".format(gn)] = ";".join(gene_list)
                data_list.append(insert_data)
            try:
                collection = self.db['sg_geneset_kegg_class_detail']
                # main_collection = self.db['sg_geneset_kegg_class']
                collection.insert_many(data_list)
                main_collection.update({"_id": ObjectId(regulate_id)}, {"$set": {"table_columns": genesets_name}})
            except Exception, e:
                self.bind_object.logger.info("导入kegg调控统计表：%s信息出错:%s" % (kegg_regulate_table, e))
            else:
                self.bind_object.logger.info("导入kegg调控统计表:%s信息成功!" % kegg_regulate_table)
