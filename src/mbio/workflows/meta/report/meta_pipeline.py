# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'
from biocluster.workflow import Workflow
from biocluster.config import Config
from bson.objectid import ObjectId
from types import StringTypes
import datetime
import os
import json
import gevent
import time


class MetaPipelineWorkflow(Workflow):
    """
    用于meta一键化交互的计算
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaPipelineWorkflow, self).__init__(wsheet_object)
        self._config = Config()
        self._client = self._config.mongo_client
        self.db = self._client['tsanger']
        options = [
            {"name": "data", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "pipe_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.pipe_submit_all = self.add_tool('meta.pipe_submit')
        self.analysis_table = {"randomforest_analyse": "sg_randomforest", "sixteens_prediction": "sg_16s",
                               "species_lefse_analyse": "sg_species_difference_lefse",
                               "alpha_rarefaction_curve": "sg_alpha_rarefaction_curve",
                               "otunetwork_analyse": "sg_network", "roc_analyse": "sg_roc",
                               "alpha_diversity_index": "sg_alpha_diversity", "alpha_ttest": "sg_alpha_ttest",
                               "beta_multi_analysis_plsda": "sg_beta_multi_analysis",
                               "beta_sample_distance_hcluster_tree": "sg_newick_tree",
                               "beta_multi_analysis_pearson_correlation": "sg_species_env_correlation",
                               "beta_multi_analysis_rda_cca": "sg_beta_multi_analysis", "hc_heatmap": "sg_hc_heatmap",
                               "beta_multi_analysis_anosim": "sg_beta_multi_anosim",
                               "species_difference_multiple": "sg_species_difference_check",
                               "beta_multi_analysis_results": "sg_species_mantel_check",
                               "beta_multi_analysis_pcoa": "sg_beta_multi_analysis",
                               "beta_multi_analysis_dbrda": "sg_beta_multi_analysis",
                               "species_difference_two_group": "sg_species_difference_check",
                               "beta_multi_analysis_nmds": "sg_beta_multi_analysis", "otu_group_analyse": "sg_otu",
                               "otu_venn": "sg_otu_venn", "beta_multi_analysis_pca": "sg_beta_multi_analysis",
                               "corr_network_analyse": "sg_corr_network", "otu_pan_core": "sg_otu_pan_core",
                               "plot_tree": "sg_phylo_tree", "enterotyping": "sg_enterotyping"}

    def run_all(self):
        """
        运行子分析接口投递的tool
        :return:
        """
        options = {
            'data': self.option("data"),
            'pipe_id': self.option("pipe_id"),
            'task_id': self.get_task_id()
        }
        self.pipe_submit_all.set_options(options)
        # self.pipe_submit_all.on('end', self.get_results)
        self.pipe_submit_all.on('end', self.end)
        self.pipe_submit_all.run()

    def get_task_id(self):
        split_id = self._sheet.id.split('_')
        split_id.pop()
        split_id.pop()
        self.task_id = '_'.join(split_id)
        return self.task_id

    def get_results(self):
        """
        等待tool运行完成之后，获得所有子分析的ids文件，通过该文件，来设定sg_status中的状态
        :return:
        """
        ids_path = self.work_dir + '/PipeSubmitAll/ids.txt'
        if not os.path.isfile(ids_path):
            raise Exception("%s 文件不存在，查看投递的tool！" % (ids_path))
        with open(ids_path, 'r') as r:
            for line in r:
                all_results = line.strip("\n")
        gevent.spawn(self.watch_end, all_results, self.option("pipe_id"))

    def run(self):
        """
        这里更具前端传进来的参数type，设置all，医口，农口
        :return:
        """
        self.run_all()
        super(MetaPipelineWorkflow, self).run()

    def end(self):
        super(MetaPipelineWorkflow, self).end()

    def set_db(self, all_results, main_table_id):
        """
        进行导表
        :param all_results:
        :param main_table_id:
        :return:
        """
        collection_pipe = self.db["sg_pipe_batch"]
        pipe_result = collection_pipe.find_one(
            {"_id": ObjectId(main_table_id)})
        project_sn = pipe_result['project_sn']
        task_id = pipe_result['task_id']
        otu_id = pipe_result['otu_id']
        status = pipe_result['status']
        all_data = json.loads(self.option("data"))
        all_results = eval(all_results)
        result_data = []
        # 判断pan_core这一特例 返回的id是一个列表, 这里将这个列表拆成2部分，重组成一个字典，并存入到all_result的列表中
        for id in all_results:
            if 'sub_anaylsis_id' in id.keys() and isinstance(id['sub_anaylsis_id'], list):
                for m in id['sub_anaylsis_id']:
                    params = {"sub_anaylsis_id": m, "level_id": id['level_id'], 'group_id': id['group_id'],
                              "submit_location": id['submit_location']}
                    result_data.append(params)
            else:
                result_data.append(id)
        all_results = result_data
        level = str(all_data['level_id']).strip().split(",")
        levels = []
        for m in level:
            levels.append(m)
        min_level = int(sorted(levels)[-1])
        try:
            first_group_id = str(all_data['first_group_id'])
        except:
            raise Exception("data中没有first_group_id这个字段，请仔细检查下！")
        group_infos = all_data['group_info']
        group_infos = eval(group_infos)
        level_name = ["Domain", "Kingdom", "Phylum", "Class",
                      "Order", "Family", "Genus", "Species", "OTU"]
        for level in levels:
            for group in group_infos:
                group_id = group['group_id']
                if str(group_id) == 'all':
                    group_id = 'all'
                    name = level_name[
                        int(level) - 1] + "All" + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    desc = level_name[int(level) - 1] + "与" + "All" + "组合结果"
                else:
                    group_id = ObjectId(group_id)
                    name = level_name[int(level) - 1] + self.find_group_name(
                        group_id).capitalize() + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    desc = level_name[int(level) - 1] + "与" + \
                        self.find_group_name(group_id) + "组合结果"
                insert_data = {
                    "project_sn": project_sn,
                    "task_id": task_id,
                    "otu_id": otu_id,
                    "pipe_batch_id": ObjectId(main_table_id),
                    "status": status,
                    "name": name,
                    "desc": desc,
                    "group_id": group_id,
                    "level_id": level,
                    "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                try:
                    collection_pipe_main = self.db['sg_pipe_main']
                    inserted_id = collection_pipe_main.insert_one(
                        insert_data).inserted_id
                except:
                    raise Exception("导入%s到sg_pipe_main失败！" % (desc))
                else:
                    print "耶！导入%s到sg_pipe_main成功！" % (desc)
                if int(level) == min_level and str(group_id) == first_group_id:
                    self.get_picture_id(
                        pipe_main_id=inserted_id, main_table_id=main_table_id)
                for anaylsis in all_results:
                    if 'sub_anaylsis_id' in anaylsis.keys():
                        if str(anaylsis['group_id']) == str(group['group_id']) and str(anaylsis['level_id']) == str(level):
                            if str(group['group_id']) == "all":
                                group_name = "All"
                                group_id = "all"
                            else:
                                group_name = self.find_group_name(
                                    str(group['group_id']))
                                group_id = ObjectId(str(group['group_id']))
                            sub_anaylsis_main_id = str(
                                anaylsis['sub_anaylsis_id']['id'])
                            collections_sub = self.db[
                                self.analysis_table[anaylsis['submit_location']]]
                            result = collections_sub.find_one(
                                {"_id": ObjectId(sub_anaylsis_main_id)})
                            try:
                                mongo_data = {
                                    "pipe_main_id": ObjectId(inserted_id),
                                    "pipe_batch_id": ObjectId(main_table_id),
                                    "status": result['status'],
                                    "table_id": result['_id'],
                                    "table_name": result['name'],
                                    "type_name": self.analysis_table[anaylsis['submit_location']],
                                    "desc": result['desc'],
                                    "time": result['created_ts'],
                                    "params": result['params'],
                                    "submit_location": anaylsis['submit_location'],
                                    "is_new": 'is_new',
                                    "task_id": result['task_id'],
                                    "group_name": group_name,
                                    "group_id": group_id,
                                    "level_id": str(level),
                                    "level_name": level_name[int(level) - 1]
                                }
                            except:
                                mongo_data = {
                                    "pipe_main_id": ObjectId(inserted_id),
                                    "pipe_batch_id": ObjectId(main_table_id),
                                    "status": result['status'],
                                    "table_id": result['_id'],
                                    "table_name": result['name'],
                                    "type_name": "",
                                    "desc": result['desc'],
                                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "params": "",
                                    "submit_location": anaylsis['submit_location'],
                                    "is_new": "new",
                                    "task_id": task_id,
                                    "group_name": group_name,
                                    "group_id": group_id,
                                    "level_id": str(level),
                                    "level_name": level_name[int(level) - 1]
                                }
                            try:
                                collection_pipe_detail = self.db[
                                    'sg_pipe_detail']
                                collection_pipe_detail.insert_one(mongo_data)
                            except:
                                raise Exception("分类水平%s——分组方案%s，导入到sg_pipe_detail失败！" % (
                                    level_name[int(level) - 1], group_name))
                    else:
                        if str(anaylsis['group_id']) == str(group['group_id']) and str(anaylsis['level_id']) == str(level):
                            if str(group['group_id']) == "all":
                                group_name = "All"
                                group_id = "all"
                            else:
                                group_name = self.find_group_name(
                                    str(group['group_id']))
                                group_id = ObjectId(str(group['group_id']))
                            mongo_data = {
                                "pipe_main_id": ObjectId(inserted_id),
                                "pipe_batch_id": ObjectId(main_table_id),
                                "status": "failed",
                                "table_id": "",
                                "table_name": "",
                                "type_name": "",
                                "desc": anaylsis['info'],
                                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "params": "",
                                "submit_location": anaylsis['submit_location'],
                                "is_new": 'new',
                                "task_id": task_id,
                                "group_name": group_name,
                                "level_name": level_name[int(level) - 1],
                                "group_id": group_id,
                                "level_id": str(level)
                            }
                            try:
                                collection_pipe_detail = self.db[
                                    'sg_pipe_detail']
                                collection_pipe_detail.insert_one(mongo_data)
                            except:
                                raise Exception("分类水平%s——分组方案%s，导入到sg_pipe_detail失败！" % (
                                    level_name[int(level) - 1], group_name))
        print "所有的表均导成功了yeyeye！程序已经运行完成！"
        self.end()

    def watch_end(self, all_results, main_table_id):
        """
        用于10s检测一次所有子分析是否已经计算完成
        :param all_results: 所有子分析的返回结果信息
        :param main_table_id: 批次表的ID
        :return:
        """
        times = 1
        while True:
            time.sleep(60)
            times += 10
            print times
            if int(times) >= 86400:
                raise Exception("程序没有响应，超过86400s自动终止！")  # 48小时没有响应就终止
            if self.check_all(all_results, main_table_id):
                self.set_db(all_results, main_table_id)
                break

    def check_all(self, all_results, main_table_id):
        """
        遍历所有的子分析的主表，查找子分析的状态是不是为failed或者end状态，用于判断计算是否结束
        :param all_results:
        :param main_table_id:
        :return:
        """
        collection_pipe = self.db["sg_pipe_batch"]
        anaysis_num = []
        no_table_analysis_num = []
        print "-------------------------------------------------------"
        all_results = eval(all_results)
        result_data = []
        for id in all_results:
            if 'sub_anaylsis_id' in id.keys() and isinstance(id['sub_anaylsis_id'], list):
                for m in id['sub_anaylsis_id']:
                    params = {"sub_anaylsis_id": m, "level_id": id['level_id'], 'group_id': id['group_id'],
                              "submit_location": id['submit_location']}
                    result_data.append(params)
            else:
                result_data.append(id)
        all_results = result_data
        for id in all_results:
            if 'sub_anaylsis_id' in id.keys():
                if 'id' in id['sub_anaylsis_id'].keys():
                    sub_anaylsis_main_id = str(id['sub_anaylsis_id']['id'])
                    try:
                        collection_status = self.db[
                            self.analysis_table[id['submit_location']]]
                        result = collection_status.find_one(
                            {"_id": ObjectId(sub_anaylsis_main_id)}, {"status": 1})
                    except:
                        raise Exception("%s分析没有找到%s主表对应的数据，请检查" % (
                            id['submit_location'], sub_anaylsis_main_id))  # 临时先这样排查状态不更新
                    if result and result['status'] != 'start':
                        anaysis_num.append(result['status'])
                    else:
                        print "没有找到%s对应的表，该分析还在计算中，请继续等候！" % (sub_anaylsis_main_id)
                elif isinstance(id['sub_anaylsis_id'], list):
                    for m in id['sub_anaylsis_id']:
                        sub_anaylsis_main_id = str(m['id'])
                        try:
                            collection_status = self.db[
                                self.analysis_table[id['submit_location']]]
                            result = collection_status.find_one(
                                {"_id": ObjectId(sub_anaylsis_main_id)}, {"status": 1})
                        except:
                            raise Exception("%s分析没有找到%s主表对应的数据，请检查" % (
                                id['submit_location'], sub_anaylsis_main_id))
                        if result and result['status'] != 'start':
                            anaysis_num.append(result['status'])
                        else:
                            print "没有找到%s对应的表，该分析还在计算中，请继续等候！" % (sub_anaylsis_main_id)
            else:
                no_table_analysis_num.append(str(id['submit_location']))

        if len(all_results) - len(no_table_analysis_num) == len(anaysis_num):
            data = {
                "status": "end",
                "percent": str(len(all_results)) + "/" + str(len(all_results))
            }
            try:
                collection_pipe.update({"_id": ObjectId(main_table_id)}, {
                                       '$set': data}, upsert=False)
            except:
                raise Exception("sg_pipe_batch状态更新失败，请检查！")
            else:
                print '所有子分析均计算完成，sg_pipe_batch状态更新成功。'
            m = True
        else:
            ready_analysis_num = len(anaysis_num) + len(no_table_analysis_num)
            percent = str(ready_analysis_num) + "/" + str(len(all_results))
            data = {
                "percent": percent
            }
            try:
                collection_pipe.update({"_id": ObjectId(main_table_id)}, {
                                       '$set': data}, upsert=False)
            except:
                raise Exception("sg_pipe_batch进度条更新失败，请检查！")
            m = False
        return m

    def find_group_name(self, group_id):
        """
        根据group_id去找group_name
        :param group_id:
        :return:
        """
        if group_id != 0 and not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_specimen_group"]
        result = collection.find_one({"_id": group_id})
        if result:
            group_name = result['group_name']
        else:
            print "没有找到group_id对应的group_name！"
            group_name = " "
        return str(group_name)

    def get_picture_id(self, pipe_main_id, main_table_id):
        """
        批量导图片时使用， 目前选取的是最低分类水平，分组方案中all后面的第一个分组
        :param pipe_main_id:
        :param main_table_id:
        :return:
        """
        if pipe_main_id != 0 and not isinstance(pipe_main_id, ObjectId):
            if isinstance(pipe_main_id, StringTypes):
                pipe_main_id = ObjectId(pipe_main_id)
            else:
                raise Exception("pipe_main_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_pipe_batch"]
        data = {
            "pipe_main_id": pipe_main_id
        }
        try:
            collection.update({"_id": ObjectId(main_table_id)}, {
                              '$set': data}, upsert=False)
        except:
            raise Exception("sg_pipe_batch中pipe_main_id更新失败，请检查！")
