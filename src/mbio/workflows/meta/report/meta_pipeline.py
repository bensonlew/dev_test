# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'
from biocluster.workflow import Workflow
from mbio.api.to_file.meta import *
from biocluster.config import Config
from mainapp.libs.signature import CreateSignature
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
        self.pipe_submit_all = self.add_tool('meta.pipe.pipe_submit_all')

    def run_all(self):
        """
        运行子分析接口投递的tool
        :return:
        """
        options = {
            'data': self.option("data"),
            'pipe_id': self.option("pipe_id")
        }
        self.pipe_submit_all.set_options(options)
        self.pipe_submit_all.on('end', self.get_results)
        self.pipe_submit_all.run()


    def get_results(self):
        """
        等待tool运行完成之后，获得所有子分析的ids文件，通过该文件，来设定sg_status中的状态
        :return:
        """
        ids_path = self.work_dir + '/PipeSubmitAll/ids.txt'
        if not os.path.isfile(ids_path):
            raise Exception("%s 文件不存在，查看投递的tool！"%(ids_path))
        with open(ids_path, 'r') as r:
            for line in r:
                all_results = line.strip("\n")
            # print type(eval(all_results))
            print "+++++"
            print all_results

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
        collection_status = self.db["sg_status"]
        collection_pipe = self.db["sg_pipe_batch"]
        pipe_result = collection_pipe.find_one({"_id": ObjectId(main_table_id)})
        project_sn = pipe_result['project_sn']
        task_id = pipe_result['task_id']
        otu_id = pipe_result['otu_id']
        status = pipe_result['status']
        all_data = json.loads(self.option("data"))
        # print all_data
        all_results = eval(all_results)
        result_data = []
        #判断pan_core这一特例 返回的id是一个列表
        for id in all_results:
            if 'sub_anaylsis_id' in id.keys() and isinstance(id['sub_anaylsis_id'], list):
                for m in id['sub_anaylsis_id']:
                    params = {"sub_anaylsis_id": m, "level_id": id['level_id'], 'group_id': id['group_id'],
                              "submit_location": id['submit_location']}
                    result_data.append(params)
            else:
                result_data.append(id)
        # print "打印出含有pan_core results"
        # print result_data
        all_results = result_data
        level = str(all_data['level_id']).strip().split(",")
        levels = []
        for m in level:
            levels.append(m)
        print levels
        group_infos = all_data['group_info']
        group_infos = eval(group_infos)
        level_name = ["Domain", "Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species", "OTU"]
        for level in levels:
            for group in group_infos:
                # print type(group)
                # print "test group type"
                group_id = group['group_id']
                if str(group_id) == 'all':
                    group_id = 'all'
                    name = level_name[int(level) - 1] + "All" + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    desc = level_name[int(level) - 1] + "与" + "All" + "组合结果"
                else:
                    group_id = ObjectId(group_id)
                    name = level_name[int(level)-1] + self.find_group_name(group_id).capitalize() + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    desc = level_name[int(level)-1] + "与" + self.find_group_name(group_id) + "组合结果"
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
                    inserted_id = collection_pipe_main.insert_one(insert_data).inserted_id
                except Exception:
                    print "导入%s到sg_pipe_main失败！"%(desc)
                else:
                    print "耶！导入%s到sg_pipe_main成功！"%(desc)
                print inserted_id
                for anaylsis in all_results:
                    if 'sub_anaylsis_id' in anaylsis.keys():
                        if str(anaylsis['group_id']) == str(group['group_id']) and str(anaylsis['level_id']) == str(level):
                            if str(group['group_id']) == "all":
                                group_name = "All"
                                group_id = "all"
                            else:
                                group_name = self.find_group_name(str(group['group_id']))
                                group_id = ObjectId(str(group['group_id']))
                            sub_anaylsis_main_id = str(anaylsis['sub_anaylsis_id']['id'])
                            result = collection_status.find_one({"table_id": ObjectId(sub_anaylsis_main_id)})
                            mongo_data = {
                                "pipe_main_id": ObjectId(inserted_id),
                                "pipe_batch_id": ObjectId(main_table_id),
                                "status": result['status'],
                                "table_id": result['table_id'],
                                "table_name": result['table_name'],
                                "type_name": result['type_name'],
                                "desc": result['desc'],
                                "time": result['time'],
                                "params": result['params'],
                                "submit_location": result['submit_location'],
                                "is_new": result['is_new'],
                                "task_id": result['task_id'],
                                "group_name": group_name,
                                "group_id": group_id,
                                "level_id": str(level),
                                "level_name": level_name[int(level)-1]
                            }
                            try:
                                collection_pipe_detail = self.db['sg_pipe_detail']
                                collection_pipe_detail.insert_one(mongo_data)
                            except Exception:
                                print "分类水平%s——分组方案%s，导入到sg_pipe_detail失败！"%(level_name[int(level)-1], group_name)
                    else:
                        if str(anaylsis['group_id']) == str(group['group_id']) and str(anaylsis['level_id']) == str(level):
                            if str(group['group_id']) == "all":
                                group_name = "All"
                                group_id = "all"
                            else:
                                group_name = self.find_group_name(str(group['group_id']))
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
                                collection_pipe_detail = self.db['sg_pipe_detail']
                                collection_pipe_detail.insert_one(mongo_data)
                            except Exception:
                                print "分类水平%s——分组方案%s，导入到sg_pipe_detail失败！" % (level_name[int(level) - 1], group_name)

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
            time.sleep(10)
            times += 10
            print times
            if int(times) >= 172800:
                raise Exception("程序没有响应，超过172800s自动终止！")
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
        collection_status = self.db["sg_status"]
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
        # print result_data
        all_results = result_data
        for id in all_results:
            if 'sub_anaylsis_id' in id.keys():
                if 'id' in id['sub_anaylsis_id'].keys():
                    sub_anaylsis_main_id = str(id['sub_anaylsis_id']['id'])
                    result = collection_status.find_one({"table_id": ObjectId(sub_anaylsis_main_id)})
                    if result and result['status'] != 'start':
                        anaysis_num.append(result['status'])
                    else:
                        print "sg_status中没有找到%s对应的表，该分析还在计算中，请继续等候！"%(sub_anaylsis_main_id)
                elif isinstance(id['sub_anaylsis_id'], list):
                    for m in id['sub_anaylsis_id']:
                        sub_anaylsis_main_id = str(m['id'])
                        result = collection_status.find_one({"table_id": ObjectId(sub_anaylsis_main_id)})
                        if result and result['status'] != 'start':
                            anaysis_num.append(result['status'])
                        else:
                            print "sg_status中没有找到%s对应的表，该分析还在计算中，请继续等候！" % (sub_anaylsis_main_id)
            else:
                no_table_analysis_num.append(str(id['submit_location']))
        print len(all_results)
        print len(no_table_analysis_num)
        print len(anaysis_num)
        if len(all_results) - len(no_table_analysis_num) == len(anaysis_num):
            data = {
                "status": "end",
                "percent": str(len(all_results)) + "/" + str(len(all_results))
            }
            try:
                collection_pipe.update({"_id": ObjectId(main_table_id)}, {'$set': data}, upsert=False)
            except Exception:
                print "sg_pipe_batch状态更新失败，请检查！"
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
                collection_pipe.update({"_id": ObjectId(main_table_id)}, {'$set': data}, upsert=False)
            except Exception:
                print "sg_pipe_batch进度条更新失败，请检查！"
            m = False
        return m

    def find_group_name(self, group_id):
        """
        根据group_id去找grup_name
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