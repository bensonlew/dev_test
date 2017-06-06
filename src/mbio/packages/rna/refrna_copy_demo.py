# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modifiy: 2017.06.05
import json
import gevent
import datetime
from bson import ObjectId
from gevent import Greenlet
from gevent.monkey import patch_all
from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort


class CopyMongo(object):
    """"""
    def __init__(self, old_task_id, new_task_id, new_project_sn, new_member_id, db='tsanger_ref_rna'):
        self.db = Config().mongo_client[db]
        self._old_task_id = old_task_id
        self._new_task_id = new_task_id
        self._new_project_sn = new_project_sn
        self._new_member_id = new_member_id
        self.specimen_id_dict = {}
        self.group_id_dict = {}
        self.stat_id_dict = {}
        self.all_greenlets = []
        self._exchange_dict = {  # 根据特定字段名称，进行特定的ID新旧替换
            'specimen_id': self.specimen_id_dict,
            'group_id': self.group_id_dict
            }

    def run(self):
        """
        运行执行复制特定ID数据的操作，如果有新的分析请参照下面的写法添加代码，不同分析表结构不同，所有需要手动添加。
        """
        patch_all()
        self.copy_member_id()
        self.copy_sg_specimen()
        self.copy_sg_specimen_group()
        # self.copy_collection_with_change('sg_specimen_graphic', change_positions=['specimen_id', ])
        # self.copy_collection_with_change('sg_specimen_group_compare')
        # self.copy_collection_with_change('sg_specimen_mapping')
        greenlet = Greenlet(self.annotation_blast)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_nr)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_swissprot)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_pfam)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_cog)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_go)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_kegg)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_query)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        greenlet = Greenlet(self.annotation_stat)
        greenlet.start()
        self.all_greenlets.append(greenlet)
        gevent.joinall(self.all_greenlets)
        gevent.joinall(self.all_greenlets)
        import socket
        reload(socket)

    def annotation_blast(self):
        annotation_blast_dict = self.copy_collection_with_change('sg_annotation_blast', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_blast_detail', 'blast_id', annotation_blast_dict, join=False)

    def annotation_nr(self):
        annotation_nr_dict = self.copy_collection_with_change('sg_annotation_nr', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_nr_pie', 'nr_id', annotation_nr_dict, join=False)

    def annotation_swissprot(self):
        annotation_swissprot_dict = self.copy_collection_with_change('sg_annotation_swissprot', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_swissprot_pie', 'swissprot_id', join=False)

    def annotation_pfam(self):
        annotation_pfam_dict = self.copy_collection_with_change('sg_annotation_pfam', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_pfam_bar', 'pfam_id', annotation_pfam_dict, join=False)
        self.copy_main_details('sg_annotation_pfam_detail', 'pfam_id', annotation_pfam_dict, join=False)

    def annotation_cog(self):
        annotation_cog_dict = self.copy_collection_with_change('sg_annotation_cog', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_cog_detail', 'cog_id', annotation_cog_dict, join=False)
        self.copy_main_details('sg_annotation_cog_table', 'cog_id', annotation_cog_dict, join=False)

    def annotation_go(self):
        annotation_go_dict = self.copy_collection_with_change('sg_annotation_go', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_go_detail', 'go_id', annotation_go_dict, join=False)
        self.copy_main_details('sg_annotation_go_level', 'go_id', annotation_go_dict, join=False)
        self.copy_main_details('sg_annotation_go_list', 'go_id', annotation_go_dict, join=False)

    def annotation_kegg(self):
        annotation_kegg_dict = self.copy_collection_with_change('sg_annotation_kegg', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_kegg_categories', 'kegg_id', annotation_kegg_dict, join=False)
        self.copy_main_details('sg_annotation_kegg_level', 'kegg_id', annotation_kegg_dict, join=False)
        self.copy_main_details('sg_annotation_kegg_table', 'kegg_id', annotation_kegg_dict, join=False)

    def annotation_query(self):
        annotation_query_dict = self.copy_collection_with_change('sg_annotation_query', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_query_detail', 'query_id', annotation_query_dict, join=False)

    def annotation_stat(self):
        annotation_stat_dict = self.copy_collection_with_change('sg_annotation_stat', change_positions=[], update_sg_status=True)
        self.copy_main_details('sg_annotation_stat_detail', 'stat_id', annotation_stat_dict, join=False)

    def copy_member_id(self):
        """
        复制sg_task的数据
        """
        coll = self.db["sg_task"]
        find = coll.find_one({'task_id': self._old_task_id})
        if not find:
            raise Exception('运行错误：找不到demo任务相关信息')
        find['task_id'] = self._new_task_id
        find['member_id'] = self._new_member_id
        find.pop('_id')
        find['project_sn'] = self._new_project_sn
        self.db["sg_task"].insert_one(find)

    def copy_sg_specimen(self):
        """
        复制样本表
        """
        finds = self.db["sg_specimen"].find({"task_id": self._old_task_id})
        news = []
        old_specimen_ids = []
        for i in finds:
            old_specimen_ids.append(str(i.pop('_id')))
            i['task_id'] = self._new_task_id
            i['project_sn'] = self._new_project_sn
            news.append(i)
        if news:
            result = self.db["sg_specimen"].insert_many(news)
        else:
            raise Exception('没有任何样本信息，请核对任务结果是否完整')
        self.specimen_id_dict = dict(zip(old_specimen_ids, [str(one) for one in result.inserted_ids]))
        self._exchange_dict['specimen_id'] = self.specimen_id_dict
        return self.specimen_id_dict

    def copy_sg_specimen_group(self):
        """
        复制sg_specimen_group表
        """
        finds = self.db["sg_specimen_group"].find({"task_id": self._old_task_id})
        news = []
        old_group_ids = []
        for i in finds:
            i['task_id'] = self._new_task_id
            i['project_sn'] = self._new_project_sn
            old_group_ids.append(str(i.pop('_id')))
            for one in i['specimen_names']:
                for sp in one.copy():
                    one[self.specimen_id_dict[sp]] = one[sp]
                    one.pop(sp)
            news.append(i)
        if news:
            result = self.db.sg_specimen_group.insert_many(news)
            self.group_id_dict = dict(zip(old_group_ids, [str(one) for one in result.inserted_ids]))
        else:
            self.group_id_dict = {}
        self.group_id_dict['all'] = 'all'  # 特殊ID
        self.group_id_dict[None] = None  # 特殊ID
        self.group_id_dict[''] = None  # 特殊ID
        self._exchange_dict['group_id'] = self.group_id_dict
        return self.group_id_dict

    def _copy_main_details(self, collection, main_field, change_dict, others_position=[]):
        """
        公共模块，一般用于更新detail表，根据提供的主表id字段名，和主表新旧ID字典，进行查找，再复制替换，others_position用于更新主表ID之外其他需要更新的ID
        params collection: detail表名称
        params main_field: 主表字段名称
        params change_dict: 主表新旧替换字典，一般来源于 copy_collection_with_change 的返回字典
        params others_position: detail表中除了主表还需要更新的字段，
            只能是 specimen_id,group_id
        """
        time_start = datetime.datetime.now()
        coll = self.db[collection]
        for old, new in change_dict.items():
            finds = coll.find({main_field: ObjectId(old)})
            news = []
            for i in finds:
                i.pop('_id')
                i[main_field] = ObjectId(new)
                for position in others_position:
                    i[position] = self.exchange_ObjectId(position, i[position])
                news.append(i)
            if news:
                coll.insert_many(news)
            else:
                print 'WARNING: 主表:{}没有detail表信息，请注意数据合理性,collection:{}'.format(old, collection)
        time_end = datetime.datetime.now()
        run_time = (time_end - time_start).seconds
        print "{}复制运行时间: {}s".format(collection, run_time)

    def copy_main_details(self, collection, main_field, change_dict, others_position=[], join=True):
        greenlet = Greenlet(self._copy_main_details, collection, main_field, change_dict, others_position)
        greenlet.start()
        if join is True:
            greenlet.join()
            return greenlet.value
        self.all_greenlets.append(greenlet)
        return greenlet

    def copy_collection_with_change(self, collection, change_positions=[], update_sg_status=False, targetcoll=None):
        """
        公共模块，一般用于导入主表数据，依靠task_id进行查询，修改change_positions提供的字段，相应修改ID为新的，同时更新params中的数据ID
        params collection: 主表名称
        params change_positions: 需要替换的ID,可用为specimen_id,group_id...
        params update_sg_status: 更新sg_status表
        params targetcoll: 更新到特定集合， 默认与collection参数相同
        """
        coll = self.db[collection]
        if targetcoll:
            targetcoll = self.db[targetcoll]
        else:
            targetcoll = self.db[collection]
        finds = coll.find({'task_id': self._old_task_id})
        news = []
        olds = []
        for i in finds:
            i['task_id'] = self._new_task_id
            if 'project_sn' in i:
                i['project_sn'] = self._new_project_sn
            olds.append(str(i.pop('_id')))
            for position in change_positions:
                if position in i:
                    print position, i[position]
                    i[position] = self.exchange_ObjectId(position, i[position])
            if 'params' in i:
                i['params'] = self.params_exchange(i['params'])
            news.append(i)
        if news:
            result = targetcoll.insert_many(news)
            if update_sg_status:
                self.insert_new_status(collection, news, result.inserted_ids)
            return dict(zip(olds, [str(one) for one in result.inserted_ids]))
        else:
            return {}

    def exchange_ObjectId(self, key, thisObjectId):
        """
        用于替换id，key是该ID的字段名，thisObjectId是旧的ID(ObjectId类型)
        """
        if isinstance(thisObjectId, ObjectId):
            return ObjectId(self._exchange_dict[key][str(thisObjectId)])
        else:
            return self._exchange_dict[key][thisObjectId]  # 不是ObjectId时直接返回也是字符串

    def params_exchange(self, params_str):
        """
        专门用于params的数据ID替换
        """
        try:
            params = json.loads(params_str)
        except Exception:
            print("WRANNING：非json格式的params：{}".format(params_str))
            return params_str
        if not params:
            return None
        if 'group_detail' in params:
            for one_group in params['group_detail']:
                params['group_detail'][one_group] = [self.specimen_id_dict[one_sp] for one_sp in params['group_detail'][one_group]]
            params['group_detail'] = group_detail_sort(params['group_detail'])
            if 'second_group_detail' in params:
                if params['second_group_detail']:
                    for one_group in params['second_group_detail']:
                        params['second_group_detail'][one_group] = [self.specimen_id_dict[one_sp] for one_sp in params['second_group_detail'][one_group]]
                    params['second_group_detail'] = group_detail_sort(params['second_group_detail'])
                    if 'second_group_id' in params:
                        params['second_group_id'] = self.group_id_dict[params['second_group_id']]
        if 'group_id' in params:
            params['group_id'] = self.group_id_dict[params['group_id']]
        return json.dumps(params, sort_keys=True, separators=(',', ':'))

    def insert_new_status(self, collection, main_docs, ids):
        """
        导入mongo表sg_status数据信息
        """
        coll = self.db["sg_status"]
        news = []
        for index, doc in enumerate(main_docs):
            try:
                submit_location = json.loads(doc['params'])['submit_location']
            except Exception:
                print("WARNING: params参数没有submit_location字段, Doc:{}".format(doc))
                submit_location = None
            status = {
                "status": doc['status'],
                "table_id": ids[index],
                "time": doc['created_ts'],
                "task_id": self._new_task_id,
                "params": doc['params'],
                "table_name": doc['name'],
                "submit_location": submit_location,
                "type_name": collection,
                "is_new": "new",
                "desc": doc['desc'] if 'desc' in doc else None
            }
            news.append(status.copy())
        if news:
            coll.insert_many(news)


if __name__ == '__main__':
    copy_task = CopyMongo('tsg_2000', 'tsg_2000_new', '20000000_new', 'zengjing_test')
    copy_task.run()
