# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.meta import Meta
from bson import ObjectId


class MantelTest(MetaController):
    """
    mantel_test检验接口
    """
    MATRIX = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord', 'euclidean',
              'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn', 'pearson', 'soergel', 'spearman_approx',
              'specprof', 'unifrac', 'unweighted_unifrac', 'weighted_normalized_unifrac', 'weighted_unifrac']

    MATRIXFACTOR = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming',
                    'binary_jaccard', 'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson',
                    'binary_sorensen_dice', 'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra',
                    'chisq', 'chord', 'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
                    'pearson', 'soergel', 'spearman_approx', 'specprof']

    def __init__(self):
        super(MantelTest, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', "group_id", "env_id", "otu_method", "env_method", "env_labs"]

        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)

        if data.otu_method not in self.MATRIX:
            info = {'success': False, 'info': '选择计算OTU表矩阵的方法不正确%s!' % data.otu_method}
            return json.dumps(info)
        if data.env_method not in self.MATRIXFACTOR:
            info = {'success': False, 'info': '选择计算环境因子表样本距离矩阵s的方法不正确%s!' % data.otu_method}
            return json.dumps(info)

        # group_detail = group_detail_sort(data.group_detail)
        print(data.group_detail)

        task_name = 'meta.report.mantel_test'
        task_type = 'workflow'
        meta = Meta()

        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])

        params_json = {
            "otu_id": data.otu_id,
            "level_id": data.level_id,
            "submit_location": data.submit_location,
            "task_type": data.task_type,
            "group_detail": group_detail_sort(data.group_detail),
            "group_id": data.group_id,
            "env_id": data.env_id,
            "otu_method": data.otu_method,
            "env_method": data.env_method,
            "env_labs": data.env_labs
                        }
        if hasattr(data, "units"):
            params_json["units"] = data.units
            main_table_name = "PartialMantelTest_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        else:
            main_table_name = 'MantelTest_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ("env_id", ObjectId(data.env_id)),
            ('status', 'start'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("level_id", int(data.level_id)),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = meta.insert_main_table('sg_species_mantel_check', mongo_data)
        update_info = {str(main_table_id): 'sg_species_mantel_check'}

        options = {
                "otu_file": data.otu_id,
                "env_file": data.env_id,
                'update_info': json.dumps(update_info),
                "level": data.level_id,
                "group_detail": data.group_detail,
                "mantel_id": str(main_table_id)
                    }

        del params_json["level_id"]
        del params_json["group_detail"]
        options.update(params_json)
        to_file = ['meta.export_otu_table_by_detail(otu_file)', "env.export_float_env(env_file)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(MantelTest, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
                }}
        print("lllllllllmmmmmmmmmllllllll")
        print(task_info)
        return json.dumps(task_info)

        # print self.returnInfo
        # return_info = json.loads(self.returnInfo)
        # if not return_info["success"]:
        #     return_info["info"] = "程序运行出错，请检查输入的环境因子是否存在分类型环境因子"
        # return json.dumps(return_info)
