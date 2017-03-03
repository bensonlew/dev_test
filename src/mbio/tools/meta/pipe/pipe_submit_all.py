## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mainapp.libs.signature import CreateSignature
from biocluster.config import Config
from mainapp.libs.param_pack import *
import urllib2
import urllib
import sys
import os
import json
from bson.objectid import ObjectId
import gevent
import time
import re


class PipeSubmitAllAgent(Agent):
    """
    用于submit子分析的接口,针对的是所有的分析，后面还会有医口与农口
    version v1.0
    author: hongdongxuan
    last_modify: 2017.02.20
    """
    def __init__(self, parent):
        super(PipeSubmitAllAgent, self).__init__(parent)
        options = [
            {"name": "data", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("piple_submit")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.piple_submit.start()
        self.step.update()

    def stepfinish(self):
        self.step.piple_submit.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("data"):
            raise OptionError("必须输入接口传进来的data值")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '50G'

    def end(self):

        super(PipeSubmitAllAgent, self).end()


class PipeSubmitAllTool(Tool):

    def __init__(self, config):
        super(PipeSubmitAllTool, self).__init__(config)
        self._version = '1.0.1'
        self._config = Config()
        self._client = self._config.mongo_client
        self.db = self._client['tsanger']


    def run_webapitest(self):
        # collection = self.db["sg_network"]
        # result = collection.find_one({"_id": ObjectId("589d7179a4e1af2fffc579d2")})
        analysis_table = {"randomforest_analyse": "sg_randomforest", "sixteens_prediction": "sg_16s",
                          "species_lefse_analyse": "sg_species_difference_lefse", "alpha_rarefaction_curve":"sg_alpha_rarefaction_curve",
                          "otunetwork_analyse": "sg_network", "roc_analyse": "sg_roc",
                          "alpha_diversity_index":"sg_alpha_diversity","alpha_ttest":"sg_alpha_ttest",
                          "beta_multi_analysis_plsda": "sg_beta_multi_analysis","beta_sample_distance": "sg_newick_tree",
                          "beta_multi_analysis_pearson_correlation":"sg_species_env_correlation","beta_multi_analysis_rda_cca": "sg_beta_multi_analysis", "hc_heatmap": "sg_hc_heatmap",
                          "beta_multi_analysis_anosim": "sg_beta_multi_anosim", "species_difference_multiple": "sg_species_difference_check",
                          "beta_multi_analysis_results": "sg_species_mantel_check", "beta_multi_analysis_pcoa": "sg_beta_multi_analysis",
                          "beta_multi_analysis_dbrda": "sg_beta_multi_analysis", "species_difference_two_group": "sg_species_difference_check",
                          "beta_multi_analysis_nmds":"sg_beta_multi_analysis","otu_group_analyse":"sg_otu",
                          "otu_venn":"sg_otu_venn","beta_multi_analysis_pca": "sg_beta_multi_analysis",
                          "corrnetwork_analyse": "sg_corr_network"}
        data = self.option('data')
        print "打印出data"
        print data
        data = json.loads(data)
        print "----"
        print data
        print type(data)
        print data.keys()
        # env_id = data['env_id']
        # env_labs = data['env_labs']
        group_infos = data['group_info']
        print type(group_infos)
        print "打印出group_infos"
        print group_infos
        group_infos = json.loads(group_infos)
        print "test group_infos"
        print type(group_infos)
        print group_infos
        level_ids = []
        level = str(data['level_id']).strip().split(",")
        for m in level:
            level_ids.append(m)
        print level_ids
        sub_analysis = data['sub_analysis']
        print sub_analysis
        print type(sub_analysis)
        sub_analysis = json.loads(sub_analysis)
        print type(sub_analysis)
        print sub_analysis
        otu_id = data['otu_id']
        client = data['client']
        method = "post"
        if client == "client01":
            base_url = 'http://bcl.i-sanger.com'
        elif client == "client03":
            base_url = "http://10.101.203.193:9100"
            #base_url = "http://192.168.12.102:9090"
        else:
            print "***client必须是client01或者client03***"
        print "client= %s, base_url= %s"%(client, base_url)
        #计算抽平
        collection_sg_otu = self.db["sg_otu"]
        result = collection_sg_otu.find_one({"_id": ObjectId(str(data['otu_id']))})
        from_id = result['from_id']
        my_param = dict()
        my_param["group_id"] = data['group_id']
        my_param['otu_id'] = from_id
        my_param["submit_location"] = "otu_statistic"
        my_param["size"] = data['size']
        my_param["filter_json"] = json.dumps(data['filter_json'])
        my_param["group_detail"] = group_detail_sort(data['group_detail'])
        my_param["task_type"] = "reportTask"
        params_otu = param_pack(my_param)
        if json.dumps(result['params']) == json.dumps(params_otu):
            otu_id = data['otu_id']
        else:
            params = {"otu_id": str(otu_id), "group_id": data['group_id'], "group_detail": data['group_detail'], "submit_location": "otu_statistic",
                  "filter_json": data['filter_json'], "task_type": "reportTask", "size": data['size'], "level_id": "9"}
            api_statistic = "meta/otu_subsample"
            results_statistic = self.run_controllers(api=api_statistic, client=client, base_url=base_url, params=params, method=method)
            results_statistic = json.loads(results_statistic)
            # print results_statistic
            otu_id = results_statistic['sub_anaylsis_id']['id']  #后面用抽平后的otu_id
        print "打印抽平后的otu_id"
        print otu_id
        list2 = [] #用于存储分类水平与分组方案的所有的组合s
        for level in level_ids:
            for group in group_infos:
                if 'env_id' in data.keys() and 'env_labs' in data.keys():
                    m = {"otu_id": str(otu_id), "level_id": str(level), "group_id": group['group_id'], "group_detail": json.dumps(group['group_detail']),
                     "env_id": str(data['env_id']), "env_labs": str(data['env_labs'])}
                else:
                    m = {"otu_id": str(otu_id), "level_id": str(level), "group_id": group['group_id'],
                         "group_detail": json.dumps(group['group_detail'])}
                list2.append(m)
        print "打印出所有的组合情况"
        print list2
        all_results = []  # 存储所有子分析的ids
        err_results = []  # 用于存储投递失败的分析信息
        #定义多样性指数与指数间差异分析的接口投递
        anaylsis_names = []
        alpha_diversity_index_data = {}
        alpha_ttest_data = {}
        species_lefse_analyse_data = {}
        sixteens_prediction_data = {}
        alpha_rarefaction_curve_data = {}
        otunetwork_analyse_data = {}
        roc_analyse_data = {}
        randomforest_analyse_data = {}
        for key in sub_analysis:
            anaylsis_names.append(str(key))
            if key == "alpha_diversity_index":
                alpha_diversity_index_data = sub_analysis[key]
            elif key == "alpha_ttest":
                alpha_ttest_data = sub_analysis[key]
            elif key == "sixteens_prediction":
                sixteens_prediction_data = sub_analysis[key]
            elif key == "species_lefse_analyse":
                species_lefse_analyse_data = sub_analysis[key]
            elif key == "alpha_rarefaction_curve":
                alpha_rarefaction_curve_data = sub_analysis[key]
            elif key == "otunetwork_analyse":
                otunetwork_analyse_data = sub_analysis[key]
            elif key == "roc_analyse":
                roc_analyse_data = sub_analysis[key]
            elif key == "randomforest_analyse":
                randomforest_analyse_data = sub_analysis[key]
            else:
                pass
        print "\n"
        print anaylsis_names
        print len(anaylsis_names)
        print alpha_diversity_index_data
        print alpha_ttest_data
        print "\n"

        if "randomforest_analyse" in anaylsis_names:
            for n in list2:
                randomforest_analyse_data['otu_id'] = n['otu_id']
                randomforest_analyse_data['level_id'] = n['level_id']
                randomforest_analyse_data['group_id'] = n['group_id']
                randomforest_analyse_data['group_detail'] = n['group_detail']
                result_randomforest_analyse = self.run_controllers(api="/".join(str(randomforest_analyse_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=randomforest_analyse_data, method=method)
                result_randomforest_analyse = json.loads(result_randomforest_analyse)
                all_results.append(result_randomforest_analyse)
        else:
            pass

        if "sixteens_prediction" in anaylsis_names:
            for n in list2:
                sixteens_prediction_data['otu_id'] = n['otu_id']
                sixteens_prediction_data['level_id'] = n['level_id']
                sixteens_prediction_data['group_id'] = n['group_id']
                sixteens_prediction_data['group_detail'] = n['group_detail']
                result_sixteens_prediction = self.run_controllers(api="/".join(str(sixteens_prediction_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=sixteens_prediction_data, method=method)
                result_sixteens_prediction = json.loads(result_sixteens_prediction)
                all_results.append(result_sixteens_prediction)
        else:
            pass
        if "species_lefse_analyse" in anaylsis_names:
            for n in list2:
                species_lefse_analyse_data['otu_id'] = n['otu_id']
                species_lefse_analyse_data['level_id'] = n['level_id']
                species_lefse_analyse_data['group_id'] = n['group_id']
                species_lefse_analyse_data['group_detail'] = n['group_detail']
                if len(self.params_check(species_lefse_analyse_data,
                                         analysis_table[species_lefse_analyse_data['submit_location']],
                                         species_lefse_analyse_data['submit_location'])) == 0:
                    print "test01"
                    result_species_lefse_analyse = self.run_controllers(api="/".join(str(species_lefse_analyse_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=species_lefse_analyse_data, method=method)
                    result_species_lefse_analyse = json.loads(result_species_lefse_analyse)
                    all_results.append(result_species_lefse_analyse)
                else:
                    print "test02"
                    all_results.append(self.params_check(species_lefse_analyse_data,
                                         analysis_table[species_lefse_analyse_data['submit_location']],
                                         species_lefse_analyse_data['submit_location']))
        else:
            pass

        if "alpha_rarefaction_curve" in anaylsis_names:
            for n in list2:
                alpha_rarefaction_curve_data['otu_id'] = n['otu_id']
                alpha_rarefaction_curve_data['level_id'] = n['level_id']
                alpha_rarefaction_curve_data['group_id'] = n['group_id']
                alpha_rarefaction_curve_data['group_detail'] = n['group_detail']
                result_alpha_rarefaction_curve = self.run_controllers(api="/".join(str(alpha_rarefaction_curve_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=alpha_rarefaction_curve_data, method=method)
                result_alpha_rarefaction_curve = json.loads(result_alpha_rarefaction_curve)
                all_results.append(result_alpha_rarefaction_curve)
        else:
            pass

        if "otunetwork_analyse" in anaylsis_names:
            for n in list2:
                otunetwork_analyse_data['otu_id'] = n['otu_id']
                otunetwork_analyse_data['level_id'] = n['level_id']
                otunetwork_analyse_data['group_id'] = n['group_id']
                otunetwork_analyse_data['group_detail'] = n['group_detail']
                if len(self.params_check(otunetwork_analyse_data, analysis_table[otunetwork_analyse_data['submit_location']],
                                         otunetwork_analyse_data['submit_location'])) == 0:
                    result_otunetwork_analyse = self.run_controllers(api="/".join(str(otunetwork_analyse_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=otunetwork_analyse_data, method=method)
                    result_otunetwork_analyse = json.loads(result_otunetwork_analyse)
                    all_results.append(result_otunetwork_analyse)
                else:
                    all_results.append(self.params_check(otunetwork_analyse_data, analysis_table[otunetwork_analyse_data['submit_location']],
                                         otunetwork_analyse_data['submit_location']))
        else:
            pass

        if "roc_analyse" in anaylsis_names:
            for n in list2:
                roc_analyse_data['otu_id'] = n['otu_id']
                roc_analyse_data['level_id'] = n['level_id']
                roc_analyse_data['group_id'] = n['group_id']
                roc_analyse_data['group_detail'] = n['group_detail']
                result_roc_analyse = self.run_controllers(api="/".join(str(roc_analyse_data['api']).strip().split("|")),
                                                                     client=client, base_url=base_url,
                                                                     params=roc_analyse_data, method=method)
                result_roc_analyse = json.loads(result_roc_analyse)
                all_results.append(result_roc_analyse)
        else:
            pass

        if "alpha_ttest" in anaylsis_names and "alpha_diversity_index" in anaylsis_names:
            for n in list2:
                alpha_diversity_index_data['otu_id'] = n['otu_id']
                alpha_diversity_index_data['level_id'] = n['level_id']
                alpha_diversity_index_data['group_id'] = n['group_id']
                alpha_diversity_index_data['group_detail'] = n['group_detail']
                # m = "/".join(str(alpha_diversity_index_data['api']).strip().split("|"))
                # print m
                results_alpha_diversity_index = self.run_controllers(api="/".join(str(alpha_diversity_index_data['api']).strip().split("|")),
                                                                        client=client, base_url=base_url,
                                                                     params=alpha_diversity_index_data, method=method)
                results_alpha_diversity_index = json.loads(results_alpha_diversity_index)
                all_results.append(results_alpha_diversity_index)
                alpha_diversity_id = results_alpha_diversity_index['sub_anaylsis_id']['id']
                alpha_ttest_data['otu_id'] = n['otu_id']
                alpha_ttest_data['level_id'] = n['level_id']
                alpha_ttest_data['group_id'] = n['group_id']
                alpha_ttest_data['alpha_diversity_id'] = str(alpha_diversity_id)
                alpha_ttest_data['group_detail'] = n['group_detail']
                # alpha_ttest_data['group_detail'] = json.loads(n['group_detail'])
                results_alpha_ttest = self.run_controllers(api="/".join(str(alpha_ttest_data['api']).strip().split("|")),
                    client=client, base_url=base_url,
                    params=alpha_ttest_data, method=method)
                results_alpha_ttest = json.loads(results_alpha_ttest)
                all_results.append(results_alpha_ttest)
        elif "alpha_diversity_index" in anaylsis_names and "alpha_ttest" not in anaylsis_names:
            for n in list2:
                alpha_diversity_index_data['otu_id'] = n['otu_id']
                alpha_diversity_index_data['level_id'] = n['level_id']
                alpha_diversity_index_data['group_id'] = n['group_id']
                alpha_diversity_index_data['group_detail'] = n['group_detail']
                results_alpha_diversity_index = self.run_controllers(
                    api="/".join(str(alpha_diversity_index_data['api']).strip().split("|")),
                    client=client, base_url=base_url,
                    params=alpha_diversity_index_data, method=method)
                results_alpha_diversity_index = json.loads(results_alpha_diversity_index)
                all_results.append(results_alpha_diversity_index)
        else:
            pass
        #删除子分析字典中alpha_diversity_index，alpha_ttest两个分析,重构sub_analysis数组
        if "alpha_diversity_index" in anaylsis_names:
            del sub_analysis['alpha_diversity_index']
        else:
            pass
        if "alpha_ttest" in anaylsis_names:
            del sub_analysis['alpha_ttest']
        else:
            pass
        if "sixteens_prediction" in anaylsis_names:
            del sub_analysis['sixteens_prediction']
        else:
            pass
        if "species_lefse_analyse" in anaylsis_names:
            del sub_analysis['species_lefse_analyse']
        else:
            pass
        if "alpha_rarefaction_curve" in anaylsis_names:
            del sub_analysis['alpha_rarefaction_curve']
        else:
            pass
        if "otunetwork_analyse" in anaylsis_names:
            del sub_analysis['otunetwork_analyse']
        else:
            pass
        if "roc_analyse" in anaylsis_names:
            del sub_analysis['roc_analyse']
        else:
            pass
        if "randomforest_analyse" in anaylsis_names:
            del sub_analysis['randomforest_analyse']
        else:
            pass
        print "打印删除子分析字典中alpha_diversity_index，alpha_ttest,以及投递的多个分析,重构后的sub_analysis"
        print sub_analysis
        print len(sub_analysis)
        #定义没有依赖的分析的通用接口投递
        for info in list2:
            print "打印出info"
            print info
            print info.keys()
            for anaylsis in sub_analysis:
                # print "打印出sub_analysis"
                # print sub_analysis[anaylsis]
                sub_analysis[anaylsis]['otu_id'] = info['otu_id']
                sub_analysis[anaylsis]['level_id'] = info['level_id']
                sub_analysis[anaylsis]['group_id'] = info['group_id']
                sub_analysis[anaylsis]['group_detail'] = info['group_detail']
                print "答应group_detail类型"
                print type(info['group_detail'])
                if anaylsis in ['beta_multi_analysis_pca', 'beta_multi_analysis_rda_cca', 'beta_multi_analysis_dbrda',
                                'beta_multi_analysis_pearson_correlation', 'beta_multi_analysis_results'] \
                        and 'env_id' in info.keys() and 'env_labs' in info.keys():
                    sub_analysis[anaylsis]['env_id'] = info['env_id']
                    sub_analysis[anaylsis]['env_labs'] = info['env_labs']
                else:
                    pass
            print "下面打印出来的是每个子分析sub_analysis添加了分组与分类水平的data值"
            print sub_analysis
            for key in sub_analysis:
                name = []
                data = []  # 用于存储子分析参数的具体数值
                submit_info = {}
                print sub_analysis[key]
                # params_data = sub_analysis[key]
                if len(self.params_check(sub_analysis[key], analysis_table[sub_analysis[key]['submit_location']], sub_analysis[key]['submit_location'])) == 0:
                    for key1 in sub_analysis[key]:
                        # print sub_analysis[key]
                        # print sub_analysis[key].keys()
                        # print sub_analysis[key]['api']
                        name.append(key1)
                        data.append(sub_analysis[key][key1])
                        submit_info = {"api": "/".join(str(sub_analysis[key]['api']).strip().split('|')),
                                   "submit_location": str(sub_analysis[key]['submit_location'])}
                    print "打印出每个分析对应的api，base_url，client，method"
                    print submit_info
                    name = ";".join(name)
                    print "打印出name值"
                    print name
                    data = ";".join(data)
                    print "打印出data值"
                    print data
                    api = submit_info['api']
                    method = "post"
                    print "***api:%s, client:%s, base_url:%s"%(api, client, base_url)
                    return_page = self.webapitest(method, api, name, data, client, base_url)
                    result = json.loads(return_page)
                    result = json.loads(result)
                    if result['success'] == True:
                        results = {"level_id": info['level_id'], "group_id": info['group_id'],
                           "sub_anaylsis_id": result['content']['ids'], "success": True, "submit_location": submit_info['submit_location']}
                        all_results.append(results)
                    elif 'content' in result.keys() and result['success'] == False:
                        results = {"level_id": info['level_id'], "group_id": info['group_id'],
                               "sub_anaylsis_id": result['content']['ids'], "success": False,
                               "submit_location": submit_info['submit_location'], "info": result['info']}
                        all_results.append(results)
                    elif 'content' not in result.keys() and result['success'] == False:
                        results = {"level_id": info['level_id'], "group_id": info['group_id'],
                               "info": result['info'], "success": False,
                               "submit_location": submit_info['submit_location']}
                        all_results.append(results)
                    else:
                        print "结果还有额外的情况出现！"
                        pass
                else:
                    all_results.append(self.params_check(sub_analysis[key], analysis_table[sub_analysis[key]['submit_location']], sub_analysis[key]['submit_location']))
        print "打印出所有子分析的ids"
        print all_results
        print len(all_results)
        output_table = os.path.join(self.work_dir, "ids.txt")
        with open(output_table, "w") as w:
            w.write(str(all_results))


    def params_check(self, ever_analysis_params, analysis_table, submit_location):
        if not isinstance(ever_analysis_params, dict):
            success.append("传入的params不是一个字典")
        if submit_location in ["beta_multi_analysis_rda_cca", "beta_multi_analysis_pca", "beta_multi_analysis_pcoa",
                               "beta_multi_analysis_nmds", "beta_multi_analysis_plsda", "beta_multi_analysis_dbrda", "otunetwork_analyse"]:
            my_param = dict()
            for key in ever_analysis_params:
                if key == "level_id":
                    my_param[key] = int(ever_analysis_params[key])
                elif key == "group_detail":
                    my_param[key] = group_detail_sort(ever_analysis_params[key])
                elif key == "api":
                    pass
                else:
                    my_param[key] = ever_analysis_params[key]
            new_params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        elif submit_location in ["species_lefse_analyse"]:
            print "yes----------"
            my_param = dict()
            for key in ever_analysis_params:
                if key == "group_detail":
                    my_param[key] = group_detail_sort(ever_analysis_params[key])
                elif key == "second_group_id" and ever_analysis_params['second_group_id']:
                    my_param[key] = group_detail_sort(ever_analysis_params[key])
                elif key == "second_group_id" and not ever_analysis_params['second_group_id']:
                    my_param[key] = ever_analysis_params[key]
                elif key == "strict" or key == "start_level" or key == "end_level":
                    my_param[key] = int(ever_analysis_params[key])
                elif key == "api":
                    pass
                elif key == "level_id":
                    pass
                else:
                    my_param[key] = ever_analysis_params[key]

                if key == "lda_filter" and re.search(r'\.0$', ever_analysis_params['lda_filter']):
                    my_param[key] = int(float(ever_analysis_params[key]))
                elif key == "lda_filter" and re.search(r'\.0$', ever_analysis_params['lda_filter']):
                    my_param[key] = float(ever_analysis_params[key])
                elif key == "lda_filter" and not re.search(r'\.0$', ever_analysis_params['lda_filter']) and not \
                        re.search(r'\.0$', ever_analysis_params['lda_filter']):
                    my_param[key] = int(ever_analysis_params[key])
            new_params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        else:
            my_param = dict()
            for key in ever_analysis_params:
                my_param[key] = ever_analysis_params[key]
            new_params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        print new_params
        print json.dumps(new_params)
        print "_--"
        collection_sg_otu = self.db["sg_otu"]
        result = collection_sg_otu.find_one({"_id": ObjectId(str(ever_analysis_params['otu_id']))})
        task_id = result['task_id']
        collections = self.db[analysis_table]
        result1 = collections.find({"task_id": task_id})
        # print result1
        results = {}
        analysis_result = []
        for datas in result1:
            # print type(datas)
            # print datas['params']
            # print type(datas['params'])
            # print ever_analysis_params
            # print type(ever_analysis_params)
            # print datas['params']
            # print type(datas['params'])
            # print type(json.dumps(datas['params']))
            print json.dumps(datas['params'])

            if json.dumps(datas['params']) == json.dumps(new_params):
                print "yes"
                print json.dumps(datas['params'])
                if datas['status'] != "end":
                    results = {"level_id": ever_analysis_params['level_id'],
                                   "group_id": ever_analysis_params['group_id'],
                                   "sub_anaylsis_id": {"id": str(datas['_id']), "name": datas['name']}, "success": False,
                                   "submit_location": submit_location}
                elif datas['status'] == "end":
                    results = {"level_id": ever_analysis_params['level_id'],
                                   "group_id": ever_analysis_params['group_id'],
                                   "sub_anaylsis_id": {"id": str(datas['_id']), "name": datas['name']}, "success": True,
                                   "submit_location": submit_location}

        print "打印出不需要计算的分析"
        print results
        return results

    def run_controllers(self, api, client, base_url, params, method, header=None):
        """
        用于重构每个子分析的输入参数，（参考sub_anaylsis中的参数）并进行投递计算,注输入的参数必须是字典格式
        params样例：{\"submit_location\": \"corrnetwork_analyse\", \"api\": \"meta/corr_network\",
        \"task_type\": \"reportTask\",\"lable\": \"0.03\", \"ratio_method\": \"pearson\", \"coefficient\":
        \"0.08\", \"abundance\": \"150\"}  注这里面要添加level_id与group_id
        :return:
        """
        if not isinstance(params, dict):
            success.append("传入的params不是一个字典")
        name = []
        data = []
        results = {}
        for key in params:
            name.append(key)
            data.append(params[key])
        name = ";".join(name)
        data = ";".join(data)
        return_page = self.webapitest(method, api, name, data, client, base_url)
        result = json.loads(return_page)
        result = json.loads(result)
        if result['success'] == True:
            results = {"level_id": params['level_id'], "group_id": params['group_id'],
                       "sub_anaylsis_id": result['content']['ids'], "success": True,
                       "submit_location": params['submit_location']}
        elif 'content' in result.keys() and result['success'] == False:
            results = {"level_id": params['level_id'], "group_id": params['group_id'],
                       "sub_anaylsis_id": result['content']['ids'], "success": False,
                       "submit_location": params['submit_location'], "info": result['info']}
        elif 'content' not in result.keys() and result['success'] == False:
            results = {"level_id": params['level_id'], "group_id": params['group_id'],
                       "info": result['info'], "success": False,
                       "submit_location": params['submit_location']}
        else:
            print "结果还有额外的情况出现！"
            pass
        # results = {"level_id": params['level_id'], "group_id": params['group_id'],
        #            "sub_anaylsis_id": result['content']['ids']}
        return json.dumps(results)



    def webapitest(self, method, api, name, data, client, base_url, header=None):
        """
        :param method: [get,post]
        :param api: the api address and url
        :param name: names for data, split by \";\"
        :param data: data or files for names, split by \";\"
        :param client: client name
        :param base_url: the base url of api, http://192.168.12.102:9010
        :param header: use header to submit signature info
        :return:
        """
        if data and not name:
            print("Error:must give the name option when the data is given!")
            sys.exit(1)
        if name and not data:
            print("Error:must give the data option when the name is given!")
            sys.exit(1)
        datas = {}
        if name:
            names_list = name.split(";")
            data_list = data.split(";")
            for index in range(len(names_list)):
                if index < len(data_list):
                    if os.path.isfile(data_list[index]):
                        with open(data_list[index], "r") as f:
                            content = f.readlines()
                            content = "".join(content)
                    else:
                        content = data_list[index]
                    datas[names_list[index]] = content
                else:
                    datas[names_list[index]] = ""
        httpHandler = urllib2.HTTPHandler(debuglevel=1)
        httpsHandler = urllib2.HTTPSHandler(debuglevel=1)
        opener = urllib2.build_opener(httpHandler, httpsHandler)

        urllib2.install_opener(opener)
        data = urllib.urlencode(datas)

        signature_obj = CreateSignature(client)
        signature = {
            "client": signature_obj.client,
            "nonce": signature_obj.nonce,
            "timestamp": signature_obj.timestamp,
            "signature": signature_obj.signature
        }

        signature = urllib.urlencode(signature)
        url = "%s/%s" % (base_url, api)
        if not header:
            if "?" in url:
                url = "%s&%s" % (url, signature)
            else:
                url = "%s?%s" % (url, signature)

        if method == "post":
            # print("post data to url %s ...\n\n" % url)
            # print("post data:\n%s\n" % data)
            request = urllib2.Request(url, data)
        else:
            if data:
                if "?" in url:
                    url = "%s&%s" % (url, data)
                else:
                    url = "%s?%s" % (url, data)
            else:
                url = "%s%s" % (base_url, api)
            print("get url %s ..." % url)
            request = urllib2.Request(url)

        if header:
            request.add_header('client', signature_obj.client)
            request.add_header('nonce', signature_obj.nonce)
            request.add_header('timestamp', signature_obj.timestamp)
            request.add_header('signature', signature_obj.signature)

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            print("%s \n" % e)
        else:
            the_page = response.read()
            print("Return page:\n%s" % the_page)
            print '\n'
            # print type(the_page)
        return json.dumps(the_page)

    # def set_output(self):
    #     """
    #     将结果文件link到output文件夹下面
    #     :return:
    #     """
    #     for root, dirs, files in os.walk(self.output_dir):
    #         for names in files:
    #             os.remove(os.path.join(root, names))
    #     self.logger.info("设置结果目录")
    #     results = os.listdir(self.work_dir + '/' + "corr_result/")
    #     for f in results:
    #         os.link(self.work_dir + '/' + "corr_result/" +f, self.output_dir + "/" +f)
    #     self.logger.info('设置文件夹路径成功')


    def run(self):
        super(PipeSubmitAllTool, self).run()
        self.run_webapitest()
        # self.set_output()
        self.end()
