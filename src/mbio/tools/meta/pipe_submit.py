# !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
# lastmodified = "shenghe"

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort, filter_json_sort, sub_group_detail_sort
from socket import error as SocketError
import urllib2
import urllib
import httplib
import json
from bson.objectid import ObjectId
import gevent
import time
import hashlib
from gevent.event import AsyncResult
import pymongo
import copy
import random
import traceback
from gevent import monkey
from biocluster.wpm.client import worker_client, wait


class PipeSubmitAgent(Agent):
    """
    用于submit子分析的接口,针对的是所有的分析，后面还会有医口与农口
    version v1.0
    author: hongdongxuan
    last_modify: 2017.03.09
    """

    def __init__(self, parent):
        super(PipeSubmitAgent, self).__init__(parent)
        options = [
            {"name": "data", "type": "string"},
            {"name": "pipe_id", "type": "string"},
            {"name": "task_id", "type": "string"}
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
        self._memory = '5G'

    def end(self):
        super(PipeSubmitAgent, self).end()


class PipeSubmitTool(Tool):

    def __init__(self, config):
        super(PipeSubmitTool, self).__init__(config)
        self._version = '1.0.1'
        self._config = Config()
        self._client = self._config.mongo_client
        self.db = self._client[self._config.MONGODB]
        self.max_instant_running = 18
        self.max_submit_running = 40
        self.count_submit_running = 0
        self.count_instant_running = 0

    def rely_error(self, ana, error_rely):
        self.logger.error('依赖对象运行错误: {}'.format(error_rely.api))
        if ana.instant:
            self.count_instant_running += 1
        else:
            self.count_submit_running += 1
        self.one_end(ana)
        # raise Exception('ERROR: 依赖对象运行错误: {}'.format(error_rely.api))

    def get_origin_task_id(self):
        split_id = self.sheet.id.split('_')
        split_id.pop()
        split_id.pop()
        self.task_id = '_'.join(split_id)
        return self.task_id

    def one_end(self, ana):
        self.count_ends += 1
        if ana.instant:
            self.count_instant_running -= 1
            self.logger.info("当前运行中的即时任务数为: {}".format(
                self.count_instant_running))
        else:
            self.count_submit_running -= 1
            self.logger.info("当前运行中的投递任务数为: {}".format(
                self.count_submit_running))
        self.logger.info("END COUNT: {}".format(self.count_ends))
        if self.count_ends == self.all_count:
            self.all_end.set()
        pass

    def get_params(self, config_name):
        """
        获取参数, 需要处理的参数请先特殊处理
        """
        config = self.analysis_params[config_name]
        params = {}
        for i in config['main']:
            params[i] = self.web_data[i]
        sub_params = self.web_data['sub_analysis'][config_name]
        for i in config['others']:
            if i == "second_group_detail" and sub_params[i]:
                params[i] = sub_group_detail_sort(sub_params[i])
            else:
                params[i] = sub_params[i]
        api = '/' + '/'.join(sub_params['api'].split('|'))
        instant = config['instant']
        collection_name = config['collection_name']
        return (api, instant, collection_name, params)

    def format_special_params(self):
        """
        预处理 参数， 如传入的filter_json和group_detail
        """
        pass

    def get_class(self, name):
        class_name = ''.join([i.capitalize() for i in name.split('_')])
        if class_name in globals():
            return globals()[class_name]
        else:
            raise Exception("没有找到相应的类模块， 请添加：{}".format(class_name))

    def get_otu_subsample_params(self, group_info):
        """
        """
        params = {
            'otu_id': self.web_data['otu_id'],
            'filter_json': filter_json_sort(self.web_data['filter_json']),
            'group_id': group_info['group_id'],
            'group_detail': group_detail_sort(group_info['group_detail']),
            'submit_location': 'otu_statistic',
            'task_type': 'reportTask',
            'size': self.web_data['size']
        }
        return '/meta/otu_subsample', True, 'sg_otu', params

    def signature(self):
        timestamp = str(int(time.time()))
        nonce = str(random.randint(1000, 10000))
        web_key = self.mysql_client_key
        sha1 = hashlib.sha1()
        key_list = [web_key, nonce, timestamp]
        key_list.sort()
        map(sha1.update, key_list)
        hashkey = sha1.hexdigest()
        signature = {
            "client": self.task_client,
            "nonce": nonce,
            "timestamp": timestamp,
            "signature": hashkey
        }
        return urllib.urlencode(signature)

    def run_webapitest(self):
        """
        进行参数判断后投递所有的接口，如果后面要添加新的分析，主要要添加以下几个地方的内容：
        1）analysis_table：analysis_table中保存的是每个分析的submit_location与每个分析对应的主表名字（mongo或者controller中可以找到）
        2）子分析分投递型的与即时型的，投递的分析要在前面重写投递的参数，即时的只要在sub_analysis这个字典中添加就ok（注意了要有每个分析的api）
        3）参数判断的那个函数中，要根据每个子分析的controller中的参数重组格式一模一样，只有这样才能匹配上对应的params，同时要在这个函数中对应的地方写上submit_location
        4）一键化的所有的分析，都是以submit_location来区分，所以在对接的时候要着重考虑与前端的submit_location是不是一致的。
        :return:
        """
        self.analysis_params = {
            "randomforest_analyse": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "ntree_id", "submit_location"], "collection_name": "sg_randomforest"},
            "sixteens_prediction": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["group_method", "task_type", "submit_location"], "collection_name": "sg_16s"},
            "species_lefse_analyse": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "second_group_id", "lda_filter", "second_group_detail", "submit_location", "start_level", "strict", "end_level"], "collection_name": "sg_species_difference_lefse"},
            "alpha_rarefaction_curve": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["index_type", "freq", "submit_location", "task_type"], "collection_name": "sg_alpha_rarefaction_curve"},
            "otunetwork_analyse": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "submit_location"], "collection_name": "sg_network"},
            "roc_analyse": {"instant": False, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "top_n_id", "method_type", "submit_location"], "collection_name": "sg_roc"},
            "alpha_diversity_index": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["index_type", "submit_location", "task_type"], "collection_name": "sg_alpha_diversity"},
            "alpha_ttest": {"instant": True, "waits": ["alpha_diversity_index"], "main": [], "others": ["task_type", "submit_location", "test_method"], "collection_name": "sg_alpha_ttest"},
            "beta_multi_analysis_plsda": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["analysis_type", "task_type", "submit_location"], "collection_name": "sg_beta_multi_analysis"},
            "beta_sample_distance_hcluster_tree": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["distance_algorithm", "hcluster_method", "task_type", "submit_location"], "collection_name": "sg_newick_tree"},
            "beta_multi_analysis_pearson_correlation": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "species_cluster", "submit_location", "method", "top_species", "env_cluster"], "collection_name": "sg_species_env_correlation"},
            "beta_multi_analysis_rda_cca": {"instant": True, "waits": ["otu_subsample"], "main": ["env_id", "env_labs"], "others": ["analysis_type", "task_type", "submit_location"], "collection_name": "sg_beta_multi_analysis"},
            "hc_heatmap": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "add_Algorithm", "submit_location", "sample_method", "species_number", "method"], "collection_name": "sg_hc_heatmap"},
            "beta_multi_analysis_anosim": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["distance_algorithm", "permutations", "submit_location", "task_type"], "collection_name": "sg_beta_multi_anosim"},
            "species_difference_multiple": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "correction", "methor", "submit_location", "coverage", "test"], "collection_name": "sg_species_difference_check"},
            "beta_multi_analysis_results": {"instant": True, "waits": ["otu_subsample"], "main": ["env_id", "env_labs"], "others": ["otu_method", "env_method", "submit_location", "task_type"], "collection_name": "sg_species_mantel_check"},
            "beta_multi_analysis_pcoa": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["analysis_type", "distance_algorithm", "submit_location", "task_type"], "collection_name": "sg_beta_multi_analysis"},
            "beta_multi_analysis_dbrda": {"instant": True, "waits": ["otu_subsample"], "main": ["env_id", "env_labs"], "others": ["analysis_type", "distance_algorithm", "submit_location", "task_type"], "collection_name": "sg_beta_multi_analysis"},
            "species_difference_two_group": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["ci", "task_type", "correction", "methor", "submit_location", "coverage", "test", "type"], "collection_name": "sg_species_difference_check"},
            "beta_multi_analysis_nmds": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["analysis_type", "distance_algorithm", "submit_location", "task_type"], "collection_name": "sg_beta_multi_analysis"},
            "otu_group_analyse": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "submit_location"], "collection_name": "sg_otu"},
            "otu_venn": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "submit_location"], "collection_name": "sg_otu_venn"},
            "beta_multi_analysis_pca": {"instant": True, "waits": ["otu_subsample"], "main": ["env_id", "env_labs"], "others": ["analysis_type", "task_type", "submit_location"], "collection_name": "sg_beta_multi_analysis"},
            "corr_network_analyse": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["abundance", "coefficient", "ratio_method", "lable", "submit_location", "task_type"], "collection_name": "sg_corr_network"},
            "otu_pan_core": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "submit_location"], "collection_name": "sg_otu_pan_core"},
            "plot_tree": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "color_level_id", "submit_location", "topn"], "collection_name": "sg_phylo_tree"},
            "enterotyping": {"instant": True, "waits": ["otu_subsample"], "main": [], "others": ["task_type", "submit_location"], "collection_name": "sg_enterotyping"},
        }
        # shenghe
        # data = json.load(open('data.json'))
        # self.web_data = data
        self.web_data = json.loads(self.option('data'))
        self.web_data['sub_analysis'] = json.loads(
            self.web_data['sub_analysis'])
        self.group_infos = json.loads(self.web_data['group_info'])
        self.levels = [int(i) for i in self.web_data[
            'level_id'].strip().split(',')]
        self.count_ends = 0
        self.task_client = self.web_data['client']
        # shenghe
        # self.mysql_client_key = worker_client().get_key(self.task_client)
        self.mysql_client_key = worker_client().add_task({})
        self.mysql_client_key = 'mykey'
        # monkey.patch_socket(aggressive=False, dns=False)
        # monkey.patch_ssl()
        self.signature = self.signature()
        self.task_id = self.option("task_id")
        self.url = "http://bcl.i-sanger.com" if self.task_client == "client01" else "http://192.168.12.102:9090"
        self.all = {}
        self.all_count = 0
        sixteens_prediction_flag = False  # 16s功能预测分析特殊性，没有分类水平参数
        for level in self.levels:
            self.all[level] = {}
            for group_info in self.group_infos:
                pipe = {}
                api, instant, collection_name, params = self.get_otu_subsample_params(
                    group_info)
                otu_subsample = SubmitOtuSubsample(
                    self, collection_name, params, api, instant)
                for i in self.web_data['sub_analysis']:
                    if i == 'sixteens_prediction' and sixteens_prediction_flag:
                        continue
                    api, instant, collection_name, params = self.get_params(i)
                    params['group_id'] = group_info['group_id']
                    params['group_detail'] = group_detail_sort(
                        group_info['group_detail'])
                    params['level_id'] = level
                    pipe[i] = self.get_class(i)(
                        self, collection_name, params, api, instant)
                pipe['otu_subsample'] = otu_subsample
                otu_subsample.start([], timeout=6000)
                for analysis, submit in pipe.iteritems():
                    if analysis == 'otu_subsample':
                        continue
                    waits = [pipe[i]
                             for i in self.analysis_params[analysis]["waits"]]
                    submit.start(waits, timeout=6000)
                self.all[level][group_info["group_id"]] = pipe
                self.all_count += len(pipe)
                sixteens_prediction_flag = True

        self.all_end = AsyncResult()
        self.all_end.get()
        self.end()

    def run(self):
        super(PipeSubmitTool, self).run()
        self.run_webapitest()


class Submit(object):
    """投递对象"""

    def __init__(self, bind_object, collection, params, api, instant):
        """
        :params bind_object:
        :params collection:
        :params params:
        :params api:
        :params instant:
        """
        self.workflow_id = ''  # workflow的ID
        self.task_id = bind_object.task_id
        self.api = api  # 接口url
        self.main_table_id = ''  # 主表_id
        self._params = params  # 参数
        self.success = False  # 是否成功
        self.is_end = False  # 是否结束
        self.error_info = None  # 错误信息
        self.mongo_collection = collection  # mongo表名称
        self.instant = instant  # 即时任务
        self._end_event = AsyncResult()  # end事件
        self.db = bind_object.db  # mongodb对象
        self.bind_object = bind_object  # tool对象
        self.out_params = {}  # 输出参数，单一个分析需要给其他分析提供参数时提供
        self.result = {}  # 返回结果

    def params_pack(self, dict_params):
        """
        参数打包，用于比对mongo数据库
        """
        return json.dumps(dict_params, sort_keys=True, separators=(',', ':'))

    def get_workflow_id(self):
        """
        获取workflow_id
        """
        # self.workflow_id = self.result['workflow_id']
        if self.workflow_id:
            return self.workflow_id
        result = self.db.workflowid2analysisid.find_one(
            {"main_id": ObjectId(self.main_table_id)}, {'workflow_id': 1})
        if result:
            self.workflow_id = result['workflow_id']
            return self.workflow_id
        else:
            self.bind_object.logger.error(
                "没有通过主表ID: {} 在workflowid2analysisid找到任务ID信息".format(self.main_table_id))
            raise Exception("没有找到任务ID信息")

    def start(self, waits, timeout=6000):
        self.waits = waits
        gevent.spawn(self._submit, waits, timeout=timeout)

    def end_fire(self):
        """
        分析结束后的后续工作
        """
        if self.result['success']:
            self.success = True
            self.set_out_params()
        self.is_end = True
        self.bind_object.one_end(self)

    def _submit(self, waits, timeout):
        """投递任务"""
        for i in waits:
            i.end_event.get(timeout=timeout)
            if not i.success:
                self.bind_object.rely_error(self, i)
                return
        self.run_permission()
        self.post_to_webapi()
        if 'success' in self.result and self.result['success']:
            if isinstance(self.result['content']['ids'], list):
                self.main_table_id = self.result['content']['ids'][0]['id']
            else:
                self.main_table_id = self.result['content']['ids']['id']
            if not self.instant:
                self.check_end(ObjectId(self.main_table_id))
        self.end_fire()
        self._end_event.set()

    def post_to_webapi(self):
        """
        投递接口
        """
        if self.check_params():
            return self.result
        self.result = self.post()

    def run_permission(self):
        if self.instant:
            for i in xrange(1000):
                if self.bind_object.count_instant_running < self.bind_object.max_instant_running:
                    break
                gevent.sleep(3)
            else:
                self.bind_object.logger.warn(
                    "任务等待时间达到上限3000s,直接运行: {}".format(self.api))
            self.bind_object.count_instant_running += 1
        else:
            for i in xrange(1000):
                if self.bind_object.count_submit_running < self.bind_object.max_submit_running:
                    break
                gevent.sleep(3)
            else:
                self.bind_object.logger.warn(
                    "任务等待时间达到上限3000s,直接运行: {}".format(self.api))
            self.bind_object.count_submit_running += 1
        self.bind_object.logger.info("任务开始投递: {}, 当前即时任务数: {}, 投递任务数: {}".format(self.api,
                                                                                 self.bind_object.count_instant_running,
                                                                                 self.bind_object.count_submit_running))

    def url_params_format(self):
        temp_params = copy.deepcopy(self._params)
        dumps_list = ["group_detail", "second_group_detail", "filter_json"]
        for i in dumps_list:
            if i in temp_params and temp_params[i]:
                temp_params[i] = json.dumps(
                    temp_params[i], sort_keys=True, separators=(',', ':'))
        return temp_params

    def post(self):
        """
        执行post
        """
        self.url = self.bind_object.url + self.api
        httpHandler = urllib2.HTTPHandler(debuglevel=1)
        httpsHandler = urllib2.HTTPSHandler(debuglevel=1)
        opener = urllib2.build_opener(httpHandler, httpsHandler)
        urllib2.install_opener(opener)
        params = self.url_params_format()
        data = urllib.urlencode(params)
        url = "%s?%s" % (self.url, self.bind_object.signature)
        request = urllib2.Request(url, data)
        for i in range(3):
            try:
                response = gevent_url_fetch(request)[1]
                # response = urllib2.urlopen(request)
            except (urllib2.HTTPError, urllib2.URLError, httplib.HTTPException, SocketError) as e:
                self.bind_object.logger.warn(
                    "接口投递失败, 第{}次(最多三次尝试), url: {}, 错误:{}".format(i + 1, self.api, e))
                time.sleep(50)  # 休眠50s 目的是为了等端口重启好
            else:
                the_page = response.read()
                return json.loads(the_page)
        return {"info": "分析项投递失败！", "success": False}

    @property
    def end_event(self):
        return self._end_event

    def check_end(self, id, timeout=3600):
        """
        检查投递任务是否结束
        """
        for i in xrange(100):
            self.bind_object.logger.info("第 {} 次 查询数据库，检测任务({}, {}))是否完成".format(
                i + 1, self.mongo_collection, self.main_table_id))
            mongo_result = self.db[self.mongo_collection].find_one(
                {"_id": ObjectId(self.main_table_id)}, {"status": 1, "desc": 1, "name": 1})
            self.result = {'success': True, "info": mongo_result['desc'],
                           'content': {"ids": {'id': str(mongo_result['_id']), 'name': mongo_result["name"]}}}
            if mongo_result["status"] == "end":
                break
            elif mongo_result["status"] != "start":
                self.result["success"] = False
                break
            gevent.sleep(20)
        return
        # self.get_workflow_id()
        # try:
        #     self.bind_object.logger.info("开始向WPM服务请求等待任务: {} 结束".format(self.workflow_id))
        #     get = gevent_url_fetch(self.bind_object.url + "/report/" + self.workflow_id)[1]
        #     self.bind_object.logger.info("向WPM服务请求等待结果为: {}".format(get.read()))
        #     # wait(self.workflow_id, timeout=timeout)
        # except Exception as e:
        #     self.bind_object.logger.info("ERROR:{}".format(traceback.format_exc()))
        #     self.bind_object.logger.info("任务等待超时: workflow_id: {}, url: {}".format(self.workflow_id, self.api))

    def check_params(self):
        """
        检查参数，发现end状态，直接放回计算完成，发现start状态，直接监控直到结束
        """
        self.waits_params_get()  # 依赖分析的参数获取
        result = self.db[self.mongo_collection].find({'task_id': self.task_id, 'params': self.params_pack(
            self._params), 'status': {'$in': ['end', 'start', "failed"]}})
        if not result.count():
            self.bind_object.logger.info("参数比对没有找到相关结果: 任务: {}, collection: {}, params: {}".format(
                self.api, self.mongo_collection, self.params_pack(self._params)))
            return False
        else:
            lastone = result.sort('created_ts', pymongo.DESCENDING)[0]
            self.bind_object.logger.info("参数比对找到已经运行的结果: 任务: {}, 状态: {}, collection: {}, _id: {}".format(
                self.api, lastone['status'], self.mongo_collection, lastone["_id"]))
            self.main_table_id = lastone['_id']
            if lastone['status'] == 'end':
                self.result = {'success': True, "info": lastone['desc'],
                               'content': {"ids": {'id': str(lastone['_id']), 'name': lastone["name"]}}}
                # self.result = {'success': True, 'main_id': lastone['_id']}
                return self.result
            elif lastone['status'] == 'start':
                self.check_end(lastone['_id'])
                self.result = {'success': True, "info": lastone['desc'],
                               'content': {"ids": {'id': str(lastone['_id']), 'name': lastone["name"]}}}
                result = self.db[self.mongo_collection].find_one(
                    {'_id': lastone['_id']}, {'status': 1, 'desc': 1, "name": 1})
                if result['status'] != 'end':  # 任务完成后检查状态
                    self.result['success'] = False
                return self.result
        return False

    def waits_params_get(self):
        """
        在子类中重写，获取等待的分析结果
        """
        pass
        return self._params

    def set_out_params(self):
        """
        在子类中重写，设置结果到out_params,方便其他分析使用
        """
        pass


class BetaSampleDistanceHclusterTree(Submit):
    """
    """

    def waits_params_get(self):
        self._params['otu_id'] = self.waits[0].out_params['otu_id']
        return self._params


class SubmitOtuSubsample(Submit):

    def set_out_params(self):
        self.out_params['otu_id'] = self.result["content"]['ids']['id']


class AlphaDiversityIndex(Submit):

    def set_out_params(self):
        self.out_params['alpha_diversity_id'] = self.result[
            "content"]['ids']['id']


class AlphaTtest(Submit):

    def waits_params_get(self):
        self._params['alpha_diversity_id'] = self.waits[
            0].out_params['alpha_diversity_id']
        return self._params


class SixteensPrediction(BetaSampleDistanceHclusterTree):
    pass


class SpeciesLefseAnalyse(BetaSampleDistanceHclusterTree):
    pass


class OtunetworkAnalyse(BetaSampleDistanceHclusterTree):
    pass


class AlphaRarefactionCurve(BetaSampleDistanceHclusterTree):
    pass


class RandomforestAnalyse(BetaSampleDistanceHclusterTree):
    pass


class RocAnalyse(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisPlsda(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisPearsonCorrelation(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisRdaCca(BetaSampleDistanceHclusterTree):
    pass


class HcHeatmap(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisAnosim(BetaSampleDistanceHclusterTree):
    pass


class SpeciesDifferenceMultiple(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisResults(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisPcoa(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisDbrda(BetaSampleDistanceHclusterTree):
    pass


class SpeciesDifferenceTwoGroup(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisNmds(BetaSampleDistanceHclusterTree):
    pass


class OtuGroupAnalyse(BetaSampleDistanceHclusterTree):
    pass


class OtuVenn(BetaSampleDistanceHclusterTree):
    pass


class BetaMultiAnalysisPca(BetaSampleDistanceHclusterTree):
    pass


class CorrNetworkAnalyse(BetaSampleDistanceHclusterTree):
    pass


class OtuPanCore(BetaSampleDistanceHclusterTree):
    pass


class PlotTree(BetaSampleDistanceHclusterTree):
    pass


class Enterotyping(BetaSampleDistanceHclusterTree):
    pass


class gevent_HTTPConnection(httplib.HTTPConnection):

    def connect(self):
        import socket
        from gevent import socket as cosocket
        if self.timeout is socket._GLOBAL_DEFAULT_TIMEOUT:
            timeout = cosocket._GLOBAL_DEFAULT_TIMEOUT
        else:
            timeout = self.timeout
        self.sock = cosocket.create_connection((self.host, self.port), timeout)


class gevent_HTTPHandler(urllib2.HTTPHandler):

    def http_open(self, request):
        return self.do_open(gevent_HTTPConnection, request)


def gevent_url_fetch(url):
    opener = urllib2.build_opener(gevent_HTTPHandler)
    resp = opener.open(url)
    return resp.headers, resp

if __name__ == "__main__":
    PipeSubmitTool().run_webapitest()
