## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mainapp.libs.signature import CreateSignature
from biocluster.config import Config
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
        # self.python_path = 'program/Python/bin/python '
        # self.script_path = os.path.join(Config().SOFTWARE_DIR, "bioinfo/meta/scripts/corr_net_calc.py")

    def run_webapitest(self):
        # collection = self.db["sg_network"]
        # result = collection.find_one({"_id": ObjectId("589d7179a4e1af2fffc579d2")})

        data = self.option('data')
        print "打印出data"
        print data
        data = json.loads(data)
        print "----"
        print data
        env_id = data['env_id']
        env_labs = data['env_labs']
        group_infos = data['group_info']
        level_ids = data['level_id']
        sub_analysis = data['sub_analysis']
        otu_id = data['otu_id']
        client = data['client']
        if client == "client01":
            base_url = 'http://bcl.i-sanger.com'
        elif client == "client03":
            base_url = "http://10.101.203.193:9100"
            #base_url = "http://192.168.12.102:9090"
        else:
            print "***client必须是client01或者client03***"
        print "client= %s, base_url= %s"%(client, base_url)
        #计算抽平
        params = {"otu_id": str(otu_id), "group_id": data['group_id'], "group_detail": data['group_detail'], "submit_location": "otu_statistic",
                  "filter_json": data['filter_json'], "task_type": "reportTask", "size": data['size'], "level_id": "9"}
        api_statistic = "meta/otu_subsample"
        method = "post"
        results_statistic = self.run_controllers(api=api_statistic, client=client, base_url=base_url, params=params, method=method)
        results_statistic = json.loads(results_statistic)
        otu_id = results_statistic['sub_anaylsis_id']['id']  #后面用抽平后的otu_id
        print "打印抽平后的otu_id"
        print otu_id
        list2 = [] #用于存储分类水平与分组方案的所有的组合
        for level in level_ids:
            for group in group_infos:
                m = {"otu_id": str(otu_id), "level_id": str(level), "group_id": group['group_id'], "group_detail": json.dumps(group['group_detail']),
                     "env_id": str(env_id), "env_labs": str(env_labs)}
                list2.append(m)
        print list2
        all_results = []  # 存储所有子分析的ids
        #定义多样性指数与指数间差异分析的接口投递
        anaylsis_names = []
        alpha_diversity_index_data = {}
        alpha_ttest_data = {}
        for m in sub_analysis:
            for key in m:
                anaylsis_names.append(key)
                if key == "alpha_diversity_index":
                    alpha_diversity_index_data = m[key]
                elif key == "alpha_ttest":
                    alpha_ttest_data = m[key]
                else:
                    pass
        print "\n"
        print anaylsis_names
        print alpha_diversity_index_data
        print alpha_ttest_data
        print "\n"
        if "alpha_ttest" in anaylsis_names and "alpha_diversity_index" in anaylsis_names:
            for n in list2:
                alpha_diversity_index_data['otu_id'] = n['otu_id']
                alpha_diversity_index_data['level_id'] = n['level_id']
                alpha_diversity_index_data['group_id'] = n['group_id']
                alpha_diversity_index_data['group_detail'] = json.loads(n['group_detail'])
                results_alpha_diversity_index = self.run_controllers(api=alpha_diversity_index_data['api'], client=client, base_url=base_url,
                                                         params=alpha_diversity_index_data, method=method)
                results_alpha_diversity_index = json.loads(results_alpha_diversity_index)
                all_results.append(results_alpha_diversity_index)
                alpha_diversity_id = results_alpha_diversity_index['sub_anaylsis_id']['id']
                alpha_ttest_data['otu_id'] = n['otu_id']
                alpha_ttest_data['level_id'] = n['level_id']
                alpha_ttest_data['group_id'] = n['group_id']
                alpha_ttest_data['alpha_diversity_id'] = str(alpha_diversity_id)
                alpha_ttest_data['group_detail'] = json.loads(n['group_detail'])
                results_alpha_ttest = self.run_controllers(api=alpha_ttest_data['api'],
                                                                     client=client, base_url=base_url,
                                                                     params=alpha_ttest_data, method=method)
                results_alpha_ttest = json.loads(results_alpha_ttest)
                all_results.append(results_alpha_ttest)
        else:
            pass
        #删除子分析字典中alpha_diversity_index，alpha_ttest两个分析,重构sub_analysis数组
        new_sub_analysis = []
        for m in sub_analysis:
            for key in m:
                if key == "alpha_diversity_index" or key == "alpha_ttest":
                    pass
                else:
                    new_sub_analysis.append(m)
        print "打印删除子分析字典中alpha_diversity_index，alpha_ttest两个分析,重构后的sub_analysis"
        print new_sub_analysis
        print len(new_sub_analysis)

        #定义没有依赖的分析的通用接口投递
        for info in list2:
            print "打印出info"
            print info
            for anaylsis in new_sub_analysis:
                print "打印出new_sub_analysis"
                print anaylsis
                for key in anaylsis:
                    anaylsis[key]['otu_id'] = info['otu_id']
                    anaylsis[key]['level_id'] = info['level_id']
                    anaylsis[key]['group_id'] = info['group_id']
                    anaylsis[key]['group_detail'] = json.loads(info['group_detail'])
                    anaylsis[key]['env_id'] = info['env_id']
                    anaylsis[key]['env_labs'] = info['env_labs']
            # print "下面打印出来的是每个子分析new_sub_analysis添加了分组与分类水平的data值"
            # print new_sub_analysis
            for m in new_sub_analysis:
                name = []
                data = [] #用于存储子分析参数的具体数值
                submit_info = {}
                for key in m:
                    for key1 in m[key]:
                        name.append(key1)
                        data.append(m[key][key1])
                        # submit_info = {"api": m[key]['api'], "base_url": m[key]['base_url'], "client": m[key]['client'], 'method': "post"}
                        submit_info = {"api": m[key]['api']}
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
                # print "打印出每个子分析的返回值"
                # print result
                results = {"level_id": info['level_id'], "group_id": info['group_id'], "sub_anaylsis_id": result['content']['ids']}
                all_results.append(results)
        print "打印出所有子分析的ids"
        print all_results
        print len(all_results)
        output_table = os.path.join(self.work_dir, "ids.txt")
        with open(output_table, "w") as w:
            w.write(str(all_results))


    def run_controllers(self, api, client, base_url, params, method, header=None):
        """
        用于重构每个子分析的输入参数，（参考sub_anaylsis中的参数）并进行投递计算,注输入的参数必须是字典格式
        params样例：{\"submit_location\": \"corrnetwork_analyse\", \"api\": \"meta/corr_network\",
        \"task_type\": \"reportTask\",\"lable\": \"0.03\", \"ratio_method\": \"pearson\", \"coefficient\":
        \"0.08\", \"abundance\": \"150\"}
        :return:
        """
        if not isinstance(params, dict):
            success.append("传入的params不是一个字典")
        name = []
        data = []
        for key in params:
            name.append(key)
            data.append(params[key])
        name = ";".join(name)
        data = ";".join(data)
        return_page = self.webapitest(method, api, name, data, client, base_url)
        result = json.loads(return_page)
        result = json.loads(result)
        results = {"level_id": params['level_id'], "group_id": params['group_id'],
                   "sub_anaylsis_id": result['content']['ids']}
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
