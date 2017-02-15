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
    last_modify: 2017.02.10
    """
    def __init__(self, parent):
        super(PipeSubmitAllAgent, self).__init__(parent)
        options = [
            {"name": "data", "type": "string"}
            # {"name": "method", "type": "string", "default": "post"},
            # {"name": "name", "type": "string"},
            # {"name": "data", "type": "string"},
            # {"name": "client", "type": "string"},
            # {"name": "base_url", "type": "string"}
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
        # if not self.option("method"):
        #     raise OptionError("必须输入method值（get or post）")
        # if not self.option('name'):
        #     raise OptionError('必须输入参数的name')
        # if not self.option('data'):
        #     raise OptionError('必须输入data')
        # if not self.option('client'):
        #     raise OptionError('必须输入client')
        # if not self.option('base_url'):
        #     raise OptionError('必须输入base_url')
        if not self.option("data"):
            raise OptionError("必须输入接口传进来的data值")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '200G'

    def end(self):
        # result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #     [".", "", "结果输出目录"],
        #     ["corr_network_attributes.txt", "txt", "网络的单值属性表"],
        #     ["corr_network_by_cut.txt", "txt", "相关系数筛选后网络边文件"],
        #     ["corr_network_centrality.txt", "txt", "网络节点的中心系数表"],
        #     ["corr_network_clustering.txt", "txt", "网络节点的聚类系数表"],
        #     ["corr_network_degree_distribution.txt", "txt", "网络节点的度分布表"],
        #     ["corr_network_node_degree.txt", "txt", "网络节点的度统计表"]
        #     ])

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
        group_infos = data['group_info']
        level_ids = data['level_id']
        sub_analysis = data['sub_analysis']
        otu_id = data['otu_id']
        list2 = [] #用于存储分类水平与分组方案的所有的组合
        for level in level_ids:
            for group in group_infos:
                m = {"otu_id": str(otu_id), "level_id": str(level), "group_id": group['group_id'], "group_detail": json.dumps(group['group_detail'])}
                list2.append(m)
        print list2
        all_results = []  # 存储所有子分析的ids
        for info in list2:
            print "打印出info"
            print info
            for anaylsis in sub_analysis:
                print "打印出sub_analysis"
                print anaylsis
                for key in anaylsis:
                    anaylsis[key]['otu_id'] = info['otu_id']
                    anaylsis[key]['level_id'] = info['level_id']
                    anaylsis[key]['group_id'] = info['group_id']
                    anaylsis[key]['group_detail'] = json.loads(info['group_detail'])
            print "下面打印出来的是每个子分析sub_analysis添加了分组与分类水平的data值"
            print sub_analysis
            sub_results = [] #存储同一分类水平与分组方案所有子分析的ids
            for m in sub_analysis:
                name = []
                data = []
                submit_info = {}
                for key in m:
                    for key1 in m[key]:
                        name.append(key1)
                        data.append(m[key][key1])
                        submit_info = {"api": m[key]['api'], "base_url": m[key]['base_url'], "client": m[key]['client'], 'method': "post"}
                print "打印出每个分析对应的api，base_url，client，method"
                print submit_info
                name = ";".join(name)
                print "打印出name值"
                print name
                data = ";".join(data)
                print "打印出data值"
                print data
                api = submit_info['api']
                print api
                method = submit_info['method']
                print method
                client = submit_info['client']
                print client
                base_url = submit_info['base_url']
                print base_url
                return_page = self.webapitest(method, api, name, data, client, base_url)
                result = json.loads(return_page)
                result = json.loads(result)
                print "打印出每个子分析的返回值"
                print result
                sub_results.append(result['content']['ids']) #用于导表
                all_results.append(result['content']['ids'])
            print "sub_results"
            print sub_results
            #这个位置写到组合表文件
        print "打印出所有子分析的ids"
        print all_results
        output_table = os.path.join(self.work_dir, "ids.txt")
        with open(output_table, "w") as w:
            w.write(str(all_results))


    # def create_
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
            print("post data to url %s ...\n\n" % url)
            print("post data:\n%s\n" % data)
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
            # print type(the_page)
        return json.dumps(the_page)

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir + '/' + "corr_result/")
        for f in results:
            os.link(self.work_dir + '/' + "corr_result/" +f, self.output_dir + "/" +f)
        self.logger.info('设置文件夹路径成功')


    def run(self):
        super(PipeSubmitAllTool, self).run()
        self.run_webapitest()
        # self.set_output()
        self.end()
