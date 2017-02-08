# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'
from biocluster.workflow import Workflow
from mbio.api.to_file.meta import *
from biocluster.config import Config
from mainapp.libs.signature import CreateSignature
import subprocess
import urllib2
import urllib
import sys
import os
import json
import gevent
import time

class MetaPipelineWorkflow(Workflow):
    """
    报告中计算稀释性曲线时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaPipelineWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "submit_location", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "pipe_id", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())


    def run(self):
        # self.python_path = '/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python '
        # self.webapitest_path = '/mnt/ilustre/users/sanger-dev/biocluster/bin/webapitest.py'
        name_list = "otu_id;level_id;submit_location;group_detail;group_id;task_type"
        # print name_list
        # data_list = "57fca514a4e1af1f3872cd46;9;otunetwork_analyse;/mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/controller_test/otunetwork1;57fca514a4e1af1f3872cd37;reportTask"
        data_list = str(self.option("otu_id")) + ";" + str(self.option("level")) + ";" + str(self.option("submit_location")) + ";" + str(self.option("group_detail")) + \
                    ";" + str(self.option("group_id")) + ";" + str(self.option("task_type"))
        print data_list
        # one_cmd = self.python_path + self.webapitest_path + ' post meta/otu_network -c client03 -b http://192.168.12.102:9100 -n '+ str(name_list) + " -d " + str(data_list)
        # self.logger.info(one_cmd)
        # try:
        #     subprocess.call(one_cmd, shell = True)

        # except subprocess.CalledProcessError:
        #     raise Exception('运行异常')
        method = "post"
        api = "meta/otu_network"
        client = "client03"
        name = name_list
        data = data_list
        # base_url = "http://192.168.12.102:9100"
        base_url = "http://10.101.203.193:9100"
        return_page = self.webapitest(method, api, name, data, client, base_url)
        result = json.loads(return_page)  #return_page返回的是字符串，json.loads将字符串转为unicode，再次调用json.loads转为dict
        result = json.loads(result)
        all_results = []
        all_results.append(result['content']['ids'])
        print all_results
        # self.api.pipe_check_all.check_all(all_results, self.option("pipe_id"))
        # self.end()
        gevent.spawn(self.watch_end(all_results, self.option("pipe_id")))

        super(MetaPipelineWorkflow, self).run()

    def end(self):
        super(MetaPipelineWorkflow, self).end()

    def watch_end(self, all_results, main_table_id):
        while True:
            time.sleep(10)
            if self.api.pipe_check_all.check_all(all_results, main_table_id) == True:
                self.end()
            else:
                break

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