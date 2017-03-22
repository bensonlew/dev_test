# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import *
from collections import defaultdict


class OtunetworkWorkflow(Workflow):
    """
    报告中进行OTU网络构建与分析时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(OtunetworkWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "int"},
            {"name": "grouptable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "network_id", "type": "string"},
            {"name": "add_Algorithm", "type": "string", "default": ""},  # 分组样本求和算法，默认不求和
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.otunetwork = self.add_tool('meta.otu.otunetwork')

    def change_otuname(self, tablepath):
        """
        这一步骤只是将otu名字中的空格去掉
        :param tablepath:
        :return:
        """
        newtable = os.path.join(self.work_dir, 'otutable1.xls')
        f2 = open(newtable, 'w+')
        with open(tablepath, 'r') as f:
            i = 0
            for line in f:
                if i == 0:
                    i = 1
                    f2.write(line)
                else:
                    line = line.strip().split('\t')
                    line_data = line[0].strip().split(' ')
                    line_he = "".join(line_data)
                    line[0] = line_he
                    for i in range(0, len(line)):
                        if i == len(line)-1:
                            f2.write("%s\n"%(line[i]))
                        else:
                            f2.write("%s\t"%(line[i]))
        f2.close()
        return newtable

    def cat_samples(self, otu, method):
        """
        合并同一分组的样本，可进行求和，求平均，求中位数
        :param method:
        :return:
        """
        grouptable = "O:\\Users\\hongdong.xuan\\Desktop\\Otunetwork_tsg_6592_4367_7992\\grouptable_input.group.xls"
        cat_otu_path = "O:\\Users\\hongdong.xuan\\Desktop\\Otunetwork_tsg_6592_4367_7992\\out.xls"
        sample_group = dict()  # 一个样本是属于哪个group的
        index_sample = dict()  # 一个OTU表中第几列属于哪个样本
        group_sample_num = defaultdict(int)  # 一个分组里面有多少的样本
        cat_otu_path = os.path.join(self.work_dir, "cat_otu.xls")
        with open(grouptable, "rb") as r:
            line = r.next()
            for line in r:
                line = line.rstrip().split("\t")
                sample_group[line[0]] = line[1]
                group_sample_num[line[1]] += 1

        with open(otu, "rb") as r, open(cat_otu_path, 'wb') as w:
            group_list = list()
            for v in sample_group.values():
                group_list.append(v)
                group_list = list(set(group_list))
            print group_list
            # l = len(group_list) #zx

            line = r.next().rstrip().split("\t")
            print line
            for i in range(len(line)):
                index_sample[i] = line[i]
            print index_sample

            w.write(index_sample[0] + "\t")
            w.write("\t".join(group_list) + "\n")
            for line in r:
                line = line.rstrip().split("\t")
                num = defaultdict(int)
                middle_num = defaultdict(int)
                tmp = list()
                list1 = []
                mid_num = dict()
                w.write(line[0] + "\t")
                for i in range(1, len(line)):
                    num[sample_group[index_sample[i]]] += int(line[i])
                for m in group_list:
                    for i in range(1, len(line)):
                        if sample_group[index_sample[i]] == m:
                            list1.append(int(line[i]))
                            if len(list1) == group_sample_num[m]:
                                list1.sort()
                                yu = int(group_sample_num[m]) % 2
                                index = int(int(group_sample_num[m]) / 2)
                                if yu == 0:
                                    mid_num[m] = int(round((int(list1[index - 1]) + int(list1[index])) / 2))
                                    list1 = []
                                else:
                                    mid_num[m] = list1[index]
                                    list1 = []

                if method == "sum":
                    for g in group_list:
                        tmp.append(str(num[g]))
                if method == "average":
                    for g in group_list:
                        avg = int(round(num[g] / group_sample_num[g]))
                        tmp.append(str(avg))
                if method == "middle":
                    for g in group_list:
                        tmp.append(str(mid_num[g]))
                w.write("\t".join(tmp))
                w.write("\n")
        return cat_otu_path


    def run_otunetwork(self):
        newtable = self.change_otuname(self.option('otutable').prop['path'])
        #newtable = os.path.join(self.work_dir, 'otutable1.xls')
        if self.option("group_id") == 'all':
            options = {
                'otutable': newtable,
            }

        else:
            options = {
                'otutable': newtable,
                'grouptable': self.option('grouptable')}

        self.otunetwork.set_options(options)
        self.otunetwork.on('end', self.set_db)
        self.output_dir = self.otunetwork.output_dir
        self.otunetwork.run()


    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "网络分析结果输出目录"],
            ["./real_node_table.txt", "txt", "网络节点属性表"],
            ["./real_edge_table.txt", "txt", "网络边的属性表"],
            ["./real_dc_otu_degree.txt", "txt", "网络物种节点度分布表"],
            ["./real_dc_sample_degree.txt", "txt", "网络sample节点度分布表"],
            ["./real_dc_sample_otu_degree.txt", "txt", "网络所有节点度分布表"],
            ["./network_centrality.txt", "txt", "网络中心系数表"],
            ["./network_attributes.txt", "txt", "网络单值属性表"],
            ["./network_degree.txt", "txt", "OTU网络度统计总表"]
        ])
        super(OtunetworkWorkflow, self).end()

    def set_db(self):
        """
        报存分析结果到mongo数据库中
        """
        api_otunetwork = self.api.otunetwork
        node_table_path = self.output_dir + '/real_node_table.txt'
        edge_table_path = self.output_dir + '/real_edge_table.txt'
        otu_degree_path = self.output_dir + '/real_dc_otu_degree.txt'
        sample_degree_path = self.output_dir + '/real_dc_sample_degree.txt'
        sample_otu_degree_path = self.output_dir + '/real_dc_sample_otu_degree.txt'
        network_centrality_path = self.output_dir + '/network_centrality.txt'
        network_attributes_path = self.output_dir + '/network_attributes.txt'
        network_degree_path = self.output_dir + '/network_degree.txt'
        if not os.path.isfile(node_table_path):
            raise Exception("找不到报告文件:{}".format(node_table_path))
        if not os.path.isfile(edge_table_path):
            raise Exception("找不到报告文件:{}".format(edge_table_path))
        if not os.path.isfile(otu_degree_path):
            raise Exception("找不到报告文件:{}".format(otu_degree_path))
        if not os.path.isfile(sample_degree_path):
            raise Exception("找不到报告文件:{}".format(sample_degree_path))
        if not os.path.isfile(sample_otu_degree_path):
            raise Exception("找不到报告文件:{}".format(sample_otu_degree_path))
        if not os.path.isfile(network_centrality_path):
            raise Exception("找不到报告文件:{}".format(network_centrality_path))
        if not os.path.isfile(network_attributes_path):
            raise Exception("找不到报告文件:{}".format(network_attributes_path))
        if not os.path.isfile(network_degree_path):
            raise Exception("找不到报告文件:{}".format(network_degree_path))
        print 'stat insert 1'
        api_otunetwork.add_node_table(file_path=node_table_path, table_id=self.option("network_id"))
        #api_otunetwork.add_node_table_group(file_path=node_table_path, table_id=self.option("network_id"))
        api_otunetwork.add_edge_table(file_path=edge_table_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_attributes(file_path=network_attributes_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_degree(file1_path=otu_degree_path,file2_path=sample_degree_path, file3_path=sample_otu_degree_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.option("network_id"))
        print 'stat insert 1'
        self.end()

    def run(self):
        self.run_otunetwork()
        super(OtunetworkWorkflow, self).run()