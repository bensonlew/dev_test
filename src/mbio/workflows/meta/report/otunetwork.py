# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import *


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
            {"name": "group_detail", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "network_id", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.otunetwork = self.add_tool('meta.otu.otunetwork')

    def change_otuname(self, tablepath):
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


    def run_otunetwork(self):
        newtable = self.change_otuname(self.option('otutable').prop['path'])
        #newtable = os.path.join(self.work_dir, 'otutable1.xls')
        options = {
            'otutable': newtable,
            # 'level': self.option('level'),
            'grouptable': self.option('grouptable')
            }

        self.otunetwork.set_options(options)
        self.otunetwork.on('end', self.set_db)
        self.output_dir = self.otunetwork.output_dir
        self.otunetwork.run()


    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "OTU网络分析结果输出目录"],
            ["./real_node_table.txt", "txt", "OTU网络节点属性表"],
            ["./real_edge_table.txt", "txt", "OTU网络边集属性表"],
            ["./real_dc_otu_degree.txt", "txt", "OTU网络OTU节点度分布表"],
            ["./real_dc_sample_degree.txt", "txt", "OTU网络sample节点度分布表"],
            ["./real_dc_sample_otu_degree.txt", "txt", "OTU网络节点度分布表"],
            ["./network_centrality.txt", "txt", "OTU网络中心系数表"],
            ["./network_attributes.txt", "txt", "OTU网络单值属性表"],
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
        sample_otu_degree_path= self.output_dir + '/real_dc_sample_otu_degree.txt'
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
        api_otunetwork.add_node_table_group(file_path=node_table_path, table_id=self.option("network_id"))
        api_otunetwork.add_edge_table(file_path=edge_table_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_attributes(file_path=network_attributes_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_degree(file_path=network_degree_path, table_id=self.option("network_id"))
        api_otunetwork.add_network_centrality(file_path=network_centrality_path, table_id=self.option("network_id"))
        print 'stat insert 1'
        self.end()

    def run(self):
        self.run_otunetwork()
        super(OtunetworkWorkflow, self).run()