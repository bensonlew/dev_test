# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

"""样本菌群分型分析模块"""
import os
import json
import shutil
import datetime
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow
from mainapp.models.mongo.public.meta.meta import Meta


class EnterotypingWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(EnterotypingWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表
            {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "level", "type": "string", "default": "9"},  # 输入的OTU level
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"}  # 输入的group_detail 示例如下
            # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
            # {"name": "method", "type": "string", "default": ""}  # 聚类方式， ""为不进行聚类
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.enterotyping = self.add_tool("meta.beta_diversity.enterotyping")
        self.plot_enterotyping = self.add_tool("meta.beta_diversity.plot-enterotyping")
        self.a = ''
        self.spe_name = ''
        self.number = ''
        group_table_path = os.path.join(self.work_dir, "group_table.xls")
        self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)

    # def check_options(self):
    #     if self.option('method') not in ['average', 'single', 'complete', ""]:
    #         raise OptionError('错误的层级聚类方式：%s' % self.option('method'))

    def run_enterotyping(self):
        self.enterotyping.set_options({
            "otu_table": self.option("in_otu_table")
            # "group_table": self.group_table_path
        })
        self.enterotyping.on("end", self.set_plot_options)
        self.enterotyping.run()

    def set_plot_options(self):
        all_path = self.enterotyping.output_dir
        print(all_path)
        cluster_path = all_path + "/cluster.txt"
        # cluster_path = os.path.join("all_path", "/cluster.txt")
        print(cluster_path)
        up_num = []
        if os.path.exists(cluster_path):
            print ("true")
            a = open(cluster_path,"r")
            content = a.readlines()
            for f in content:
                if f.startswith("name") == False:
                    f = f.strip().split("\t")
                    up_num.append(f[1])
            a.close()
        up_num.sort()
        print(up_num)
        g = int(up_num[-1])
        print(g)
        a = []
        for i in range(1,g+1):
                n = str(i)
                a.append(n)
        a = ','.join(a)
        print (a)
        self.number = g+1
        self.a = a
        name = os.listdir(all_path)
        file_number = len(name)-2
        spe_name = []
        spe_name_re = []
        for i in range(1,file_number+1):
            path_c = "/" + str(i) + ".cluster.txt"
            print(all_path + path_c)
            if os.path.exists(all_path + path_c):
                b = open(all_path + path_c, "r")
                content = b.readlines()
                for f in content:
                    if f.startswith("taxon_name") == False:
                        f = f.strip().split("\t")
                        if f[0] not in spe_name_re :
                            spe_name_re.append(f[0])
                            t = f[0].split(" ")
                            spe_name.append(t[-1])
                            break
                        else:
                            continue
                b.close()
            else:
                break
        spe_name = ','.join(spe_name)
        print(spe_name)
        self.spe_name = spe_name
        self.run_plot_enterotyping()



    def run_plot_enterotyping(self):
        self.plot_enterotyping.set_options({
            "otu_table": self.option("in_otu_table"),
            "g": self.a,
            "s": self.spe_name,
	        # "group": self.option("group_detail")
            "group": self.group_table_path
        })
        # trans_otu = os.path.join(self.work_dir, "otu.trans")
        # self.sort_samples.option("out_otu_table").transposition(trans_otu)
        self.plot_enterotyping.on('end', self.set_db)
        self.plot_enterotyping.run()


    def set_db(self):
        self.logger.info("正在写入mongo数据库")
        cluster_name = []
        for i in range(1, int(self.number)):
            cluster_name.append(str(i) + ".cluster.txt")
        # newick_id = ""
        myParams = json.loads(self.sheet.params)
        # if self.option("method") != "":
        #     api_heat_cluster = self.api.heat_cluster
        #     name = "heat_cluster_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        #     newick_id = api_heat_cluster.create_newick_table(self.sheet.params, self.option("method"), myParams["otu_id"], name)
        #     self.hcluster.option("newicktree").get_info()
        #     api_heat_cluster.update_newick(self.hcluster.option("newicktree").prop['path'], newick_id)
        #     self.add_return_mongo_id("sg_newick_tree", newick_id, "", False)
        api_otu = self.api.enterotyping_db
        new_id = api_otu.add_sg_enterotyping(self.sheet.params, self.option("input_otu_id"), cluster_name = cluster_name)
        api_otu.add_sg_enterotyping_detail(new_id, self.enterotyping.output_dir + "/ch.txt", x = "x", y = "y", name = "ch.txt")
        api_otu.add_sg_enterotyping_detail(new_id, self.enterotyping.output_dir + "/cluster.txt",x = "sample_name", y = "enterotyping_group", name = "cluster.txt")
        api_otu.add_sg_enterotyping_detail(new_id, self.plot_enterotyping.output_dir + "/circle.txt", x = "x", y = "y", name = "circle.txt", detail_name = "circle_name")
        api_otu.add_sg_enterotyping_detail(new_id, self.plot_enterotyping.output_dir + "/point.txt", x="x", y="y",
                                           name="point.txt", detail_name="sample_name")
        for i in range(1, int(self.number)):
            api_otu.add_sg_enterotyping_detail_cluster(new_id, self.enterotyping.output_dir + "/" + str(i) + ".cluster.txt", name = str(i) + ".cluster.txt")
        api_otu.add_sg_enterotyping_detail_summary(new_id, self.plot_enterotyping.output_dir + "/summary.txt",
                                                   name="summary.txt")

        # new_id = api_otu.add_sg_enterotyping("nnnnn", self.enterotyping.option("result_dir").prop["path"] + "/ch.txt", self.option("input_otu_id"))
        # new_otu_id = api_otu.add_sg_otu(self.sheet.params, self.option("input_otu_id"), None, newick_id)
        # api_otu.add_sg_otu_detail(self.sort_samples.option("out_otu_table").prop["path"], new_otu_id, self.option("input_otu_id"))
        self.add_return_mongo_id("sg_enterotyping", new_id)
        self.end()

    def end(self):
        try:
            shutil.copy2(self.plot_enterotyping.output_dir + "/summary.txt", self.output_dir + "/summary.txt")
            shutil.copytree(self.enterotyping.output_dir, self.output_dir + "/enterotyping")
        except Exception as e:
            self.logger.info("summary.txt copy success{}".format(e))
            self.set_error("summary.txt copy success{}".format(e))
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "样本菌群分型分析结果输出目录"],
            ["./summary.txt", "txt", "summary数据表"],
            ["./enterotyping", "dir", "分型数据文件夹"],
            ["./enterotyping/ch.txt", "txt", "CH指数数据表"],
            ["./enterotyping/cluster.txt", "txt", "cluster数据表"]
        ])
        result_dir.add_regexp_rules([
            ["enterotyping/.+\cluster.txt$", "txt", "分型后各组数据表"]
        ])
        super(EnterotypingWorkflow, self).end()

    def run(self):
        self.run_enterotyping()
        super(EnterotypingWorkflow, self).run()
