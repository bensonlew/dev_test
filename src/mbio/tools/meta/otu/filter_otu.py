# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import json
import re
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class FilterOtuAgent(Agent):
    """
    根据传入的json，对一张OTU表进行过滤，过滤的条件有三种:
    species_filter: 物种过滤，用于保留或者滤去特定的物种
    sample_filter: 用于滤去在x个样本中序列数小于y的OTU
    reads_filter: 用于滤去序列数小于x的OTU
    """
    def __init__(self, parent):
        super(FilterOtuAgent, self).__init__(parent)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU文件
            {"name": "filter_json", "type": "string", "default": ""},  # 输入的json文件
            {"name": "out_otu_table", "type": "outfile", "format": "meta.otu.otu_table"}  # 输出的结果OTU表
        ]
        self.add_option(options)
        self.step.add_steps("filter_otu")

    def start_filter_otu(self):
        self.step.filter_otu.start()
        self.step.update()

    def end_filter_otu(self):
        self.step.filter_otu.end()
        self.step.update()

    def check_options(self):
        """
        参数检测
        """
        if not self.option("in_otu_table").is_set:
            raise OptionError("输入的OTU文件不能为空")
        if self.option("filter_json") == "":
            raise OptionError("输入的筛选json不能为空")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["out_otu.xls", "xls", "结果OTU表格"]
        ])
        super(FilterOtuAgent, self).end()

    def set_resource(self):
        """
        设置所需的资源
        """
        self._cpu = 2
        self._memory = "3G"


class FilterOtuTool(Tool):
    def __init__(self, config):
        super(FilterOtuTool, self).__init__(config)
        self.otu_sample_dict = self.option("in_otu_table").extract_info()
        self.json = list()
        self.otu_json = list()  # 将整个OTU表读入，生成一个列表，方便后面进行过滤操作
        with open(self.option("in_otu_table").prop["path"], 'rb') as r:
            self.otu_json = r.readlines()
        self.otu_head = self.otu_json.pop(0)  # OTU表的表头
        self.LEVEL = {
            1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
            6: "f__", 7: "g__", 8: "s__", 9: "otu"
        }
        self.keep_list = list()  # 处理物种筛选中的保留的逻辑

    def filter_table(self):
        my_json = json.loads(self.option("filter_json"))
        keep_flag = 0
        for d in my_json:
            if d["name"] == "species_filter" and d["type"] == "keep":
                self.keep_species(d)
                keep_flag = 1
        if keep_flag:
            self.otu_json = self.keep_list[:]
        for d in my_json:
            if d["name"] == "species_filter" and d["type"] == "remove":
                self.remove_species(d)
        for d in my_json:
            if d["name"] == "sample_filter":
                self.filter_samples(d)
            elif d["name"] == "reads_filter":
                self.filter_reads(d)

    def keep_species(self, my_json):
        j_value = re.sub("^\w__", "", my_json["value"])
        for line in self.otu_json:
            sp_name = line.split("\t")[0].split("; ")
            my_level = int(my_json["level_id"]) - 1
            """
            # 当级别是9的时候，也即是OTU的时候，进行精确匹配
            if int(my_json["level_id"]) == 9:
                if sp_name[my_level] == my_json["value"] or sp_name[my_level].lower() == my_json["value"]:
                    self.keep_list.append(line)
            # 当级别不是OTU的时候, 进行模糊匹配。
            else:
                pattern = self.LEVEL[int(my_json["level_id"])] + ".*" + j_value
                if re.search(pattern, sp_name[my_level], re.IGNORECASE):
                    self.keep_list.append(line)
            """
            str = my_json["value"]
            str = str.lstrip()
            if sp_name[my_level] == str or sp_name[my_level].lower() == str:
                self.keep_list.append(line)
            
    def remove_species(self, my_json):
        j_value = re.sub("^\w__", "", my_json["value"])
        tmp_list = self.otu_json[:]
        for line in self.otu_json:
            sp_name = line.split("\t")[0].split("; ")
            my_level = int(my_json["level_id"]) - 1
            """
            # 当级别是9的时候，也即是OTU的时候，进行精确匹配
            if int(my_json["level_id"]) == 9:
                if sp_name[my_level] == my_json["value"] or sp_name[my_level].lower() == my_json["value"]:
                    tmp_list.remove(line)
            # 当级别不是OTU的时候, 进行模糊匹配。
            else:
                pattern = self.LEVEL[int(my_json["level_id"])] + ".*" + j_value
                if re.search(pattern, sp_name[my_level], re.IGNORECASE):
                    tmp_list.remove(line)
            """
            # edited by sj
            str = my_json["value"]
            str = str.lstrip()
            if sp_name[my_level] == str or sp_name[my_level].lower() == str:
                tmp_list.remove(line)
        self.otu_json = tmp_list[:]

    def filter_samples(self, my_json):
        """
        保留至少在x个样本中序列数大于y的物种(OTU)
        """
        tmp_list = self.otu_json[:]
        my_c = 0
        for line in self.otu_json:
            otu = line.split("\t")[0]  # otu即是第一列，也是self.otu_sample_dict的第一个key值
            count = 0
            for sp in self.otu_sample_dict[otu]:
                if self.otu_sample_dict[otu][sp] >= int(my_json["reads_num"]):
                    count += 1
            if count < int(my_json["sample_num"]):
                tmp_list.remove(line)
            else:
                my_c += 1
        self.otu_json = tmp_list[:]

    def filter_reads(self, my_json):
        """
        保留序列数总和大于x的物种(OTU)
        """
        tmp_list = self.otu_json[:]
        for line in self.otu_json:
            otu = line.split("\t")[0]  # otu即是第一列，也是self.otu_sample_dict的第一个key值
            summary = 0
            for sp in self.otu_sample_dict[otu]:
                summary += self.otu_sample_dict[otu][sp]
            if summary < int(my_json["reads_num"]):
                tmp_list.remove(line)
        self.otu_json = tmp_list[:]

    def run(self):
        super(FilterOtuTool, self).run()
        self.filter_table()
        # if len(self.otu_json) == 0:
        #    raise Exception("过滤之后的结果OTU是空的, 请查看过滤的条件是否正确！")
        with open(os.path.join(self.output_dir, "filter_otu.xls"), "wb") as w:
            w.write(self.otu_head)
            for line in self.otu_json:
                w.write(line)
        self.option("out_otu_table").set_path(os.path.join(self.output_dir, "filter_otu.xls"))
        self.logger.info("OTU过滤完成")
        self.end()
