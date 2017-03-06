# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import shutil
import linecache
import numpy as np
from collections import defaultdict
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class SortSamplesAgent(Agent):
    """
    传入一个group表，以及是否进行样本合并的参数生成一张OTU表并对并依照group表OTU表进行筛选合并
    """
    def __init__(self, parent):
        super(SortSamplesAgent, self).__init__(parent)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU文件
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表
            {"name": "method", "type": "string", "default": ""},  # 样本的合并方式, ""为不进行合并
            {"name": "out_otu_table", "type": "outfile", "format": "met a.otu.otu_table"}  # 输出的结果OTU表
        ]
        self.add_option(options)
        self.step.add_steps("sort_samples")
        self.on('start', self.start_sort_samples)
        self.on('end', self.end_sort_samples)

    def start_sort_samples(self):
        self.step.sort_samples.start()
        self.step.update()

    def end_sort_samples(self):
        self.step.sort_samples.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        """
        if not self.option("in_otu_table").is_set:
            raise OptionError("输入的OTU文件不能为空")
        if self.option("method"):
            if self.option("method") not in ["", "no", "none", "No", "None", None, "average", "sum", "middle"]: # add middle by zhouxuan 20161205
                raise OptionError("参数method设置错误！")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["out_otu.xls", "xls", "结果OTU表格"],
            # ["level_percents.xls", "xls", "结果OTU表格"]
        ])
        super(SortSamplesAgent, self).end()

    def set_resource(self):
        """
        设置所需的资源
        """
        self._cpu = 2
        self._memory = "3G"


class SortSamplesTool(Tool):
    def __init__(self, config):
        super(SortSamplesTool, self).__init__(config)
        samples = list()
        with open(self.option("group_table").prop["path"], "rb") as r:
            line = r.next()
            for line in r:
                line = line.rstrip().split("\t")
                samples.append(line[0])
        self.samples = samples

    def filter_samples(self):
        no_zero_otu = os.path.join(self.work_dir, "otu.nozero")
        self.option("in_otu_table").sub_otu_sample(self.samples, no_zero_otu)
        return no_zero_otu

    def cat_samples(self, otu, method):
        sample_group = dict()  # 一个样本是属于哪个group的
        index_sample = dict()  # 一个OTU表中第几列属于哪个样本
        group_sample_num = defaultdict(int)  # 一个分组里面有多少的样本
        cat_otu_path = os.path.join(self.work_dir, "cat_otu.xls")
        with open(self.option("group_table").prop["path"], "rb") as r:
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
                # l = len(group_list) #zx

            line = r.next().rstrip().split("\t")
            for i in range(len(line)):
                index_sample[i] = line[i]

            w.write(index_sample[0] + "\t")
            w.write("\t".join(group_list) + "\n")
            for line in r:
                line = line.rstrip().split("\t")
                num = defaultdict(int)
                middle_num = defaultdict(int) #
                tmp = list()
                list1 = [] # add 2 lines by zhouxuan 20161205
                mid_num = dict()

                w.write(line[0] + "\t")
                for i in range(1, len(line)):
                    num[sample_group[index_sample[i]]] += int(line[i])
                for m in group_list: # add 12 lines by zhouxuan 20161205
                    for i in range(1, len(line)):
                        if sample_group[index_sample[i]] == m:
                            list1.append(int(line[i]))
                            if len(list1) == group_sample_num[m]:
                                list1.sort()
                                yu = int(group_sample_num[m]) % 2
                                index = int(int(group_sample_num[m]) / 2)
                                if yu == 0:
                                    mid_num[m] = int(round((int(list1[index-1]) + int(list1[index]))/2))
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
                if method == "middle": # add 3 line by zhouxuan 20161205
                    for g in group_list:
                        tmp.append(str(mid_num[g]))
                """
                else :
                    if method == "average":
                        for g in group_list:
                            avg = int(round(num[g] / group_sample_num[g]))
                            tmp.append(str(avg))
                    else method == "middle": # add 6 line by zhouxuan 20161205
                        for g in group_list:
                            list1.sort()
                            index = int(round(len(line)-1 / 2))
                            middle_num = list1[index]
                            tmp.append(str(middle_num))
                """

                w.write("\t".join(tmp))
                w.write("\n")
        return cat_otu_path

    def percent(self, origin_file, percent_file):
        self.logger.info("开始运行百分比")
        tmp = np.loadtxt(origin_file, dtype=np.str, delimiter="\t")
        data = tmp[1:, 1:].astype(np.float)
        array_data = data / data.sum(0)
        array = np.c_[tmp[1:, 0], array_data]
        array_all = np.row_stack((tmp[0, :], array))
        s = np.char.encode(array_all, 'utf-8')
        np.savetxt(percent_file, s, fmt='%s',  delimiter='\t', newline='\n')

    def run(self):
        super(SortSamplesTool, self).run()
        final_otu = self.filter_samples()
        self.logger.info('filter success')
        if self.option("method") in ["average", "sum", "middle"]:
            final_otu = self.cat_samples(final_otu, self.option("method"))
        self.logger.info("111")
        out_otu = os.path.join(self.output_dir, "out_otu.xls")
        shutil.copy2(final_otu, out_otu)
        final_level_percents = os.path.join(self.output_dir, "level_percents.xls")
        self.logger.info("222")
        self.percent(out_otu, final_level_percents)
        self.option("out_otu_table").set_path(out_otu)
        self.end()
