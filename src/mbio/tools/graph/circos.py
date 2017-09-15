# -*- coding: utf-8 -*-
# __author__ = zengjing
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import pandas as pd


class CircosAgent(Agent):
    """
    小工具弦图：对二维表格(行为样本)进行合并
    """
    def __init__(self, parent):
        super(CircosAgent, self).__init__(parent)
        options = [
            {"name": "data_table", "type": "infile", "format": "toolapps.table"},
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table"},
            {"name": "merge_value", "type": "float"}  #合并小于此数值的区域的值
        ]
        self.add_option(options)
        self.step.add_steps("circos")

    def check_options(self):
        if not self.option("data_table").is_set:
            raise OptionError("缺少输入的数据表格")

    def set_resource(self):
        self._cpu = 10
        self._memory = "10G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "Circos结果目录"]
        ])


class CircosTool(Tool):
    def __init__(self, config):
        super(CircosTool, self).__init__(config)
        self._version = 1.0

    def get_group_detail(self):
        """
        根据分组文件得到具体的分组方案
        """
        group_samples = {}  # 分组对应中新样本对应的旧样本
        with open(self.option('group_table').prop['path'], "r") as f:
            line = f.readline().rstrip()
            line = re.split("\t", line)
            if line[1] == "##empty_group##":
                is_empty = True
            else:
                is_empty = False
            for i in range(1, len(line)):
                group_samples[line[i]] = {}
            for item in f:
                item = item.rstrip().split("\t")
                for i in range(1, len(line)):
                    try:
                        if item[i] and item[i] not in group_samples[line[i]]:
                            group_samples[line[i]][item[i]] = []
                            group_samples[line[i]][item[i]].append(item[0])
                        elif item[i]:
                            group_samples[line[i]][item[i]].append(item[0])
                        else:
                            self.logger.info("{}样本不在分组方案{}内".format(item[0], line[i]))
                    except:
                        self.logger.info("{}样本不在分组方案{}内".format(item[0], line[i]))
        return group_samples

    def get_group_data_table(self):
        if self.option('group_table').is_set and self.option("group_method") in ["average", "sum", "median"]:
            group_samples = self.get_group_detail()
            self.logger.info(group_samples)
            with open(self.new_data, "r") as f:
                lines = f.readlines()
                line = lines[0].rstrip().split("\t")
                for group in group_samples:
                    new_samples = list(group_samples[group].keys())
                    new_sample_index = {}
                    for s in new_samples:
                        old_samples = group_samples[group][s]
                        new_sample_index[s] = []
                        for o in old_samples:
                            for i in range(len(line)):
                                if o == line[i]:
                                    new_sample_index[s].append(i)
                    table_path = os.path.join(self.work_dir, group + "_table.xls")
                    with open(table_path, "w") as w:
                        header = line[0] + "\t" + '\t'.join(new_samples) + "\n"
                        w.write(header)
                        for item in lines[1:]:
                            item = item.rstrip().split("\t")
                            w.write(item[0] + "\t")
                            tmp = []
                            for s in new_samples:
                                summary = 0
                                for i in new_sample_index[s]:
                                    summary += int(item[i])
                                tmp.append(str(summary))
                            w.write('\t'.join(tmp) + "\n")


    def data_table_stat(self):
        fp = self.option("data_table").prop["path"]
        data = pd.read_table(fp, header=0)
        self.
        with open(fp, "r") as f, open("new_data.xls", "w") as w:
            lines = f.readlines()
            header = lines[0]
            w.write("#name\tall\n")
            for line in lines[1:]:
                item = line.strip().split("\t")
                new_line = []
                w.write(item[0] + "\t")
                sum = 0
                for i in range(1, len(item)):
                    sum += item[i]

    def merge_value(fp, combined_value, out):
        """
        对
        """
        data = pd.read_table(fp, header=0)
        col_sum = data.sum(axis=1)
        col_value = col_sum.values
        row_sum = data.T.iloc[1:].sum(axis=1)
        row_value = row_sum.values
        name = data.T.iloc[0].to_frame(name="name")
        with open(fp, "r") as f, open(out, "w") as w:
            lines = f.readlines()
            w.write(lines[0].strip() + "\t" + "sum" + "\n")
            others = {}
            samples = lines[0].strip().split("\t")
            for s in samples[1:]:
                others[s] = 0
            other = False
            for line in lines[1:]:
                line = line.strip().split("\t")
                flag = False
                for i in range(1, len(line)):
                    percent = float(line[i]) / float(row_value[i-1])
                    if percent > combined_value:
                        flag = True
                if flag:
                    w.write('\t'.join(line) + "\t" + str(row_value[i-1]) + "\n")
                else:
                    other = True
                    for i in range(1, len(line)):
                        for j in range(1, len(samples)):
                            if i == j:
                                others[samples[j]] += float(line[i])
            if other:
                other_value = []
                other_sum = 0
                for s in samples[1:]:
                    other_sum += others[s]
                    other_value.append(str(others[s]))
                w.write("others" + "\t" + '\t'.join(other_value) + "\t" + str(other_sum) + "\n")
