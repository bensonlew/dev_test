# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import os
import shutil
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.core.exceptions import FileError
import re
from collections import defaultdict


class SimpleBarAgent(Agent):
    """
    version 1.0
    author: wangzhaoyue
    last_modify: 2017.04.25
    """
    def __init__(self, parent):
        super(SimpleBarAgent, self).__init__(parent)
        options = [
            {"name": "input_table", "type": "infile", "format": "meta.otu.otu_table"},
            # 输入的表格，矩阵
            {"name": "show_type", "type": "string", "default": "bar"},  # 展示的图片类型，bar or pie
            {"name": "file_type", "type": "string", "default": "matrix"},  # 输入文件的类型，矩阵matrix,列表list
            {"name": "method", "type": "string", "default": "row"},  # 样本名的方向，默认样本在行row,column
            {"name": "combined_value", "type":"string", "default": "0.01"}  # 合并小于此值的属性
        ]
        self.add_option(options)
        self.step.add_steps('simple_bar')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.simple_bar.start()
        self.step.update()

    def step_end(self):
        self.step.simple_bar.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("input_table"):
            raise OptionError("参数input_table不能为空")
    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class SimpleBarTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(SimpleBarTool, self).__init__(config)
        self._version = 1.0

    def create_common_table(self):
        """
        输入的文件统一处理成标准格式的文件,第一列为样本名
        """
        if self.option("file_type") == "matrix":
            fianl_txt = self.work_dir + "/matrix_table.xls"
            if self.option("method") == "row":
                dic = defaultdict(list)
                with open(self.option("input_table").prop["path"], "r") as r, open(fianl_txt, "w+")as fw:
                    lines = r.readlines()
                    lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                    lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                    names = lines[0].strip().split("\t")[1:]
                    group = []
                    for line in lines[1:]:
                        line_split = line.strip().split("\t")
                        group.append(line_split[0])
                        for i in range(len(names)):
                            dic[names[i]].append(line_split[i+1])
                    first_line = "samples\features"
                    for i in group:
                        first_line = first_line + "\t" + str(i)
                    fw.write(first_line + "\n")
                    for key in dic.keys():
                        new_line = key
                        for value in dic[key]:
                            new_line = new_line + '\t' + value
                        fw.write(new_line + "\n")
            if self.option("method") == "column":
                with open(self.option("input_table").prop["path"], "r") as r, open(fianl_txt, "w+")as fw:
                    lines = r.readlines()
                    lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                    lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                    for line in lines:
                        fw.write(line)
            combined_txt = self.work_dir + "/final_table.xls"
            with open(fianl_txt) as fr, open(combined_txt, "w+")as fw:
                lines = fr.readlines()
                lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                first_line = lines[0].strip().split("\t")  # 物种名称列表
                print first_line
                dic_origin = defaultdict(list)
                samples = []
                for line in lines[1:]:
                    line_split = line.strip().split("\t")
                    samples.append(line_split[0])  # 按顺序存放样本名称
                    int_value = []
                    for i in line_split[1:]:
                        int_value.append(float(i))
                    dic_origin[line_split[0]] = int_value
                print dic_origin
                dic_percent = defaultdict(list)
                for key in dic_origin.keys():
                    percent_list = []
                    for percent_value in dic_origin[key]:
                        per = "%10f" % (percent_value / sum(dic_origin[key]))
                        percent_list.append(float(per))
                    dic_percent[key] = percent_list
                print dic_percent  # 百分比列表产生
                dic_compare = defaultdict(list)
                for i in range(len(first_line) - 1):
                    for key in dic_percent.keys():
                        dic_compare[i].append(dic_percent[key][i])
                print dic_compare
                index = []
                for key in dic_compare.keys():
                    put_or_not = []
                    for per in dic_compare[key]:
                        if float(per) > float(self.option("combined_value")):
                            put_or_not.append(per)
                        else:
                            pass
                    if len(put_or_not) == 0:
                        index.append(int(key))
                print index
                new_names = []
                if len(index) != 0:  # 不为0，说明有需要合并的列
                    new_dic = defaultdict(list)
                    for key in dic_origin.keys():
                        value = []
                        other = 0
                        for i in range(len(first_line) - 1):
                            if i in index:
                                other += float(dic_origin[key][i])
                            else:
                                value.append(float(dic_origin[key][i]))
                        value.append(other)
                        new_dic[key] = value
                    for i in range(len(first_line) - 1):
                        if i not in index:
                            new_names.append(first_line[i + 1])
                        else:
                            pass
                    new_names.append("others")
                else:
                    new_names = first_line[1:]
                    new_dic = dic_origin
                print new_names
                print new_dic
                new_first_line = "samples\\features"
                for i in new_names:
                    new_first_line = new_first_line + "\t" + str(i)
                fw.write(new_first_line + "\n")
                for j in samples:
                    new_lines = j
                    for value in new_dic[j]:
                        new_lines += "\t" + str(value)
                    fw.write(new_lines + "\n")
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        if self.option("file_type") == "matrix":
            if self.option("show_type") == "bar":
                shutil.copy2(self.work_dir + '/final_table.xls', self.output_dir + '/matrix_bar.xls')
            else:
                shutil.copy2(self.work_dir + '/final_table.xls', self.output_dir + '/matrix_pie.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(SimpleBarTool, self).run()
        self.create_common_table()
        self.end()
