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


class SimplePieAgent(Agent):
    """
    version 1.0
    author: wangzhaoyue
    last_modify: 2017.04.25
    """
    def __init__(self, parent):
        super(SimplePieAgent, self).__init__(parent)
        options = [
            {"name": "input_table", "type": "infile", "format": "toolapps.table"},   # 输入的表格，矩阵
            {"name": "method", "type": "string", "default": "row"},  # 样本名的方向，默认样本在行row,column
            {"name": "combined_value", "type": "float", "default": 0.01}  # 合并小于此值的属性
        ]
        self.add_option(options)
        self.step.add_steps('simple_pie')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.simple_pie.start()
        self.step.update()

    def step_end(self):
        self.step.simple_pie.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("input_table"):
            raise OptionError("参数input_table不能为空")
        if self.option('group_table').is_set:
            if self.option('method') == 'column':
                for i in self.option('group_table').prop['sample_name']:
                    if i not in self.option('input_table').prop['row_sample']:
                        raise Exception('分组文件中的样本{}不存在于表格第一列中，查看是否是数据取值选择错误'.format(i))
            else:
                for i in self.option('group_table').prop['sample_name']:
                    if i not in self.option('input_table').prop['col_sample']:
                        raise Exception('分组文件中的样本{}不存在于表格第一行中，查看是否是数据取值选择错误'.format(i))

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1
        self._memory = '1G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "饼图结果目录"],
            ["./final_value.xls", "xls", "结果表"],
        ])
        super(SimplePieAgent, self).end()


class SimplePieTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(SimplePieTool, self).__init__(config)
        self._version = 1.0

    def create_common_table(self):
        """
        输入的文件统一处理成标准格式的文件,第一列为样本名
        """
        # os.system('dos2unix -c Mac {}'.format(self.option('input_table').prop['new_table']))
        fianl_txt = self.work_dir + "/matrix_table.xls"
        if self.option("method") == "row":
            dic = defaultdict(list)
            with open(self.option("input_table").prop["new_table"], "r") as r, open(fianl_txt, "w+")as fw:
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
            with open(self.option("input_table").prop["new_table"], "r") as r, open(fianl_txt, "w+")as fw:
                lines = r.readlines()
                lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                for line in lines:
                    fw.write(line)
        combined_txt = self.work_dir + "/final_table.xls"
        value_table = self.work_dir + "/final_value.xls"
        with open(fianl_txt) as fr, open(combined_txt, "w+")as fw,  open(value_table, "w+")as fw2:
            lines = fr.readlines()
            lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
            lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
            first_line = lines[0].strip().split("\t")  # 物种名称列表
            # print first_line
            dic_origin = defaultdict(list)
            samples = []
            for line in lines[1:]:
                line_split = line.strip().split("\t")
                samples.append(line_split[0])  # 按顺序存放样本名称
                int_value = []
                for i in line_split[1:]:
                    int_value.append(float(i))
                dic_origin[line_split[0]] = int_value
            # print dic_origin
            dic_percent = defaultdict(list)
            for key in dic_origin.keys():
                percent_list = []
                for percent_value in dic_origin[key]:
                    if sum(dic_origin[key]) == 0:
                        self.set_error("{}对应的这组信息全为0，不能画图，请剔除，再做分析".format(key))
                    else:
                        per = "%10f" % (percent_value / sum(dic_origin[key]))
                        percent_list.append(float(per))
                dic_percent[key] = percent_list
            # print dic_percent  # 百分比列表产生
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
            # print index
            new_names = []
            if len(index) != 0:  # 不为0，说明有需要合并的列
                new_dic = defaultdict(list)   # 处理之后的百分比表格
                for key in dic_percent.keys():
                    value = []
                    other = 0
                    for i in range(len(first_line) - 1):
                        if i in index:
                            other += float(dic_percent[key][i])
                        else:
                            value.append(float(dic_percent[key][i]))
                    value.append(other)
                    new_dic[key] = value
                for i in range(len(first_line) - 1):
                    if i not in index:
                        new_names.append(first_line[i + 1])
                    else:
                        pass
                new_names.append("others")
                new_value_dic = defaultdict(list)  # 处理之后的数值表格
                for key in dic_origin.keys():
                    value = []
                    other = 0
                    for i in range(len(first_line) - 1):
                        if i in index:
                            other += float(dic_origin[key][i])
                        else:
                            value.append(float(dic_origin[key][i]))
                    value.append(other)
                    new_value_dic[key] = value
            else:
                new_names = first_line[1:]
                new_dic = dic_percent
                new_value_dic = dic_origin
            # print new_names
            # print new_dic
            # print new_value_dic
            new_first_line = "ID"
            for i in new_names:
                new_first_line = new_first_line + "\t" + str(i)

            print new_first_line
            fw.write(new_first_line + "\n")
            fw2.write(new_first_line + "\n")
            for j in samples:
                new_lines = j
                new_value_lines = j
                for value in new_dic[j]:
                    new_lines += "\t" + str(value)
                fw.write(new_lines + "\n")
                for value2 in new_value_dic[j]:
                    new_value_lines += "\t" + str(value2)
                fw2.write(new_value_lines + "\n")
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        shutil.copy2(self.work_dir + '/final_value.xls', self.output_dir + '/final_value.xls')
        shutil.copy2(self.work_dir + '/final_table.xls', self.output_dir + '/matrix_pie.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(SimplePieTool, self).run()
        self.create_common_table()
        self.end()
