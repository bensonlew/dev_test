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


class BoxPlotAgent(Agent):
    """
    version 1.0
    author: wangzhaoyue
    last_modify: 2017.04.25
    """
    def __init__(self, parent):
        super(BoxPlotAgent, self).__init__(parent)
        options = [
            {"name": "input_table", "type": "infile", "format": "toolapps.table"},  # 输入的表格，矩阵
            {"name": "method", "type": "string", "default": "row"}  # 统计数据的方向，row,column
        ]
        self.add_option(options)
        self.step.add_steps('box_plot')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.box_plot.start()
        self.step.update()

    def step_end(self):
        self.step.box_plot.finish()
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
        self._cpu = 1
        self._memory = '2G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "箱线图结果目录"],
            # ["./final_value.xls", "xls", "结果表"],
        ])
        super(BoxPlotAgent, self).end()


class BoxPlotTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(BoxPlotTool, self).__init__(config)
        self._version = 1.0

    def create_common_table(self):
        """
        输入的文件统一处理成标准格式的文件,第一列为样本名
        """
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
                first_line = "samples"
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

    def box_plot_run(self):
        # box_dic = {}
        box_data_file = self.work_dir + "/box_data.xls"
        with open(self.work_dir + "/matrix_table.xls")as fr, open(box_data_file, "w")as fw:
            fw.write("ID\tmin\tq1\tq2\tq3\tmax\toutliers\n")
            fr.next()
            for line in fr:
                line_split = line.strip().split("\t")
                change_type_list = []
                for i in line_split[1:]:
                    change_type_list.append(float(i))
                box_data = self.get_box_data(change_type_list)
                new_line = line_split[0]
                for i in box_data[0:-1]:
                    new_line = new_line + '\t' + str(i)
                if len(box_data[-1]) == 0:
                    new_line = new_line + '\n'
                elif len(box_data[-1]) == 1:
                    filter = str(box_data[-1][0])
                    new_line = new_line + '\t' + filter + '\n'
                else:
                    out_list = []
                    for i in box_data[-1]:
                        out_list.append(str(i))
                    filter = ','.join(out_list)
                    new_line = new_line + '\t' + filter + '\n'
                fw.write(new_line)
        self.set_output()

    def get_box_data(self, mylist):
        """
        对列表中的数据进行箱线图数据统计
        :param mylist: 输入的列表
        :return: 输出的列表，[min,1/4,med,3/4,max,[异常值]]
        """
        mylist = sorted(mylist)
        length = len(mylist)
        filter_list = []
        out = []
        if length > 3:
            q1_index = 0.25 * (length + 1)
            q2_index = 0.5 * (length + 1)
            q3_index = 0.75 * (length + 1)
            q1_index_int = int(q1_index)
            q2_index_int = int(q2_index)
            q3_index_int = int(q3_index)
            q1 = mylist[q1_index_int - 1] + (mylist[q1_index_int] - mylist[q1_index_int - 1]) * (
            q1_index - q1_index_int)
            q3 = mylist[q3_index_int - 1] + (mylist[q3_index_int] - mylist[q3_index_int - 1]) * (
            q3_index - q3_index_int)
            q2 = mylist[q2_index_int - 1] + (mylist[q2_index_int] - mylist[q2_index_int - 1]) * (
            q2_index - q2_index_int)
            qd = q3 - q1
            max_limit = 1.5 * qd + q3
            min_limit = q1 - 1.5 * qd
            new_list = []
            for i in mylist:
                if i >= min_limit and i <= max_limit:
                    new_list.append(i)
                else:
                    filter_list.append(i)
            max_box = new_list[-1]
            min_box = new_list[0]
        elif (length == 3):
            max_box = mylist[2]
            min_box = mylist[0]
            q3 = float(mylist[1] + mylist[2]) / 2
            q2 = mylist[1]
            q1 = float(mylist[1] + mylist[0]) / 2
        elif (length == 2):
            max_box = mylist[1]
            min_box = mylist[0]
            q2 = float(mylist[1] + mylist[0]) / 2
            q3 = (mylist[1] + q2) / 2
            q1 = (q2 + mylist[0]) / 2
        elif (length == 1):
            max_box = mylist[0]
            min_box = mylist[0]
            q2 = mylist[0]
            q3 = mylist[0]
            q1 = mylist[0]
        out.append(min_box)
        out.append(q1)
        out.append(q2)
        out.append(q3)
        out.append(max_box)
        out.append(filter_list)
        self.logger.info(out)
        return out


    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        shutil.copy2(self.work_dir + "/box_data.xls", self.output_dir + "/box_data.xls")
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(BoxPlotTool, self).run()
        self.create_common_table()
        self.box_plot_run()
        self.end()
