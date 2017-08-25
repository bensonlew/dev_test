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
            {"name": "input_table", "type": "infile", "format": "toolapps.table"},  # 输入的表格，矩阵
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table"},  # 输入的group表格
            {"name": "method", "type": "string", "default": "row"},  # 样本名的方向，默认样本在行row,column
            {"name": "combined_value", "type": "string", "default": "0.01"},  # 合并小于此值的属性
            {"name": "calculation", "type": "string", "default": "none"}  # 组内合并参数，none,sum,average,middle
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
        self._cpu = 1
        self._memory = '1G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "柱形图结果目录"],
            ["./final_value.xls", "xls", "结果表"],
        ])
        super(SimpleBarAgent, self).end()


class SimpleBarTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(SimpleBarTool, self).__init__(config)
        self._version = 1.0
        self.Python_path = 'program/Python/bin/python '
        self.path = self.config.SOFTWARE_DIR + '/bioinfo/statistical/scripts/simple_operation.py'

    def create_common_table(self):
        """
        输入的文件统一处理成标准格式的文件,第一列为样本名
        """
        input_table = self.option("input_table").prop['new_table']
        middle_input = self.work_dir + "/middle_input.xls"  # 样本在行
        final_input = self.work_dir + "/final_input.xls"  # 样本在列，方便计算
        combined_txt = self.work_dir + "/final_table.xls"
        value_table = self.work_dir + "/final_value.xls"
        if self.option("group_table").is_set:
            group_file = self.option("group_table").prop['new_table']
            if self.option("calculation") == "none":
                cmd = self.Python_path + self.path + " -i %s -method %s -i1 %s -i2 %s -o1 %s -o2 %s -combined %s" % (
                    input_table, self.option('method'), middle_input, final_input, combined_txt, value_table, self.option("combined_value"))
            else:
                cmd = self.Python_path + self.path + " -i %s -method %s -i1 %s -i2 %s -o1 %s -o2 %s -combined %s -group %s -calculation %s" % (
                    input_table, self.option('method'), middle_input, final_input, combined_txt, value_table,self.option("combined_value"),
                    group_file, self.option("calculation"))
        else:
            cmd = self.Python_path + self.path + " -i %s -method %s -i1 %s -i2 %s -o1 %s -o2 %s -combined %s" % (
                input_table, self.option('method'), middle_input, final_input, combined_txt, value_table, self.option("combined_value"))

        self.logger.info('运行python脚本，进行计算')
        command = self.add_command("bar", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("python运行完成")
        else:
            self.set_error("python运行出错!")

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        shutil.copy2(self.work_dir + '/final_value.xls', self.output_dir + '/final_value.xls')   # 丰度表格
        shutil.copy2(self.work_dir + '/final_table.xls', self.output_dir + '/matrix_bar.xls')    # 百分比表格
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(SimpleBarTool, self).run()
        self.create_common_table()
        self.set_output()
        self.end()
