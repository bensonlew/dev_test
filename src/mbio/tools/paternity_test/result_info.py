## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20161123

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re


class ResultInfoAgent(Agent):
    """
    亲子鉴定的结果输出
    包含家系图，存入报告的图、胎儿浓度等
    version v1.0
    author: moli.zhou
    last_modify: 2016.11.21
    """
    def __init__(self, parent):
        super(ResultInfoAgent, self).__init__(parent)
        options = [#输入的参数
            # {"name": "dad_tab", "type": "infile", "format": "tab"},
            # {"name": "tab_merged", "type": "outfile", "format": "Rdata"}

            {"name": "tab_merged", "type": "string"}, #format:Rdata
            # {"name": "family.png", "type": "string"},
            # {"name": "fig1.png", "type": "string"},
            # {"name": "fig2.png", "type": "string"},
            # {"name": "preg_percent.png", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("result_info")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.result_info.start()
        self.step.update()

    def stepfinish(self):
        self.step.result_info.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # if not self.option('query_amino'):
        #     raise OptionError("必须输入氨基酸序列")
        if not self.option('tab_merged'):
            raise OptionError("必须提供合并之后的家系表")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            ["family.png", "png", "家系图"],
        ])
        super(ResultInfoAgent, self).end()


class ResultInfoTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(ResultInfoTool, self).__init__(config)
        self._version = '1.0.1'
        # self.python_path = Config().SOFTWARE_DIR + '/program/Python/'

        self.R_path = 'program/R-3.3.1/bin/'
        self.script_path = Config().SOFTWARE_DIR + '/bioinfo/medical/scripts/'

    def run_tf(self):
        plot_cmd = "{}Rscript {}plot.R {}".\
            format(self.R_path,self.script_path,self.option("tab_merged"))
        self.logger.info(plot_cmd)
        self.logger.info("开始运行结果信息图的绘制")
        cmd = self.add_command("plot_cmd", plot_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行绘制结果图成功")
        else:
            self.logger.info("运行绘制结果图出错")

        convert_cmd = "bioinfo/medical/scripts/convert2png.sh {}".format(self.work_dir)
        self.logger.info(convert_cmd)
        self.logger.info("开始运行结果图的转化")
        cmd = self.add_command("convert_cmd", convert_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行转化结果图成功")
        else:
            self.logger.info("运行转化结果图出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)
        for f in results:
            if re.search(r'.*.png$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            if re.search(r'.*info_show\.txt$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            if re.search(r'.*test_pos\.txt$', f):
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
        self.logger.info('设置文件夹路径成功')

    def run(self):
        super(ResultInfoTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()
