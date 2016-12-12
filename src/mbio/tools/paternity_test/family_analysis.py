## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "moli.zhou"
#last_modify:20161121

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os

class FamilyAnalysisAgent(Agent):
    """
    合并家族之后的一系列分析
    包括父权值、有效率、无效率、错配率等等
    version v1.0
    author: moli.zhou
    last_modify: 2016.11.21
    """
    def __init__(self, parent):
        super(FamilyAnalysisAgent, self).__init__(parent)
        options = [#输入的参数
            # {"name": "dad_tab", "type": "infile", "format": "tab"},
            # {"name": "tab_merged", "type": "outfile", "format": "Rdata"}

            {"name": "tab_merged", "type": "string"}, #format:Rdata
            {"name": "analysis_result", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("family_analysis")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.family_analysis.start()
        self.step.update()

    def stepfinish(self):
        self.step.family_analysis.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # if not self.option('query_amino'):
        #     raise OptionError("必须输入氨基酸序列")
        if not self.option('tab_merged') :
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
            ["family_analysis.Rdata", "Rdata", "父权值等计算"],
        ])
        super(FamilyAnalysisAgent, self).end()


class FamilyAnalysisTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(FamilyAnalysisTool, self).__init__(config)
        self._version = '1.0.1'

        self.R_path = 'program/R-3.3.1/bin/'
        self.script_path = Config().SOFTWARE_DIR+'/bioinfo/medical/scripts/'

    def run_tf(self):
        analysis_cmd = "{}Rscript {}data_analysis.R {}".\
            format(self.R_path,self.script_path,self.option("tab_merged"))
        self.logger.info(analysis_cmd)
        self.logger.info("开始运行家系的分析")
        cmd = self.add_command("analysis_cmd", analysis_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行家系分析成功")
        else:
            self.logger.info("运行家系分析出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        f = 'family_analysis.Rdata'
        os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
        f1 = 'family_analysis.txt'
        os.link(self.work_dir + '/' + f1, self.output_dir + '/' + f1)
        self.logger.info('设置文件夹路径成功')

    def run(self):
        super(FamilyAnalysisTool, self).run()
        self.run_tf()
        self.set_output()
        self.end()
