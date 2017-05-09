## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re


class BedAnalysisAgent(Agent):
    """
    调用bed_analysis.r脚本，对nipt shell脚本生成的bed文件进行分析
    example: ~/app/program/R-3.3.1/bin/Rscript bed_analysis.R ./9.bed.2 /mnt/ilustre/users/sanger-dev/app/database/
    human/hg38_nipt/ref_cor/2.ref.cor.Rdata 10 1 nipt_output >nipt.r.Rout
    version v1.0
    author: hongdongxuan
    last_modify: 20170508
    """
    def __init__(self, parent):
        super(BedAnalysisAgent, self).__init__(parent)
        options = [
            {"name": "bed_file", "type": "infile", "format": "nipt.bed"},
            {"name": "bw", "type": "int", "default": 10},
            {"name": "bs", "type": "int", "default": 1},
            {"name": "ref_group", "type": "int", "default": 2}
        ]
        self.add_option(options)
        self.step.add_steps("bed_analysis")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.bed_analysis.start()
        self.step.update()

    def stepfinish(self):
        self.step.bed_analysis.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("bed_file").is_set:
            raise OptionError("必须输入bed_file文件！")
        if not self.option("bw") or not isinstance(self.option("bw"), int):
            raise OptionError("必须输入bw值,且bw必须为整数！")
        if not self.option("bs") or not isinstance(self.option("bs"), int):
            raise OptionError("必须输入bs值,且bs必须为整数！")
        if not self.option("ref_group") or not isinstance(self.option("ref_group"), int):
            raise OptionError("必须输入ref_group值,且ref_group必须为整数！")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '10G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["z.xls", "xls", "z值分析结果"],
            ["zz.xls", "xls", "统计zz值"]
        ])
        super(BedAnalysisAgent, self).end()


class BedAnalysisTool(Tool):
    """
    nipt bed文件分析tool
    """
    def __init__(self, config):
        super(BedAnalysisTool, self).__init__(config)
        self._version = '1.0.1'
        self.r_path = 'program/R-3.3.1/bin/Rscript'
        self.script_path = os.path.join(Config().SOFTWARE_DIR, "bioinfo/medical/scripts/")
        self.ref_cor_cn = Config().SOFTWARE_DIR + "/database/human/hg38_nipt/ref_cor/" + str(self.option("ref_group")) + ".ref.cor.Rdata"
        self.logger.info(self.ref_cor_cn)
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin')
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')

    def run_bed_analysis(self):
        one_cmd = self.r_path + " %sbed_analysis.R %s %s %s %s %s" % (self.script_path,
                                                                      self.option('bed_file').prop['path'],
                                                                      self.ref_cor_cn, self.option("bw"),
                                                                      self.option("bs"), 'nipt_output')
        self.logger.info(one_cmd)
        self.logger.info("开始运行one_cmd")
        cmd = self.add_command("one_cmd", one_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行one_cmd成功")
        else:
            self.set_error("运行one_cmd出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir + '/nipt_output/')
        for f in results:
            if re.search(r'.*Rdata$', f):
                pass
            else:
                os.link(self.work_dir + '/nipt_output/' + f, self.output_dir + '/' + f)
        self.logger.info('设置文件夹路径成功')


    def run(self):
        super(BedAnalysisTool, self).run()
        self.run_bed_analysis()
        self.set_output()
        self.end()
