# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import shutil
import re


class GffcompareAgent(Agent):
    """
    有参转录组gffcompare比较
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2017.02.10
    """
    def __init__(self, parent):
        super(GffcompareAgent, self).__init__(parent)
        options = [
            {"name": "merged.gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 拼接合并之后的转录本文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "cuff.gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},
        ]
        self.add_option(options)
        self.step.add_steps("gffcompare")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.gffcompare.start()
        self.step.update()

    def stepfinish(self):
        self.step.gffcompare.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('merged.gtf'):
            raise OptionError('必须输入所有样本拼接合并后gtf文件')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "3G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["cuffcmp.annotated.gtf", "gtf", "gffcompare输出的相关文件"]
        ])
        super(GffcompareAgent, self).end()


class GffcompareTool(Tool):
    def __init__(self, config):
        super(GffcompareTool, self).__init__(config)
        self._version = "v0.9.8.linux_x86_64"
        self.gffcompare_path = 'bioinfo/rna/gffcompare-0.9.8.Linux_x86_64/'
        tmp = os.path.join(self.config.SOFTWARE_DIR, self.gffcompare_path)
        tmp1 = tmp + ":$PATH"
        self.logger.debug(tmp1)
        self.set_environ(PATH=tmp1)

    def run(self):
        """
        运行
        :return:
        """
        super(GffcompareTool, self).run()
        self.run_gffcompare()
        self.set_output()
        self.end()

    def run_gffcompare(self):
        """
        运行gffcompare软件进行新转录本预测
        """
        cmd = self.gffcompare_path + 'gffcompare  {} -o {}cuffcmp -r {} '.format(
            self.option('merged.gtf').prop['path'], self.work_dir+"/", self.option('ref_gtf').prop['path'])
        self.logger.info('运行gffcompare软件，进行比较')
        command = self.add_command("gffcompare_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("gffcompare运行完成")
        else:
            self.set_error("gffcompare运行出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        try:
            shutil.copy2(self.work_dir + "/cuffcmp.annotated.gtf", self.output_dir + "/cuffcmp.annotated.gtf")
            self.option('cuff.gtf').set_path(self.output_dir + "/cuffcmp.annotated.gtf")
            self.logger.info("设置拼接比较结果目录成功")
        except Exception as e:
            self.logger.info("设置拼接比较分析结果目录失败{}".format(e))
            self.set_error("设置拼接比较分析结果目录失败{}".format(e))
