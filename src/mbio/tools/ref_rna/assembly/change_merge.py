# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import shutil
import re


class ChangeMergeAgent(Agent):
    """
    有参转录组merged.gtf换ID
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.13
    """
    def __init__(self, parent):
        super(ChangeMergeAgent, self).__init__(parent)
        options = [
            {"name": "merged.gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},  # merge后的gtf文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "change_merged.gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},  # 换过ID的merged.gtf
            {"name": "mothod", "type": "string", "default": "cufflinks"},  # 拼接的软件
        ]
        self.add_option(options)
        self.step.add_steps("change_merge")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.change_merge.start()
        self.step.update()

    def stepfinish(self):
        self.step.change_merge.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('merged.gtf'):
            raise OptionError('必须输入合并后的gtf文件')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "1G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["change_merged.gtf", "gtf", "转换ID后的合并的gtf文件"]
        ])
        super(ChangeMergeAgent, self).end()


class ChangeMergeTool(Tool):
    def __init__(self, config):
        super(ChangeMergeTool, self).__init__(config)
        self._version = "v1.0.1"
        self.Python_path = 'program/Python/bin/python '
        self.change_gtf_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/scripts/new_trans.py'
        self.script_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/scripts/trans_script.py'
        tmp = os.path.join(self.config.SOFTWARE_DIR, self.Python_path)
        tmp_new = tmp + ":$PATH"
        self.logger.debug(tmp_new)
        self.set_environ(PATH=tmp_new)

    def run(self):
        """
        运行
        :return:
        """
        super(ChangeMergeTool, self).run()
        self.run_change_merge_gtf()
        self.set_output()
        self.end()

    def run_change_merge_gtf(self):
        """
        运行python脚本，调用package"trans_script.py",转换merged.gtf
        """
        new_merged = self.output_dir + "/merged.gtf"
        cmd = self.Python_path + self.change_gtf_path + " -s %s -marge %s -out_merge %s -ref_gtf %s -method %s" % (
                  self.script_path, self.option('merged.gtf').prop['path'], new_merged, self.option('ref_gtf').prop['path'], self.option('mothod'))
        self.logger.info('运行python，转换merged.gtf的ID')
        command = self.add_command("change_merge_gtf_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("merged.gtf转换完成")
        else:
            self.set_error("merged.gtf转换出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        try:
            # self.option('merged.gtf').set_path(self.work_dir + "/merged.gtf")
            self.logger.info("设置拼接合并分析结果目录成功")

        except Exception as e:
            self.logger.info("设置拼接合并分析结果目录失败{}".format(e))
            self.set_error("设置拼接合并分析结果目录失败{}".format(e))
