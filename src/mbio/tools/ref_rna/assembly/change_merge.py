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
            {"name": "merged.gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # merge后的gtf文件
            {"name": "annotated.gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # gffcompare 后的annotated.gtf
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "change_merged.gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},  # 换过ID的merged.gtf
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
        self.gffread_path = 'bioinfo/rna/cufflinks-2.2.1/'
        self.change_gtf_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/scripts/main_merge_id_modify.py'
        self.script_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/scripts/change_id.py '
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
        self.run_gtf_to_fa()
        self.set_output()
        self.end()

    def run_change_merge_gtf(self):
        """
        运行python脚本，调用package"trans_script.py",转换merged.gtf
        """
        new_merged = self.work_dir + "/change_merged.gtf"
        cmd = self.Python_path + self.change_gtf_path + " -s %s -merge %s -out_merge %s -ref_gtf %s -tmp change_merge -combined %s -batch_no 5" % (
                  self.script_path, self.option('merged.gtf').prop['path'], new_merged, self.option('ref_gtf').prop['path'], self.option('annotated.gtf').prop['path'])
        self.logger.info('运行python，转换merged.gtf的ID')
        command = self.add_command("change_merge_gtf_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("merged.gtf转换ID完成")
        else:
            self.set_error("merged.gtf转换ID出错!")

    def run_gtf_to_fa(self):
        """
        运行gtf_to_fasta，转录本gtf文件转fa文件
        """
        cmd = self.gffread_path + "gffread %s -g %s -w change_merged.fa" % (
            self.work_dir + "/" + "change_merged.gtf", self.option('ref_fa').prop['path'])
        self.logger.info('运行gtf_to_fasta，形成fasta文件')
        command = self.add_command("gtf_to_fa_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("gtf_to_fasta运行完成")
        else:
            self.set_error("gtf_to_fasta运行出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        try:
            shutil.copy2(self.work_dir + "/change_merged.gtf", self.output_dir + "/change_merged.gtf")
            self.option('change_merged.gtf').set_path(self.work_dir + "/change_merged.gtf")
            shutil.copy2(self.work_dir + "/change_merged.fa", self.output_dir + "/change_merged.fa")
            self.logger.info("设置拼接合并分析结果目录成功")

        except Exception as e:
            self.logger.info("设置拼接合并分析结果目录失败{}".format(e))
            self.set_error("设置拼接合并分析结果目录失败{}".format(e))
