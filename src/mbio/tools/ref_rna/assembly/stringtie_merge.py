# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import shutil
import re


class StringtieMergeAgent(Agent):
    """
    有参转录组stringtie合并
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.13
    """
    def __init__(self, parent):
        super(StringtieMergeAgent, self).__init__(parent)
        options = [
            {"name": "assembly_GTF_list.txt", "type": "infile", "format": "ref_rna.assembly.merge_txt"},
            # 所有样本比对之后的bam文件路径列表
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数
            {"name": "merged.gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},  # 输出的合并文件
        ]
        self.add_option(options)
        self.step.add_steps("stringtie_merge")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.stringtie_merge.start()
        self.step.update()

    def stepfinish(self):
        self.step.stringtie_merge.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('assembly_GTF_list.txt'):
            raise OptionError('必须输入所有样本gtf路径文件为txt格式')
        if not self.option('ref_fa'):
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('ref_gtf'):
            raise OptionError('必须输入参考序列ref.gtf')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "10G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["merged.gtf", "gtf", "样本合并之后的gtf文件"]
        ])
        super(StringtieMergeAgent, self).end()


class StringtieMergeTool(Tool):
    def __init__(self, config):
        super(StringtieMergeTool, self).__init__(config)
        self._version = "v1.0.1"
        self.stringtie_merge_path = 'bioinfo/rna/stringtie-1.2.4/'
        self.gffread_path = "bioinfo/rna/cufflinks-2.2.1/"
        tmp = os.path.join(self.config.SOFTWARE_DIR, self.stringtie_merge_path)
        tmp_new = tmp + ":$PATH"
        self.logger.debug(tmp_new)
        self.set_environ(PATH=tmp_new)

    def run(self):
        """
        运行
        :return:
        """
        super(StringtieMergeTool, self).run()
        self.run_stringtie_merge()
        self.run_gtf_to_fa()
        self.set_output()
        self.end()

    def run_stringtie_merge(self):
        """
        运行stringtie软件，进行拼接合并
        """
        cmd = self.stringtie_merge_path + 'stringtie --merge {} -p {} -G {} -s {} -o {}merged.gtf ' .format(
            self.option('assembly_GTF_list.txt').prop['path'], self.option('cpu'), self.option('ref_gtf').prop['path'],
            self.option('ref_fa').prop['path'], self.work_dir+"/")
        self.logger.info('运行stringtie软件，进行拼接合并')
        command = self.add_command("stringtie_merge_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("stringtie_merge运行完成")
        else:
            self.set_error("stringtie_merge运行出错!")

    def run_gtf_to_fa(self):
        """
        运行gtf_to_fasta，转录本gtf文件转fa文件
        """
        cmd = self.gffread_path + "gffread %s -g %s -w merged.fa" % (
        self.work_dir + "/"+"merged.gtf", self.option('ref_fa').prop['path'])
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
            shutil.copy2(self.work_dir + "/merged.gtf", self.output_dir + "/merged.gtf")
            self.option('merged.gtf').set_path(self.work_dir + "/merged.gtf")
            shutil.copy2(self.work_dir + "/merged.fa", self.output_dir + "/merged.fa")
            self.logger.info("设置拼接合并分析结果目录成功")

        except Exception as e:
            self.logger.info("设置拼接合并分析结果目录失败{}".format(e))
            self.set_error("设置拼接合并分析结果目录失败{}".format(e))
