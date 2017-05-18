# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re
import shutil


class StringtieAgent(Agent):
    """
    有参转录组stringtie拼接
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.06
    """
    def __init__(self, parent):
        super(StringtieAgent, self).__init__(parent)
        options = [
            {"name": "sample_bam", "type": "infile", "format": "align.bwa.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            {"name": "sample_gtf", "type": "outfile", "format": "gene_structure.gtf"}  # 输出的gtf文件
        ]
        self.add_option(options)
        self.step.add_steps("stringtie")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.stringtie.start()
        self.step.update()

    def stepfinish(self):
        self.step.stringtie.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('sample_bam'):
            raise OptionError('必须输入样本文件为bam格式')
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
        self._cpu = 1
        self._memory = "10G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["_out.gtf", "gtf", "样本拼接之后的gtf文件"]
        ])
        super(StringtieAgent, self).end()


class StringtieTool(Tool):
    def __init__(self, config):
        super(StringtieTool, self).__init__(config)
        self._version = "v1.0.1"
        self.stringtie_path = 'bioinfo/rna/stringtie-1.2.4/'
        self.gffread_path = "bioinfo/rna/cufflinks-2.2.1/"

    def run(self):
        """
        运行
        :return:
        """
        super(StringtieTool, self).run()
        self.run_stringtie()
        self.run_gtf_to_fa()
        self.set_output()
        self.end()

    def run_stringtie(self):
        """
        运行stringtie软件，进行拼接组装
        """
        sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        cmd = self.stringtie_path + 'stringtie %s -p %s -G %s -s %s -o ' % (
            self.option('sample_bam').prop['path'], self.option('cpu'), self.option('ref_gtf').prop['path'],
            self.option('ref_fa').prop['path'])+sample_name+"_out.gtf"
        self.logger.info('运行stringtie软件，进行组装拼接')
        command = self.add_command("stringtie_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("stringtie运行完成")
        else:
            self.set_error("stringtie运行出错!")

    def run_gtf_to_fa(self):
        """
        运行gtf_to_fasta，转录本gtf文件转fa文件
        """
        sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        cmd = self.gffread_path + "gffread %s -g %s -w %s_out.fa" % (
        self.work_dir + "/"+sample_name+"_out.gtf", self.option('ref_fa').prop['path'],
        self.work_dir + "/" + sample_name)
        self.logger.info('运行gtf_to_fasta，形成fasta文件')
        command = self.add_command("gtf_to_fa_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("gtf_to_fasta运行完成")
        else:
            self.set_error("gtf_to_fasta运行出错!")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        try:
            sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
            shutil.copy2(self.work_dir + "/" + sample_name + "_out.fa", self.output_dir +
            "/" + sample_name + "_out.fa")
            shutil.copy2(self.work_dir + "/"+sample_name+"_out.gtf", self.output_dir + "/" + sample_name+"_out.gtf")
            self.option('sample_gtf').set_path(self.work_dir + "/" + sample_name+"_out.gtf")
            self.logger.info("设置组装拼接分析结果目录成功")

        except Exception as e:
            self.logger.info("设置组装拼接分析结果目录失败{}".format(e))
            self.set_error("设置组装拼接分析结果目录失败{}".format(e))
