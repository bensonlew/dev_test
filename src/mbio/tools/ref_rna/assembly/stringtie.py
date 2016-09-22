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
            {"name": "sample_bam", "type": "infile", "format": "ref_rna.assembly.bam"},  # 所有样本比对之后的bam文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 参考基因的注释文件
            {"name": "cpu", "type": "int", "default": 10},  # stringtie软件所分配的cpu数量
            # {"name": "memory", "type": "string", "default": '100G'},  # stringtie软件所分配的内存，单位为GB
            # {"name": "fr-unstranded", "type": "string"},  # 是否链特异性
            # {"name": "fr-firststrand", "type": "string"},  # 链特异性时选择正链
            # {"name": "fr-secondstrand", "type": "string"},  # 链特异性时选择负链
            {"name": "sample_gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"}# 输出的gtf文件
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
        # if not self.option("fr-unstranded") and not self.option("fr-firststrand").is_set and not self.option(
        #         "fr-secondstrand").is_set:
        #     raise OptionError("当链特异性时必须选择正负链")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "100G"

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

    def run(self):
        """
        运行
        :return:
        """
        super(StringtieTool, self).run()
        self.run_stringtie()
        self.set_output()
        self.end()

    def run_stringtie(self):
        """
        运行stringtie软件，进行拼接组装
        """
        sample_name = os.path.basename(self.option('sample_bam').prop['path']).split('.bam')[0]
        cmd = self.stringtie_path + 'stringtie %s -p %s -G %s -s %s -o ' % (self.option('sample_bam').prop['path'], self.option('cpu'), self.option('ref_gtf').prop['path'], self.option('ref_fa').prop['path'])+sample_name+"_out.gtf"
        self.logger.info('运行stringtie软件，进行组装拼接')
        command = self.add_command("stringtie_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("stringtie运行完成")
        else:
            self.set_error("stringtie运行出错!")

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
            shutil.copy2(self.work_dir + "/"+sample_name+"_out.gtf", self.output_dir +"/"+ sample_name+"_out.gtf")
            self.option('sample_gtf').set_path(self.work_dir +"/"+ sample_name+"_out.gtf")
            self.logger.info("设置组装拼接分析结果目录成功")

        except Exception as e:
            self.logger.info("设置组装拼接分析结果目录失败{}".format(e))
            self.set_error("设置组装拼接分析结果目录失败{}".format(e))
