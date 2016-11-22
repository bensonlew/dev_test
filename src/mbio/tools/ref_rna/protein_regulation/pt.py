## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
#last_modify:20161108

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re
import shutil


class PtAgent(Agent):
    """
    调用pt.sh脚本，完成无创亲子鉴定的生信call SNP制表等分析流程，获得mem.sort.hit.vcf.tab文件
    version v1.0
    author: hongdongxuan
    last_modify: 2016.11.08
    """
    def __init__(self, parent):
        super(PtAgent, self).__init__(parent)
        options = [
            {"name": "fastq_R1", "type": "infile", "format": "sequence.fastq"}, #输入F/M/S的fastq文件
            {"name": "fastq_R2", "type": "infile", "format": "sequence.fastq"},
            #{"name": "cpu_number", "type": "int", "default": 4},
            #{"name": "input_dir", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("pt_tool")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.pt_tool.start()
        self.step.update()

    def stepfinish(self):
        self.step.pt_tool.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("fastq_R1").is_set:
            raise OptionError("必须输fastq_R1文件")
        if not self.option("fastq_R2").is_set:
            raise OptionError("必须输入fastq_R2文件")
        # if not self.option('input_dir'):
        #     raise OptionError('必须提供fastq文件所在的路径')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '200G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
                    ])
        result_dir.add_regexp_rules([
            [r".mem.sort.hit.vcf.tab", "tab", "所有位点的信息"],

        ])
        super(PtAgent, self).end()


class PtTool(Tool):
    """
    运行pt.sh文件，获得tab文件
    example: pt.sh sample fastq_path
    """
    def __init__(self, config):
        super(PtTool, self).__init__(config)
        self._version = '1.0.1'
        self.cmd_path = "bioinfo/rna/scripts/pt.sh"

    def run_pt(self):
        file_path = os.path.split(self.option("fastq_R1").prop["path"])[0]
        print file_path
        sample_name = os.path.split(self.option("fastq_R1").prop["path"])[1].split("_")[0]
        print sample_name
        pt_cmd = self.cmd_path + " %s %s" % (sample_name, file_path)
        print pt_cmd
        self.logger.info(pt_cmd)
        self.logger.info("开始运行cmd")
        cmd = self.add_command("cmd", pt_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行cmd成功")
        else:
            self.logger.info("运行cmd出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        file_path = os.path.split(self.option("fastq_R1").prop["path"])[0] + "/"
        print file_path
        results = os.listdir(file_path)
        for f in results:
            if re.search(r'.*tab$', f):
                shutil.copy(file_path + f, self.output_dir)
            else:
                pass
        self.logger.info('设置文件夹路径成功')


    def run(self):
        super(PtTool, self).run()
        self.run_pt()
        self.set_output()
        self.end()
