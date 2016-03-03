# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""bcl2fastq 工具 """
import os
import errno
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError
from mbio.packages.datasplit.miseq_split import code2index


class Bcl2fastqAgent(Agent):
    """
    bcl2fastq
    version 2.17
    """
    def __init__(self, parent=None):
        super(Bcl2fastqAgent, self).__init__(parent)
        self._run_mode = "ssh1"
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'}  # 样本拆分信息表
        ]
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        """
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 1
        self._memory = ''


class Bcl2fastqTool(Tool):
    """
    """
    def __init__(self, config):
        super(Bcl2fastqTool, self).__init__(config)
        self._version = 1.0
        self.bcl2fastq_path = "rawdata/bcl2fastq/bin/bcl2fastq"
        self.option('sample_info').get_info()
        if not self.option('sample_info').check_parent_repeat():
            self.set_error("父样本中的index重复")
            raise Exception("父样本中的index重复")
        if not self.option('sample_info').check_child_repeat():
            self.set_error("属于同一个父样本的子样本中的index重复")
            raise Exception("属于同一个父样本的子样本中的index重复")

    def create_sample_sheet(self):
        """
        生成SampleSheet.csv, 供程序bcl2fastq使用
        """
        sample_sheet_path = os.path.join(self.work_dir, "soft_input", "SampleSheet.csv")
        self.logger.info("开始创建sample_sheet")
        with open(sample_sheet_path, 'w+') as w:
            head = "[Data],,,\nLane,Sample_ID,index,Sample_Project\n"
            w.write(head)
            for p_id in self.option('sample_info').prop["parent_ids"]:
                lane = self.option('sample_info').parent_sample(p_id, "lane")
                name = self.option('sample_info').parent_sample(p_id, "mj_sn")
                index = self.option('sample_info').parent_sample(p_id, "index")
                index = code2index("index", "bcl2fastq")[0]
                project = self.option('sample_info').parent_sample(p_id, "project")
                line = lane + "," + name + "," + index + "," + project + "\n"
                w.write(line)
            w.close()

    def bcl2fastq(self):
        """
        运行bcl2fastq
        """
        basecall = os.path.join(self.option('sample_info').prop['file_path'], "Data/Intensities/BaseCalls")
        output_dir = os.path.join(self.work_dir, "soft_output")
        sample_sheet_path = os.path.join(self.work_dir, "soft_input", "SampleSheet.csv")
        self.logger.debug(basecall)
        bcl2fastqstr = (self.bcl2fastq_path + " --input-dir " + basecall + " --runfolder-dir " +
                        self.option('sample_info').prop['file_path'] + " --output-dir " + output_dir +
                        " --sample-sheet " + sample_sheet_path +
                        " --barcode-mismatches " + str(self.option('sample_info').prop["index_missmatch"])
                        + " --use-bases-mask " + str(self.option('sample_info').prop["base_mask"]) +
                        " -p 8 -d 8"
                        )
        if self.option('sample_info').prop["ignore_missing_bcl"]:
            bcl2fastqstr = bcl2fastqstr + " --ignore-missing-bcl"
        bcl2fastqcmd = self.add_command("bcl2fastq", bcl2fastqstr)
        self.logger.info("开始运行bcl2fastq")
        self.logger.debug(bcl2fastqstr)
        bcl2fastqcmd.run()
        self.wait(bcl2fastqcmd)
        self.logger.debug(bcl2fastqcmd.return_code)
        if bcl2fastqcmd.return_code == 0:
            self.logger.info("bcl2fastq运行成功")
        else:
            self.logger.info("bcl2fastq运行失败")
            raise OSError("bcl2fastq运行失败")

    def make_ess_dir(self):
        """
        为软件bcl2fastq的运行创建必要的运行目录
        """
        input_dir = os.path.join(self.work_dir, "soft_input")
        output_dir = os.path.join(self.work_dir, "soft_output")
        dir_list = [input_dir, output_dir]
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录失败")

    def run(self):
        super(Bcl2fastqTool, self).run()
        self.make_ess_dir()
        self.create_sample_sheet()
        self.bcl2fastq()
        self.end()
