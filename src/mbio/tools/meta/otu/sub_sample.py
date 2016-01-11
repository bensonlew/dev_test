# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import re
import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from mbio.files.meta.otu.otu_table import OtuTableFile


class SubSampleAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.19
    需要mothur 版本1.30
    需要shared2otu.pl
    需要otu2shared.pl
    """
    def __init__(self, parent):
        super(SubSampleAgent, self).__init__(parent)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},  # 输入的OTU文件
            {"name": "out_otu_table", "type": "outfile", "format": "meta.otu.otu_table"},  # 输出的OTU文件
            {"name": "level", "type": "string", "default": "otu"}]  # 物种水平
        self.add_option(options)
        self.step.add_steps("sub_sample")
        self.on('start', self.start_sub_sample)
        self.on('end', self.end_sub_sample)

    def start_sub_sample(self):
        self.step.sub_sample.start()
        self.update()

    def end_sub_sample(self):
        self.step.sub_sample.end()
        self.update()

    def check_options(self):
        """
        参数检测
        """
        if not self.option("in_otu_table").is_set:
            raise OptionError("参数in_otu_table不能为空")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class',
                                        'order', 'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        return True

    def set_resource(self):
        """
        设置所需的资源
        """
        self._cpu = 1
        self._memory = ""


class SubSampleTool(Tool):
    def __init__(self, config):
        super(SubSampleTool, self).__init__(config)
        self.mothur_path = "meta/mothur.1.30"
        self.shared2otu_path = os.path.join(Config().SOFTWARE_DIR, "meta/scripts/shared2otu.pl")

    def sub_sample(self):
        """
        运行mothur的subsample，进行抽平
        """
        if self.option("in_otu_table").format == "meta.otu.tax_summary_dir":
            otu_table = os.path.basename(self.option("otu_table").get_table(self.option("level")))
        else:
            otu_table = os.path.basename(self.option("in_otu_table").prop["path"])
        shared_path = os.path.join(self.work_dir, otu_table + ".shared")
        mothur_dir = os.path.join(self.work_dir, "mothur")
        if not os.path.exists(mothur_dir):
            os.mkdir(mothur_dir)
        my_table = OtuTableFile()
        basename = ""
        if self.option("in_otu_table").format == "meta.otu.tax_summary_dir":
            my_table.set_path(self.option("in_otu_table").get_table(self.option("level")))
            my_table.get_info()
            basename = my_table.prop['basename']
            my_table.convert_to_shared(shared_path)
        else:
            self.logger.debug(self.option("in_otu_table").format)
            self.option("in_otu_table").get_info()
            self.option("in_otu_table").convert_to_shared(shared_path)
            basename = self.option("in_otu_table").prop['basename']
        cmd = self.mothur_path + " \"#set.dir(output=" + mothur_dir\
            + ");sub.sample(shared=" + shared_path + ")\""
        sub_sample_cmd = self.add_command("sub_sample_cmd", cmd)
        self.logger.info("开始运行sub.sample")
        sub_sample_cmd.run()
        self.wait(sub_sample_cmd)
        if sub_sample_cmd.return_code == 0:
            self.logger.info("运行sub.sample完成")
        else:
            self.set_error("运行sub.sample出错")
        self.logger.info("运行share2otu,将shared文件转化为otu")
        dir_ = os.listdir(mothur_dir)
        for file_ in dir_:
            if re.search(r'subsample', file_):
                sub_sampled_shared = os.path.join(mothur_dir, file_)
                break
        match = re.search(r"(^.+)(\..+$)", basename)
        prefix = match.group(1)
        suffix = match.group(2)
        sub_sampled_otu = os.path.join(self.work_dir, "output", prefix + ".subsample" + suffix)
        cmd = self.shared2otu_path + " -l 0.97 -i " + sub_sampled_shared + " -o " + sub_sampled_otu
        try:
            subprocess.check_call(cmd, shell=True)
            self.option("out_otu_table").set_path(sub_sampled_otu)
        except subprocess.CalledProcessError:
            raise Exception("shared2otu.pl 运行出错")

    def run(self):
        """
        运行
        """
        super(SubSampleTool, self).run()
        self.sub_sample()
        self.end()
