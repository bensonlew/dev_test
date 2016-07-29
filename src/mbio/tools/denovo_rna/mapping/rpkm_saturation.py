# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


class RpkmSaturationAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.27
    """

    def __init__(self, parent):
        super(RpkmSaturationAgent, self).__init__(parent)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "quality", "type": "int", "default": 30}  # 质量值
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bam").is_set:
            raise OptionError("请传入比对结果bam格式文件")
        if not self.option("bed").is_set:
            raise OptionError("请传入bed格式文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class RpkmSaturationTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(RpkmSaturationTool, self).__init__(config)
        self.python_path = "program/Python/bin/"
        self.perl_path = "program/perl/perls/perl-5.24.0/bin/perl"
        self.plot_script = self.config.SOFTWARE_DIR + "/bioinfo/plot/scripts/saturation2plot.pl"
        self.plot_cmd = []

    def rpkm_saturation(self, bam, out_pre):
        bam_name = bam.split("/")[-1]
        out_pre = out_pre + "_" + bam_name
        satur_cmd = "{}RPKM_saturation.py -i {} -r {} -o {} -q {}".format(self.python_path, bam, self.option("bed").prop["path"], out_pre, self.option("quality"))
        print(satur_cmd)
        self.logger.info("开始运行RPKM_saturation.py脚本")
        satur_command = self.add_command("{}_satur".format(bam_name.lower()), satur_cmd)
        satur_command.run()
        return satur_command

    def multi_satur(self, bam_dir, out_pre):
        cmds = []
        bams = glob.glob("{}/*.bam".format(bam_dir))
        for bam in bams:
            cmd = self.rpkm_saturation(bam, out_pre)
            cmds.append(cmd)
        return cmds

    def rpkm_plot(self, rpkm, out_pre):
        cmd = "{} {} -in {} -out {}".format(self.perl_path, self.plot_script, rpkm, out_pre)
        self.logger.info(cmd)
        cmd = self.add_command("rpkm_plot_{}".format(out_pre.lower()), cmd)
        cmd.run()
        self.wait()
        if cmd.return_code == 0:
            self.logger.info("运行{}结束!".format(cmd.name))
        else:
            self.set_error("运行{}出错!".format(cmd.name))
        return cmd

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        satur_file = glob.glob(r"satur*")
        print(satur_file)
        for f in satur_file:
            output_dir = os.path.join(self.output_dir, f)
            os.link(os.path.join(self.work_dir, f), output_dir)
        self.logger.info("set done")
        self.end()

    def run(self):
        """
        运行
        """
        super(RpkmSaturationTool, self).run()
        if self.option("bam").format == "align.bwa.bam":
            saturation = self.rpkm_saturation(self.option("bam").prop["path"], "satur")
            self.wait(saturation)
            if saturation.return_code == 0:
                self.logger.info("运行RPKM_saturation.py脚本结束！")
            else:
                self.set_error("运行RPKM_saturation.py脚本过程出错")
        elif self.option("bam").format == "align.bwa.bam_dir":
            saturation = self.multi_satur(self.option("bam").prop["path"], "satur")
            self.wait()
            for cmd in saturation:
                if cmd.return_code == 0:
                    self.logger.info("运行{}结束!".format(cmd.name))
                else:
                    self.set_error("运行{}出错!".format(cmd.name))
        for f in os.listdir(self.work_dir):
            if "RPKM" in f:
                self.logger.info("saturation2plot")
                self.logger.info(f)
                plot_cmd = self.rpkm_plot(os.path.join(self.work_dir, f), "satur_" + f.split("_")[1].split(".")[0])
                self.logger.info(plot_cmd)
        self.set_output()
        # for f in os.listdir(self.output_dir):


