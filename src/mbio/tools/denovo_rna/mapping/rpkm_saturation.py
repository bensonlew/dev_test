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
            {"name": "bed", "type": "infile", "format": "gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "quality", "type": "int", "default": 30},  # 质量值
            {"name": "low_bound", "type": "int", "default": 5},  # Sampling starts from this percentile
            {"name": "up_bound", "type": "int", "default": 100},  # Sampling ends at this percentile
            {"name": "step", "type": "int", "default": 5},  # Sampling frequency
            {"name": "rpkm_cutof", "type": "float", "default": 0.01}  #  Transcripts with RPKM smaller than this number will be ignored
        ]
        self.add_option(options)
        self.step.add_steps('satur')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.satur.start()
        self.step.update()

    def step_end(self):
        self.step.satur.finish()
        self.step.update()

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
        self._memory = '15G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*eRPKM\.xls", "xls", "RPKM表"],
            [r".*cluster_percent\.xls", "xls", "作图数据"]
        ])
        super(RpkmSaturationAgent, self).end()


class RpkmSaturationTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(RpkmSaturationTool, self).__init__(config)
        self.python_path = "program/Python/bin/"
        self.python_full_path = self.config.SOFTWARE_DIR + "/program/Python/bin/"
        self.perl_path = "program/perl/perls/perl-5.24.0/bin/perl"
        self.plot_script = self.config.SOFTWARE_DIR + "/bioinfo/plot/scripts/saturation2plot.pl"
        self.plot_cmd = []

    def rpkm_saturation(self, bam, out_pre):
        bam_name = bam.split("/")[-1].split(".")[0]
        out_pre = out_pre + "_" + bam_name
        satur_cmd = "{}python {}RPKM_saturation.py -i {} -r {} -o {} -q {} -l {} -u {} -s {} -c {}"\
            .format(self.python_path, self.python_full_path, bam, self.option("bed").prop["path"], out_pre, self.option("quality"), self.option("low_bound"), self.option("up_bound"), self.option("step"), self.option("rpkm_cutof"))
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
        files = os.listdir(self.work_dir)
        satur_file = []
        for f in files:
            if "cluster_percent.xls" in f:
                satur_file.append(f)
            if "eRPKM.xls" in f and "saturation.pdf" not in f:
                satur_file.append(f)
            if "saturation.r" in f:
                satur_file.append(f)
            if "saturation.pdf" in f and "eRPKM.xls" not in f:
                satur_file.append(f)
        print(satur_file)
        for f in satur_file:
            output_dir = os.path.join(self.output_dir, f)
            if os.path.exists(output_dir):
                os.remove(output_dir)
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
                plot_cmd = self.rpkm_plot(os.path.join(self.work_dir, f), f)
                self.logger.info(plot_cmd)
        self.set_output()
        # for f in os.listdir(self.output_dir):
