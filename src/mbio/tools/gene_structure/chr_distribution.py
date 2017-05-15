# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import re
import subprocess


class ChrDistributionAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.27
    """

    def __init__(self, parent):
        super(ChrDistributionAgent, self).__init__(parent)
        options = [
            {"name": "bam", "type": "infile", "format": "align.bwa.bam"},  # bam格式文件,排序过的
            {"name": "range", "type": "int", "default": 10000}  # 质量值
        ]
        self.add_option(options)
        self.step.add_steps('dup')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.dup.start()
        self.step.update()

    def step_end(self):
        self.step.dup.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bam").is_set:
            raise OptionError("请传入比对结果bam格式文件")

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
        super(ChrDistributionAgent, self).end()


class ChrDistributionTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(ChrDistributionTool, self).__init__(config)
        self.python_path = "program/Python/bin/"
        self.python_full_path = self.config.SOFTWARE_DIR + "/program/Python/bin/"
        self.samtools_path = self.config.SOFTWARE_DIR + "/bioinfo/align/samtools-1.3.1/"
        self.chr_distribution_path = self.config.SOFTWARE_DIR + "/bioinfo/gene-structure/scripts/reads_distribution.py"
        self.bam_name = ""

    def chr_distribution(self):
        bam_path = self.option("bam").prop["path"]
        self.bam_name = bam_path.split("/")[-1]
        cmd = "{}samtools view {} | cut -f 3 |uniq -c > {}".format(self.samtools_path, bam_path, self.bam_name + "_chr_stat.xls")
        print(cmd)
        self.logger.info("开始运行统计reads在染色体的分布情况")
        try:
            result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            with open("chr_stat.out", "w") as w:
                w.write(result)
            self.logger.info("开始运行统计reads在染色体的分布情况结束")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("统计出错")
            return False

    def chr_range_stat(self):
        cmd = "{} python {} -i {}".format(self.python_full_path, self.chr_distribution_path, self.option("bam").prop["path"])
        command = self.add_command('chr_range_stat', cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("统计染色体分布信息完成")
        else:
            self.set_error('统计染色体分布信息完成')

    def set_output(self):
        self.logger.info("set out put")
        out_put = self.output_dir + "/" + self.bam_name + "_chr_stat.xls"
        self.logger.info(out_put)
        # self.logger.info("sed \'$d\' {}".format(self.bam_name + "_chr_stat.xls"))
        # self.logger.info("awk \'BEGIN{print \"#chr\tread_num\"};{print $2\" \"$1}\'")
        cmd = "sed \'$d\' %s |awk \'BEGIN{print \"#chr\tread_num\"};{print $2\" \"$1}\' | awk \'NR==1||$2>100000||length($1)<8\' |sort -n -k 1 > %s" % (self.bam_name + "_chr_stat.xls", out_put)
        self.logger.info(cmd)
        os.system(cmd)
        self.logger.info("set done")
        self.end()

    def run(self):
        """
        运行
        """
        super(ChrDistributionTool, self).run()
        self.chr_distribution()
        self.set_output()
