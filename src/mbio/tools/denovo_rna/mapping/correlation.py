# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import numpy as np
from biocluster.core.exceptions import OptionError
import os
import subprocess


class CorrelationAgent(Agent):
    """
    计算样本间相关系数的工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.11
    """

    def __init__(self, parent):
        super(CorrelationAgent, self).__init__(parent)
        options = [
            {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # Fpkm矩阵表
            # {"name": "", "type": "outfile", "format": "denovo_rna.gene_structure.bed"}  # bed格式文件
        ]
        self.add_option(options)
        self.step.add_steps('correlation')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.correlation.start()
        self.step.update()

    def step_end(self):
        self.step.correlation.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fpkm").is_set:
            raise OptionError("请传入比对结果bam格式文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = '2G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./correlation_matrix.xls", "xls", "相关系数矩阵表"],
            ["./hcluster_tree_correlation_matrix.xls_average.tre", "xls", "相关系数树文件"]
        ])
        super(CorrelationAgent, self).end()


class CorrelationTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(CorrelationTool, self).__init__(config)
        # self.python_path = "program/Python/bin/"
        self.fpkm_path = self.option("fpkm").prop["path"]
        self.Rscript_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin/"
        self.perl_path = self.config.SOFTWARE_DIR + "/program/perl/perls/perl-5.24.0/bin/"
        self.hcluster_script_path = self.config.SOFTWARE_DIR + "/bioinfo/statistical/scripts/"

    def correlation(self):
        with open(self.fpkm_path, "r") as f, open("correlation_matrix.xls", "w") as w:
            row = []
            samples = f.readline().strip().split()
            for sample in samples:
                row.append([])
            for line in f:
                line_sp = line.strip().split()
                line_sp.pop(0)
                for index, value in enumerate(line_sp):
                    row[index].append(value)
            sample_array = np.array(row, float)
            correlation_matrix = np.corrcoef(sample_array)
            sample_line = "\t".join(samples)
            write_line = "\t{}\n".format(sample_line)
            w.write(write_line)
            for i in range(len(correlation_matrix)):
                line = samples[i]
                for num in correlation_matrix[i]:
                    line += "\t{}".format(num)
                # print line
                w.write(line + "\n")

    def plot_hcluster(self):
        perl_cmd = "{}perl {}plot-hcluster_tree.pl -i correlation_matrix.xls -o {}".\
            format(self.perl_path, self.hcluster_script_path, "hcluster")
        r_cmd = "{}Rscript {}".format(self.Rscript_path, "hc.cmd.r")
        self.logger.info(perl_cmd)
        self.logger.info(r_cmd)
        os.system(perl_cmd)
        try:
            subprocess.check_output(r_cmd, shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("运行hcluster出错")
            return False

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+"/"+"correlation_matrix.xls", self.output_dir+"/"+"correlation_matrix.xls")
        os.link(self.work_dir+"/hcluster/hcluster_tree_correlation_matrix.xls_average.tre",
                self.output_dir + "/hcluster_tree_correlation_matrix.xls_average.tre")
        self.logger.info("done")
        self.end()

    def run(self):
        """
        运行
        """
        super(CorrelationTool, self).run()
        self.correlation()
        self.plot_hcluster()
        self.set_output()
