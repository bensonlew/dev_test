# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import numpy as np
from biocluster.core.exceptions import OptionError
import os


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
        self._memory = ''


class CorrelationTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(CorrelationTool, self).__init__(config)
        self.python_path = "Python/bin/"
        self.fpkm_path = self.option("fpkm").prop["path"]

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

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+"/"+"correlation_matrix.xls", self.output_dir+"/"+"correlation_matrix.xls")
        self.logger.info("done")
        self.end()

    def run(self):
        """
        运行
        """
        super(CorrelationTool, self).run()
        self.correlation()
        self.set_output()
