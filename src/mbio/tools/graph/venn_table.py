# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
# import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.graph.venn_table import venn_graph
from collections import defaultdict
from biocluster.core.exceptions import FileError
import re
import shutil


class VennTableAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.11
    需要R软件
    """
    def __init__(self, parent):
        super(VennTableAgent, self).__init__(parent)
        options = [
            {"name": "otu_table", "type": "infile",
             "format": "meta.otu.otu_table,meta.otu.tax_summary_dir,denovo_rna.express.express_matrix"},
            # 输入的表格，可以是矩阵
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表格
            {"name": "level", "type": "string", "default": "otu"},  # 物种水平
            {"name": "method", "type": "string", "default": "matrix"}  # 输入的文件类型，默认矩阵 add by wzy 20170424
        ]
        self.add_option(options)
        self.step.add_steps('venn_table')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.venn_table.start()
        self.step.update()

    def step_end(self):
        self.step.venn_table.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("otu_table"):
            raise OptionError("参数otu_table不能为空")
        #  如果有分组文件，则判断分组文件是否在6个以内
        if self.option("group_table").is_set:
            with open(self.option("group_table").prop['path']) as f:
                samples = set()
                for line in f:
                    if re.search("#", line):
                        line1 = line.strip().split('\t')
                        if len(line1) == 2:
                            pass
                        else:
                            raise FileError("分组方案能有一个")
                    else:
                        line_split = line.strip().split('\t')
                        samples.add(line_split[1])
                if len(samples) > 6:
                    raise FileError("目前分组有{}组，分组不能超过6组".format(len(samples)))
                else:
                    pass
        # 如果没有分组文件，则判断样本是否在6个以内
        else:
            if self.option("method") == "matrix":
                with open(self.option("otu_table").prop['path'])as r:
                    names = r.readline().strip().split("\t")
                    names = [name for name in names if (name != "\r\n") and (name != "\n")]
                    if len(names) > 6:
                        raise FileError("目前有{}个样本，样本不能超过6个，请增加分组文件或删减样本".format(len(names)-1))
                    else:
                        pass

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class VennTableTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(VennTableTool, self).__init__(config)
        self.R_path = '/program/R-3.3.1/bin/'
        self.venn_path = self.config.SOFTWARE_DIR + '/bioinfo/plot/scripts/'
        self.python_path = self.config.SOFTWARE_DIR + '/program/Python/bin/'
        self._version = 1.0

    def _create_venn_table_matrix(self):
        """
        调用脚本venn_table.py,输出venn表格
        有分组用分组，没有分组新建一个分组，每个样本是一个组
        """
        if self.option("group_table").is_set:
            group_file = self.option("group_table").prop['path']
        else:
            group_file = self.work_dir + '/bulit_group.xls'
            with open(group_file, "w+")as g, open(self.option("otu_table").prop['path'])as r:
                g.write("#sample\tAAA\n")
                names = r.readline().strip().split("\t")
                names = [name for name in names if (name != "\r\n") and (name != "\n")]
                for i in names[1:]:
                    g.write(i + "\t" + i + "\n")
        #  above add by wzy 20170424
        otu_table = self.option("otu_table").prop['path']
        if self.option("otu_table").format is "meta.otu.tax_summary_dir":
            otu_table = self.option("otu_table").get_table(self.option("level"))
        num_lines = sum(1 for line in open(otu_table))
        if num_lines < 3:  # change by wzy 20170424
            self.set_error("输入文件的行数小于3个！请更换输入文件！")
            raise Exception("输入文件的行数小于3个！请更换输入文件！")
        if self.option("group_table").is_set:
            if len(self.option("group_table").prop['group_scheme']) == 1:
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (
                self.python_path, self.venn_path, otu_table, group_file)
            # add by qiuping, for denovo_rna venn, 2060728
            else:
                self.option('group_table').sub_group(self.work_dir + '/venn_group',
                                                     self.option("group_table").prop['group_scheme'][0])
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (
                self.python_path, self.venn_path, otu_table, self.work_dir + '/venn_group')
        else:
            venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (
                self.python_path, self.venn_path, otu_table, group_file)
        # add end
        self.logger.info(venn_cmd)
        os.system(venn_cmd)
        cmd = self.R_path + 'Rscript cmd.r'
        # print cmd
        self.logger.info("开始运行venn_table")
        command = self.add_command("venn_table", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行venn_table完成")
        else:
            self.set_error("运行venn_table运行出错!")
            raise Exception("运行venn_table运行出错，请检查输入的表格和group表是否正确")
        # 统计各组所有otu/物种名 add by qindanhua
        venn_graph(otu_table, group_file, "venn_graph.xls")
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir + '/venn_table.xls', self.output_dir + '/venn_table.xls')
        if os.path.exists(self.work_dir + "/venn_graph.xls"):
            os.link(self.work_dir + '/venn_graph.xls', self.output_dir + '/venn_graph.xls')
        if self.option("group_table").is_set:
            shutil.copy2(self.option("group_table").prop['path'], self.work_dir + '/group.xls')
        else:
            shutil.copy2(self.work_dir + '/bulit_group.xls', self.work_dir + '/group.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        if self.option("method") == "matrix":
            self._create_venn_table_matrix()
        self.end()
