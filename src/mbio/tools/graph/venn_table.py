# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
# import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
# from mbio.packages.graph.venn_table import venn_table


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
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.otu_tax_summary_dir"},
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表格
            # {"name": "venn_table.xls", "type": "outfile", "format": "meta.otu.venn_table"},  # 输入的Venn表格
            {"name": "level", "type": "string", "default": "otu"}  # 物种水平
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
        if not self.option("otu_table").is_set:
            raise OptionError("参数otu_table不能为空")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class',
                                        'order', 'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        if not self.option("group_table").is_set:
            raise OptionError("参数group_table不能为空")

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
        print self.R_path
        self._version = 1.0

    def _create_venn_table(self):
        """
        调用脚本venn_table.py,输出venn表格
        """
        otu_table = self.option("otu_table").prop['path']
        if self.option("otu_table").format is "meta.otu.tax_summary_dir":
            otu_table = self.option("otu_table").get_table(self.option("level"))
        num_lines = sum(1 for line in open(otu_table))
        if num_lines < 11:
            self.set_error("Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！")
            raise Exception("Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！")
        venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (self.python_path, self.venn_path, otu_table,
                                                                      self.option("group_table").prop['path'])
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
            raise Exception("运行venn_table运行出错，请检查输入的otu表和group表是否正确")
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir + '/venn_table.xls', self.output_dir + '/venn_table.xls')
        # self.option('venn_table.xls').set_path(self.output_dir+'/venn_table.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        self._create_venn_table()
        self.end()
