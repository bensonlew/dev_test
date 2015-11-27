# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
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
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表格
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表格
            {"name": "venn_table.xls", "type": "outfile", "format": "meta.otu.venn_table"} # 输入的Venn表格
        ]
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("otu_table").is_set:
            raise OptionError("参数otu_table不能为空")
        if not self.option("group_table").is_set:
            raise OptionError("参数group_table不能为空")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class VennTableTool(Tool):
    def __init__(self, config):
        super(VennTableTool, self).__init__(config)
        self.R_path = 'R-3.2.2/bin/'
        self.venn_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'
        self.python_path = '/mnt/ilustre/users/sanger/app/Python/bin/'
        print self.R_path
        self._version = 1.0

    def _create_venn_table(self):
        """
        调用脚本venn_table.py,输出venn表格
        """
        venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' %(self.python_path, self.venn_path,
                    self.option("otu_table").prop['path'], self.option("group_table").prop['path'])
        print venn_cmd
        os.system(venn_cmd)
        cmd = self.R_path + 'Rscript cmd.r'
        print cmd
        self.logger.info("开始运行venn_table")
        command = self.add_command("venn_table", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行venn_table完成")
            # self.end()
        else:
            self.set_error("运行venn_table运行出错!")
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set out put")
        os.link(self.work_dir+'/venn_table.xls', self.output_dir+'/venn_table.xls')
        self.option('venn_table.xls').set_path(self.output_dir+'/venn_table.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        self._create_venn_table()
        self.end()
