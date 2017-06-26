# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
# import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.graph.venn_table import venn_graph


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
            {"name": "otu_table", "type": "infile", "format": "toolapps.table, meta.otu.otu_table,meta.otu.tax_summary_dir,denovo_rna.express.express_matrix"},
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table, meta.otu.group_table"},  # 输入的group表格
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
        if not self.option("otu_table"):
            raise OptionError("参数otu_table不能为空")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class',
                                        'order', 'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        if not self.option("group_table").is_set:
            raise OptionError("参数group_table不能为空")
        # if self.option("group_table").format == 'meta.otu.otu_table':    # add by wzy 2017.6.23
        #     group_file = self.option("group_table").prop['path']
        if self.option("group_table").format == 'toolapps.group_table':
            group_file = self.option("group_table").prop['new_table']
            with open(group_file) as f:   # add by wzy 20170621
                first_line = f.readline()
                line_split = first_line.strip().split("\t")
                if len(line_split) > 2:
                    raise OptionError("分组文件只能有一个分组方案，请去除其他分组方案")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''

    def end(self):  # add by wzy 20170608
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "venn图结果目录"],
        ])
        super(VennTableAgent, self).end()


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

    def _create_venn_table(self):
        """
        调用脚本venn_table.py,输出venn表格
        """
        if self.option("group_table").format == 'toolapps.group_table':  # add by wzy 2017.6.23
            group_file = self.option("group_table").prop['new_table']
        else:
            group_file = self.option("group_table").prop['path']
        if self.option("otu_table").format == 'toolapps.table':
            otu_table = self.option("otu_table").prop['new_table']
        elif self.option("otu_table").format is "meta.otu.tax_summary_dir":
            otu_table = self.option("otu_table").get_table(self.option("level"))
        else:
            otu_table = self.option("otu_table").prop['path']
        # os.system('dos2unix -c Mac {}'.format(otu_table))  # add by wzy 20170609
        # os.system('dos2unix -c Mac {}'.format(group_file))

        num_lines = sum(1 for line in open(otu_table))
        if num_lines < 11:
            self.set_error("输入文件的行数小于10个！请更换输入文件！")
            raise Exception("输入文件的行数小于10个！请更换输入文件！")
        if len(self.option("group_table").prop['group_scheme']) == 1:
            venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (self.python_path, self.venn_path, otu_table, group_file)
        # add by qiuping, for denovo_rna venn, 20160728
        else:
            self.option('group_table').sub_group(self.work_dir + '/venn_group', self.option("group_table").prop['group_scheme'][0])
            venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (self.python_path, self.venn_path, otu_table, self.work_dir + '/venn_group')
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
            raise Exception("运行venn_table运行出错，请检查输入的otu表和group表是否正确")
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
        # self.option('venn_table.xls').set_path(self.output_dir+'/venn_table.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        self._create_venn_table()
        self.end()
