# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyeu'
import os
# import subprocess
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.graph.venn_table import venn_graph


class VennTableAgent(Agent):
    """
    小工具模块的venn图
    version 1.0
    author: wangzhaoyue
    last_modify: 2015.11.11
    需要R软件
    """
    def __init__(self, parent):
        super(VennTableAgent, self).__init__(parent)
        options = [
            {"name": "otu_table", "type": "infile", "format": "toolapps.table"},
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table"},  # 输入的group表格
            # # {"name": "venn_table.xls", "type": "outfile", "format": "meta.otu.venn_table"},  # 输入的Venn表格
            # {"name": "level", "type": "string", "default": "otu"}  # 物种水平
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
        # if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class',
        #                                 'order', 'family', 'genus', 'species']:
        #     raise OptionError("请选择正确的分类水平")
        if not self.option("group_table").is_set:
            raise OptionError("参数group_table不能为空")
        # if self.option("group_table").format == 'toolapps.group_table':
        #     group_file = self.option("group_table").prop['new_table']
        #     with open(group_file) as f:   # add by wzy 20170621
        #         first_line = f.readline()
        #         line_split = first_line.strip().split("\t")
        #         if len(line_split) > 2:
        #             raise OptionError("分组文件只能有一个分组方案，请去除其他分组方案")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = '10G'

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
        self.software = 'program/parafly-r2013-01-21/bin/bin/ParaFly'
        self._version = 1.0

    def _create_venn_table(self):
        """
        调用脚本venn_table.py,输出venn表格
        """
        group_file = self.option("group_table").prop['new_table']
        otu_table = self.option("otu_table").prop['new_table']
        cmd_list = []
        if len(self.option("group_table").prop['group_scheme']) == 1:
            venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (self.python_path, self.venn_path, otu_table, group_file)
        # add by qiuping, for denovo_rna venn, 20160728
        else:
            for i in range(len(self.option("group_table").prop['group_scheme'])):
                self.option('group_table').sub_group(self.work_dir + '/venn_group' + i, self.option("group_table").prop['group_scheme'][i])
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (self.python_path, self.venn_path, otu_table, self.work_dir + '/venn_group' + i)
                cmd_list.append(venn_cmd)
        # add end
        n = len(cmd_list) / 10
        if len(cmd_list) % 10 != 0:
            n += 1
        for i in range(0, n):
            cmd_file = os.path.join(self.work_dir, 'list_{}.txt'.format(i + 1))
            wrong_cmd = os.path.join(self.work_dir, 'failed_cmd_{}.txt'.format(i + 1))
            with open(cmd_file, "w")as c:
                cmd = cmd_list.pop
                c.write(cmd + '\n')
            final_cmd = '{} -c {} -CPU 10 -failed_cmds {}'.format(self.software, cmd_file, wrong_cmd)
            command = self.add_command("all_cmd_{}".format(i+1), final_cmd).run()
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
