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
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir,denovo_rna.express.express_matrix,toolapps.table"},
            {"name": "group_table", "type": "infile", "format": " meta.otu.group_table, toolapps.group_table"},  # 输入的group表格
            # # {"name": "venn_table.xls", "type": "outfile", "format": "meta.otu.venn_table"},  # 输入的Venn表格
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
        if self.option("group_table").format == 'toolapps.group_table':
            for i in self.option('group_table').prop['sample_name']:
                if i not in self.option('otu_table').prop['col_sample']:
                    raise Exception('分组文件中的样本不存在于表格中，查看是否是数据取值选择错误')

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
        # self.R_path2 = self.config.SOFTWARE_DIR + '/program/R-3.3.1/bin/'  # 循环投递时需要全路径
        self.venn_path = self.config.SOFTWARE_DIR + '/bioinfo/plot/scripts/'
        self.python_path = self.config.SOFTWARE_DIR + '/program/Python/bin/'
        self.python_path2 = '/program/Python/bin/'  # 用于小工具
        self.software = 'program/parafly-r2013-01-21/bin/bin/ParaFly'
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
        get_cmd_list = []
        cmd_list = []
        if len(self.option("group_table").prop['group_scheme']) == 1:   # 判断分组方案的个数
            os.link(group_file, self.work_dir + '/group_table')  # venn_table的结果与分组文件的目录一致，所以需要将分组文件放在工作目录下
            if self.option("otu_table").format == 'toolapps.table':
                self.option("otu_table").get_table_of_main_table(otu_table, self.work_dir + '/new_input.xls',group_file)
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (
                self.python_path, self.venn_path, self.work_dir + '/new_input.xls', self.work_dir + '/group_table')
            else:
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o cmd.r' % (
                self.python_path, self.venn_path, self.option("otu_table").prop['path'], self.work_dir + '/group_table')
            self.logger.info(venn_cmd)
            os.system(venn_cmd)
            self.logger.info('运行venn_cmd')
            cmd = self.R_path + 'Rscript cmd.r'
            self.logger.info("开始运行venn_table")
            command = self.add_command("venn_table", cmd)
            command.run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("运行venn_table完成")
            else:
                self.set_error("运行venn_table运行出错!")
                raise Exception("运行venn_table运行出错，请检查输入的表格是否正确")
            # 统计各组所有otu/物种名 add by qindanhua
            venn_graph(otu_table, group_file, "venn_graph.xls")

        else:  # 小工具专用，用于批量生成多个分组方案对应的结果
            for i in range(len(self.option("group_table").prop['group_scheme'])):
                select_group = []
                sample_dir = self.work_dir + '/' + self.option("group_table").prop['group_scheme'][i]
                os.mkdir(sample_dir)
                select_group.append(self.option("group_table").prop['group_scheme'][i])
                self.option('group_table').sub_group(sample_dir + '/venn_group_' + str(i+1), select_group)
                self.option("otu_table").get_table_of_main_table(otu_table, sample_dir + '/input_' + str(i + 1),
                                                                   group_file)
                venn_cmd = '%spython %svenn_table.py -i %s -g %s -o %scmd_%s.r' % (self.python_path2, self.venn_path, sample_dir + '/input_' + str(i + 1), sample_dir + '/venn_group_' + str(i+1), sample_dir + '/', i+1)
                get_cmd_list.append(venn_cmd)  # 存放所有生成cmd.r的命令
                cmd_list.append(self.R_path + 'Rscript {}cmd_{}.r'.format(sample_dir + '/', i+1))  # 存放所有运行cmd.r的命令
            #  循环投递，批量生成cmd.r文件，结果及日志存放在对应分组方案的文件夹下
            n = 0
            for i in get_cmd_list:
                command = self.add_command("get_cmd_{}".format(n), i).run()
                self.wait(command)
                if command.return_code == 0:
                    self.logger.info("运行{}完成".format(command.name))
                    n += 1
                else:
                    self.set_error("运行{}运行出错!".format(command.name))
                    raise Exception("运行venn_table运行出错，请检查输入的otu表和group表是否正确")

            # 循环投递，批量运行cmd.r文件，结果及日志存放在对应分组方案的文件夹下
            n = 0
            for i in cmd_list:
                command = self.add_command("cmd_{}".format(n), i).run()
                self.wait(command)
                if command.return_code == 0:
                    self.logger.info("运行{}完成".format(command.name))
                else:
                    self.set_error("运行{}运行出错!".format(command.name))
                    raise Exception("运行venn_table运行出错，请检查运行命令是否正确")
                # 统计各组所有otu/物种名 add by qindanhua
                venn_graph(otu_table, self.work_dir + '/' + self.option("group_table").prop['group_scheme'][
                    n] + '/venn_group_' + str(n + 1),
                           self.work_dir + '/' + self.option("group_table").prop['group_scheme'][n] + "/venn_graph.xls")
                n += 1

    def set_output(self):
        """
        将结果文件链接至output
        """
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        if len(self.option("group_table").prop['group_scheme']) == 1:
            os.link(self.work_dir + '/venn_table.xls', self.output_dir + '/venn_table.xls')
            if os.path.exists(self.work_dir + "/venn_graph.xls"):
                 os.link(self.work_dir + '/venn_graph.xls', self.output_dir + '/venn_graph.xls')
        else:
            for i in range(len(self.option("group_table").prop['group_scheme'])):
                file_graph = self.work_dir + '/' + self.option("group_table").prop['group_scheme'][i] + '/venn_graph.xls'
                file_table = self.work_dir + '/' + self.option("group_table").prop['group_scheme'][i] + '/venn_table.xls'
                os.link(file_graph,
                        self.output_dir + '/' + self.option("group_table").prop['group_scheme'][i] + '_venn_graph.xls')
                os.link(file_table,
                        self.output_dir + '/' + self.option("group_table").prop['group_scheme'][i] + '_venn_table.xls')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        self.logger.info("开始运行！")
        self._create_venn_table()
        self.logger.info("set out put")
        self.set_output()
        self.end()
