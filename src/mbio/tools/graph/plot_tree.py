# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os


class PlotTreeAgent(Agent):
    """
    R
    version 1.0
    author shenghe
    last_modified:2016.10.18
    """

    def __init__(self, parent):
        super(PlotTreeAgent, self).__init__(parent)
        options = [
            {"name": "abundance_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "newicktree", "type": "infile", "format": "meta.beta_diversity.newick_tree"},
            {"name": "leaves_group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "sample_group", "type": "infile", "format": "meta.otu.group_table"},
        ]
        self.add_option(options)
        self.step.add_steps('plot_tree')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.plot_tree.start()
        self.step.update()

    def step_end(self):
        self.step.plot_tree.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('abundance_table').is_set:
            raise OptionError('必须提供输入文件:物种丰富文件')
        else:
            otulist = [line.split('\t')[0] for line in open(self.option('abundance_table').prop['path'])][1:]  # 获取所有OTU/物种名
            sample = open(self.option('abundance_table').prop['path']).readline().rstrip().split('\t')[1:]
        if not self.option('newicktree').is_set:
            raise OptionError('必须提供输入文件:进化树')
        for i in otulist:
            if i not in self.option('newicktree').prop["sample"]:
                raise OptionError("丰度表中的物种在进化树中不存在:{}".format(i))
        if self.option("leaves_group").is_set:
            for i in self.option("leaves_group").prop['sample']:
                if i not in self.option("newicktree").prop['sample']:
                    raise OptionError("叶分组文件有叶信息不再进化树中")
        if self.option('sample_group').is_set:
            for i in sample:
                if i not in self.option("sample_group").prop['sample']:
                    raise OptionError("otu/物种分度表中样本不在样本分组文件中")


    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 5
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "距离矩阵计算结果输出目录"],
            ["fan.png", "png", "环形树图结果文件"],
            ["bar.png", "png", "带有bar图的树结果文件"]
        ])
        super(PlotTreeAgent, self).end()


class PlotTreeTool(Tool):

    def __init__(self, config):
        super(PlotTreeTool, self).__init__(config)
        self.plot_tree_bar_path = self.config.SOFTWARE_DIR + '/bioinfo/meta/scripts/plot-treeygbar.pl'
        self.plot_tree_fan_path = self.config.SOFTWARE_DIR + '/bioinfo/meta/scripts/plot-tree.pl'
        self.perl_path = 'program/perl/perls/perl-5.24.0/bin/perl '

    def run(self):
        """
        运行
        """
        super(PlotTreeTool, self).run()
        self.run_plot_tree()

    def run_plot_tree(self):
        cmd_fan = self.perl_path + self.plot_tree_fan_path + ' -i ' + self.option('newicktree').path + ' -o ' + self.output_dir + '/fan.png' + ' -tretype fan '
        if self.option('leaves_group').is_set:
            cmd_fan += ' -d ' + self.option('leaves_group').path
        cmd_bar = self.perl_path + self.plot_tree_bar_path + ' -i ' + self.option('newicktree').path + ' -o ' + self.output_dir + '/bar.png'
        cmd_bar += ' -t ' + self.option('abundance_table').path
        if self.option("sample_group").is_set:
            cmd_bar += ' -g ' + self.option('sample_group').path
        else:
            cmd_bar += ' -g ALL'
        heigth = len(self.option('newicktree').prop['sample']) / 10
        if heigth < 7:
            heigth = 7
        if self.option('leaves_group').is_set:
            cmd_bar += ' -d ' + self.option('leaves_group').path
        else:
            cmd_bar += ' -d ALL'
        fan_label_size = 1
        if len(self.option('newicktree').prop['sample']) > 500:
            fan_label_size = 0.5
        elif len(self.option('newicktree').prop['sample']) > 100:
            fan_label_size = 1 - len(self.option('newicktree').prop['sample']) / 100.0 / 10.0
        else:
            fan_label_size = 1
        cmd_bar += ' -h {} -eg 0.5 -w 10'.format(heigth)
        cmd_fan += ' -h {} -w {} -cex {}'.format(heigth / 2, heigth / 2, fan_label_size)
        fan_command = self.add_command('fan', cmd_fan)
        bar_command = self.add_command('bar', cmd_bar)
        fan_command.run()
        bar_command.run()
        self.wait()
        if bar_command.return_code == 0 and fan_command.return_code == 0:
            if os.path.isfile(self.output_dir + '/fan.png') and os.path.isfile(self.output_dir + '/bar.png'):
                self.end()
            else:
                self.set_error('结果文件没有正确生成')
        else:
            self.logger.info(bar_command.return_code)
            self.logger.info(fan_command.return_code)
            self.set_error('程序计算错误')
