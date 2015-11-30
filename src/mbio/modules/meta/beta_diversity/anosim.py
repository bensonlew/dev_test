# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError


class AnosimModule(Module):
    def __init__(self, work_id):
        super(AnosimModule, self).__init__(work_id)
        self.box = self.add_tool('meta.beta_diversity.distance_box')
        self.anosim = self.add_tool('meta.beta_diversity.anosim')
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "phy_newick", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"},
            {"name": "permutations", "type": "int", "default": 999},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"}
        ]
        self.add_option(options)

    def check_options(self):
        if self.option('permutations') < 0 or self.option('permutations') > 10000:
            raise OptionError('参数permutations：%s 不在范围内(0-10000)' % self.option('permutations'))
        samplelist = open(self.option('otutable').prop['path']).readline().strip().split('\t')[1:]
        for sample in self.option('group').prop['sample']:
            if sample not in samplelist:
                raise OptionError('分组文件的样本(%s)在otu表的样本中不存在' % sample)

    def matrix_run(self):
        """
        运行计算距离矩阵
        :return:
        """
        matrix = self.add_tool('meta.beta_diversity.distance_calc')
        if self.option('phy_newick').is_set:
            matrix.set_options({'method': self.option('method'),
                                'otutable': self.option('otutable').prop['path'],
                                'newicktree': self.option('phy_newick').prop['path']})
        else:
            matrix.set_options({'method': self.option('method'),
                                'otutable': self.option('otutable').prop['path']})
        self.on_rely(matrix, self.anosim_run)
        self.on_rely(matrix, self.box_run)
        matrix.on('end', self.set_output, 'distance')
        matrix.run()

    def anosim_run(self, relyobj):
        """
        计算anosim分析和adonis分析
        :param relyobj: 绑定对象
        :return:
        """
        output_file_obj = relyobj.rely[0].option('dis_matrix')
        self.anosim.set_options({'dis_matrix': output_file_obj.prop['path'],
                                 'permutations': self.option('permutations'),
                                 'group': self.option('group').prop['path']})
        self.anosim.on("end", self.set_output, 'anosim')
        self.anosim.run()

    def box_run(self, relyobj):
        """
        计算绘制距离箱线图图的数据
        :param relyobj: 绑定对象
        :return:
        """
        output_file_obj = relyobj.rely[0].option('dis_matrix')
        self.box.set_options({'dis_matrix': output_file_obj.prop['path'],
                              'group': self.option('group').prop['path']})
        self.box.on("end", self.set_output, 'box_data')
        self.box.run()

    def set_output(self, event):
        """
        设置输出
        :param event: 触发事件内容
        :return:
        """
        obj_bind = event['bind_object']
        if event['data'] == 'anosim':
            self.linkdir(obj_bind.output_dir, 'Anosim')
        elif event['data'] == 'box_data':
            self.linkdir(obj_bind.output_dir, 'DistanceBoxData')
        elif event['data'] == 'distance':
            self.linkdir(obj_bind.output_dir, 'DistanceCalc')
        else:
            pass

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def run(self):
        self.matrix_run()
        self.on_rely([self.box, self.anosim], self.end)
        super(AnosimModule, self).run()

