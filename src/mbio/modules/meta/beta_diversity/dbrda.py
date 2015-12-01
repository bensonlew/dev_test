# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError
# from biocluster.core.function import load_class_by_path


class DbrdaModule(Module):
    def __init__(self, work_id):
        super(DbrdaModule, self).__init__(work_id)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "phy_newick", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"}
        ]
        self.add_option(options)

    def check_options(self):
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
        self.on_rely(matrix, self.dbrda_run)
        matrix.on('end', self.set_output, 'distance')
        matrix.run()

    def dbrda_run(self, relyobj):
        """
        运行dbrda-tool
        :param relyobj: 依赖对象
        :return:
        """
        output_file_obj = relyobj.rely[0].option('dis_matrix')
        dbrda = self.add_tool('meta.beta_diversity.dbrda')
        dbrda.set_options({'dis_matrix': output_file_obj.prop['path'],
                           'group': self.option('group').prop['path']})
        dbrda.on("end", self.set_output, 'dbrda')
        dbrda.run()

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'dbrda':
            self.linkdir(obj.output_dir, 'Dbrda')
            self.end()
        elif event['data'] == 'distance':
            self.linkdir(obj.output_dir, 'DistanceCalc')
        else:
            pass

    def run(self):
        self.matrix_run()
        super(DbrdaModule, self).run()

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
