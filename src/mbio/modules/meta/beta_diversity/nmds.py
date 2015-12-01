# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.module import Module
import os
# from biocluster.core.exceptions import OptionError
# from biocluster.core.function import load_class_by_path


class NmdsModule(Module):
    def __init__(self, work_id):
        super(NmdsModule, self).__init__(work_id)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "phy_newick", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"}
        ]
        self.add_option(options)

    def check_options(self):
        pass

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
        self.on_rely(matrix, self.nmds_run)
        matrix.on('end', self.set_output, 'distance')
        matrix.run()

    def nmds_run(self, relyobj):
        """
        运行nmds-tool
        :param relyobj:  依赖对象
        :return:
        """
        output_file_obj = relyobj.rely[0].option('dis_matrix')
        nmds = self.add_tool('meta.beta_diversity.nmds')
        nmds.set_options({'dis_matrix': output_file_obj.prop['path']})
        nmds.on("end", self.set_output, 'nmds')
        nmds.run()

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'nmds':
            self.linkdir(obj.output_dir, 'Nmds')
            self.end()
        elif event['data'] == 'distance':
            self.linkdir(obj.output_dir, 'DistanceCalc')
        else:
            pass

    def run(self):
        self.matrix_run()
        super(NmdsModule, self).run()

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
