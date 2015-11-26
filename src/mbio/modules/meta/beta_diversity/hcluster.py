# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.module import Module
import os
# from biocluster.core.exceptions import OptionError
# from biocluster.core.function import load_class_by_path


class HclusterModule(Module):
    def __init__(self, work_id):
        super(HclusterModule, self).__init__(work_id)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            # {"name": "dis_matrix", "type": "outfile",
            #  "format": "meta.beta_diversity.distance_matrix"},
            {"name": "phy_newick", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"},
            {"name": "dis_newick", "type": "outfile",
                "format": "meta.beta_diversity.newick_tree"},
            {"name": "linkage", "type": "string", "default": "average"}
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
        self.on_rely(matrix, self.hcluster_run)
        matrix.run()
    def hcluster_run(self, relyobj):
        """
        运行计算层级聚类
        :relyobj:
        :return:
        """
        # matrix_agent_class = load_class_by_path('meta.beta_diversity.distance_calc')
        output_file_obj = relyobj.rely[0].option('dis_matrix')
        hcluster = self.add_tool('meta.beta_diversity.hcluster')
        hcluster.set_options({'dis_matrix': output_file_obj.prop['path'],
                              'linkage': self.option('linkage')})
        hcluster.on("end", self.set_output)
        hcluster.run()

    def set_output(self, event):
        hcluster = event['bind_object']
        linkfile = self.output_dir + '/hcluster.tre'
        tree = hcluster.option('newicktree').prop['path']
        if os.path.exists(linkfile):
            os.remove(linkfile)
        self.logger.info(tree)
        self.logger.info(linkfile)
        os.link(tree, linkfile)
        self.option('dis_newick', linkfile)
        self.end()

    def run(self):
        self.matrix_run()
        super(HclusterModule, self).run()

