# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""otu表的样本距离计算"""

import os
import re
from biocluster.workflow import Workflow
from bson import ObjectId
from mbio.packages.beta_diversity.filter_newick import *


class DistanceCalcWorkflow(Workflow):
    """
    报告中调用otu计算样本距离时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DistanceCalcWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "method", "type": "string", "default": 'bray_curtis'},
            {"name": "update_info", "type": "string"},
            {"name": "otu_id", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "matrix_id", "type": "string"},
            # {"name": "matrix_out", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_tool("meta.beta_diversity.distance_calc")
        self.logger.info(self.option('otu_file'))
        # temp_otu_file = self.option('otu_file').path + '.temp'
        # with open(self.option('otu_file').path, 'r') as ff, open(temp_otu_file, 'w') as ww:
        #     for line in ff:
        #         linesplit = line.split('\t')
        #         linesplit[0] = '\'' + linesplit[0] + '\''
        #         ww.write('\t'.join(linesplit))
        if 'unifrac' in self.option('method'):
            newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                              tempdir=self.work_dir, return_file=False, bind_obj=self)
            all_find = re.findall(r'\'.+?\'', newicktree)
            for n, m in enumerate(all_find):
                all_find[n] = m.strip('\'')
            all_find = dict((i[1], i[0]) for i in enumerate(all_find))

            def match_newname(matchname):
                if hasattr(match_newname, 'count'):
                    match_newname.count = match_newname.count + 1
                else:
                    match_newname.count = 1
                print match_newname.count
                return 'OTU' + str(match_newname.count)
            newline = re.sub(r'\'.+?\'', match_newname, newicktree)
            temp_tree_file = self.work_dir + '/temp.tree'
            tempfile = open(temp_tree_file, 'w')
            tempfile.write(newline)
            tempfile.close()
            self.logger.info('get_newick:' + temp_tree_file)
            otu_table = self.option('otu_file').path
            temp_otu_file = self.option('otu_file').path + '.temp'
            all_lines = open(otu_table, 'r').readlines()
            if len(all_lines) < 3:
                raise Exception('分类水平：%s,otu表数据少于2行：%s' % (self.option('level'), len(all_lines)))
            new_all = []
            new_all.append(all_lines[0])
            for line in all_lines[1:]:
                name = line.split('\t')
                name[0] = 'OTU' + str(all_find[name[0]] + 1)
                new_all.append('\t'.join(name))
            otu_file_temp = open(temp_otu_file, 'w')
            otu_file_temp.writelines(new_all)
            otu_file_temp.close()
            options = {
                'method': self._sheet.option('method'),
                'otutable': temp_otu_file,
                'newicktree': temp_tree_file
            }
        else:
            options = {
                'method': self.option('method'),
                'otutable': self.option('otu_file')
            }
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(DistanceCalcWorkflow, self).run()

    def get_phylo_tree(self):
        tree_path = ''
        return tree_path

    def set_db(self):
        """
        保存结果距离矩阵表到mongo数据库中
        """
        api_distance = self.api.distance
        matrix_path = self.output_dir + '/' + os.listdir(self.output_dir)[0]
        if not os.path.isfile(matrix_path):
            raise Exception("找不到报告文件:{}".format(matrix_path))
        api_distance.add_dist_table(matrix_path, dist_id=ObjectId(self.option('matrix_id')), )
        self.logger.info('运行self.end')
        self.end()
