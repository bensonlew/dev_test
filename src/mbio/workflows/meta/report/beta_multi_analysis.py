# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""beta多元分析"""

import os
import re
from biocluster.workflow import Workflow
from mbio.packages.beta_diversity.filter_newick import *
from bson import ObjectId


class BetaMultiAnalysisWorkflow(Workflow):
    """
    报告中调用beta多元分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(BetaMultiAnalysisWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "analysis_type", "type": "string", "default": 'pca'},
            {"name": "dist_method", "type": "string", "default": 'bray_curtis'},
            {"name": "update_info", "type": "string"},
            {"name": "otu_id", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "multi_analysis_id", "type": "string"},
            {"name": "env_file", "type": "infile", "format": "meta.env_table"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            # {"name": "matrix_out", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_module("meta.beta_diversity.beta_diversity")
        self.logger.info(self.option('otu_file').path)
        options = {
            'analysis': self.option('analysis_type'),
            'dis_method': self.option('dist_method'),
            'otutable': self.option('otu_file')

        }
        if self.option('env_file').is_set:
            options['envtable'] = self.option('env_file').path
        elif self.option('group_file').is_set:
            options['group'] = self.option('group_file').path
        else:
            pass
        if self.option('analysis_type') in ['pcoa', 'nmds', 'dbrda']:
            if 'unifrac' in self.option('dist_method'):
                newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                                  tempdir=self.work_dir, return_file=False, bind_obj=self)
                all_find = re.findall(r'\'.+?\'', newicktree)
                for n, m in enumerate(all_find):
                    all_find[n] = m.strip('\'')
                all_find = dict((i[1], i[0]) for i in enumerate(all_find))
                # for test
                myfile = open(self.work_dir + '/newicktree.test', 'w')
                myfile.writelines([i + '\n' for i in all_find.iterkeys()])
                myfile.close()
                # test ending

                self.logger.info(str(all_find))
                self.logger.info(len(all_find.values()))

                def match_newname(matchname):
                    if hasattr(match_newname, 'count'):
                        match_newname.count = match_newname.count + 1
                    else:
                        match_newname.count = 1
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
                self.logger.info(len(all_lines))
                new_all = []
                new_all.append(all_lines[0])
                for line in all_lines[1:]:
                    name = line.split('\t')
                    if all_find.has_key(name[0]):
                        name[0] = 'OTU' + str(all_find[name[0]] + 1)
                    new_all.append('\t'.join(name))
                otu_file_temp = open(temp_otu_file, 'w')
                otu_file_temp.writelines(new_all)
                otu_file_temp.close()
                options['otutable'] = temp_otu_file
                options['phy_newick'] = temp_tree_file
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(BetaMultiAnalysisWorkflow, self).run()

    def set_db(self):
        """
        保存结果距离矩阵表到mongo数据库中
        """
        # api_distance = self.api.distance
        # matrix_path = self.output_dir + '/' + os.listdir(self.output_dir)[0]
        # if not os.path.isfile(matrix_path):
        #     raise Exception("找不到报告文件:{}".format(matrix_path))
        # api_distance.add_dist_table(matrix_path, dist_id=ObjectId(self.option('matrix_id')), )
        self.logger.info('运行self.end')
        self.end()
