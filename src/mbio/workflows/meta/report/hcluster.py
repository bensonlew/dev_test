# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""距离矩阵层级聚类"""

import datetime
from biocluster.workflow import Workflow
from mbio.packages.beta_diversity.filter_newick import get_level_newicktree
from bson import ObjectId
import re
import os
import json
import shutil


class HclusterWorkflow(Workflow):
    """
    报告中调用距离矩阵计算样本层级聚类数使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(HclusterWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "dist_method", "type": "string", "default": 'bray_curtis'},
            {"name": "hcluster_method", "type": "string", "default": 'average'},
            {"name": "level", "type": 'int', "default": 9},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "main_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "otu_id", "type": "string"},
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.dist = self.add_tool("meta.beta_diversity.distance_calc")
        self.hcluster = self.add_tool("meta.beta_diversity.hcluster")

    def run(self):
        if 'unifrac' in self.option('dist_method'):
            # 查找OTU表对应的进化树
            if self.option('level') != 9:
                newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                                  tempdir=self.work_dir, return_file=False, bind_obj=self)
                all_find = re.findall(r'\'.+?\'', newicktree)  # 找到所有带引号的进化树中复杂的名称
                for n, m in enumerate(all_find):
                    all_find[n] = m.strip('\'')
                all_find = dict((i[1], i[0]) for i in enumerate(all_find))  # 用名称做键，找到的位置数字做值

                def match_newname(matchname):
                    '随着自身被调用，自身的属性count随调用次数增加，返回OTU加次数，用于重命名进化树复杂的名称'
                    if hasattr(match_newname, 'count'):
                        match_newname.count = match_newname.count + 1
                    else:
                        match_newname.count = 1
                    return 'OTU' + str(match_newname.count)  # 后面替换OTU中名称用同样的命名规则
                newline = re.sub(r'\'.+?\'', match_newname, newicktree)  # 替换树种的复杂名称用 OTU 加数字代替 , 选哟注意的是这里的sub查找与findall查到方式是一致的
                temp_tree_file = self.work_dir + '/temp.tree'
                tempfile = open(temp_tree_file, 'w')
                tempfile.write(newline)
                tempfile.close()
                self.logger.info('get_newick:' + temp_tree_file)
                otu_table = self.option('otu_table').path
                temp_otu_file = self.option('otu_table').path + '.temp'
                all_lines = open(otu_table, 'r').readlines()
                if len(all_lines) < 3:
                    raise Exception('分类水平：%s,otu表数据少于2行：%s' % (self.option('level'), len(all_lines)))
                new_all = []
                new_all.append(all_lines[0])
                for line in all_lines[1:]:  # 遍历OTU表，将OTU表的复杂OTU名称改为之前find到的复杂名称对应的字典
                    name = line.split('\t')
                    if name[0] not in all_find:
                        raise Exception('OTU表中存在不是直接通过组合原始表分类名称的OTU名：%s' % name[0])
                    name[0] = 'OTU' + str(all_find[name[0]] + 1)
                    new_all.append('\t'.join(name))
                otu_file_temp = open(temp_otu_file, 'w')
                otu_file_temp.writelines(new_all)
                otu_file_temp.close()
                options = {
                    'method': self.option('dist_method'),
                    'otutable': temp_otu_file,
                    'newicktree': temp_tree_file
                }
            else:
                newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                                  tempdir=self.work_dir, return_file=False, bind_obj=self)
                temp_tree_file = self.work_dir + '/temp.tree'
                tempfile = open(temp_tree_file, 'w')
                tempfile.write(newicktree)
                tempfile.close()
                otu_table = self.option('otu_table').path
                temp_otu_file = self.option('otu_table').path + '.temp'
                all_lines = open(otu_table, 'r').readlines()
                new_all = []
                new_all.append(all_lines[0])
                for line in all_lines[1:]:  # OTU表中有复杂的名称OTU名称，包含进化物种类型，进化树种只有OTU名称
                    name = line.split('\t')
                    name[0] = name[0].split(';')[-1].strip()
                    new_all.append('\t'.join(name))
                otu_file_temp = open(temp_otu_file, 'w')
                otu_file_temp.writelines(new_all)
                otu_file_temp.close()
                options = {
                    'method': self.option('dist_method'),
                    'otutable': temp_otu_file,
                    'newicktree': temp_tree_file
                }
        else:
            options = {
                'method': self.option('dist_method'),
                'otutable': self.option('otu_table')
            }
        self.dist.set_options(options)
        self.dist.on('end', self.run_hcluster)
        self.hcluster.on('end', self.set_db)
        self.dist.run()
        super(HclusterWorkflow, self).run()


    def run_hcluster(self):
        options = {
            'linkage': self.option('hcluster_method'),
            'dis_matrix': self.dist.option('dis_matrix')
        }
        self.hcluster.set_options(options)
        self.hcluster.run()



    def set_db(self):
        """
        保存结果树结果到mongo数据库中
        """
        params_json = json.loads(self.option('params'))
        api_distance = self.api.distance
        matrix_path = self.dist.output_dir + '/' + os.listdir(self.dist.output_dir)[0]
        final_matrix_path = os.path.join(self.output_dir, os.listdir(self.dist.output_dir)[0])
        shutil.copy2(matrix_path, final_matrix_path)
        if not os.path.isfile(matrix_path):
            raise Exception("找不到报告文件:{}".format(matrix_path))
        dist_name = 'Distance_{}_{}'.format(self.option('dist_method'),
                                            datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        matrix_id = api_distance.add_dist_table(matrix_path,
                                                major=True,
                                                name=dist_name,
                                                level=self.option('level'),
                                                otu_id=self.option('otu_id'),
                                                params=params_json)
        # self.add_return_mongo_id('sg_beta_specimen_distance', matrix_id)
        api_newick = self.api.newicktree
        collection = api_newick.db["sg_beta_specimen_distance"]
        newick_fath = self.hcluster.output_dir + "/hcluster.tre"
        final_newick_path = os.path.join(self.output_dir, "hcluster.tre")
        shutil.copy2(newick_fath, final_newick_path)
        if not os.path.isfile(newick_fath):
            raise Exception("找不到报告文件:{}".format(newick_fath))
        return_id = api_newick.add_tree_file(newick_fath, major=False, tree_id=self.option('main_id'),
                                             update_dist_id=matrix_id)
        collection.update_one({"_id": ObjectId(matrix_id)}, {'$set': {'newick_tree_id': ObjectId(return_id)}})
        self.end()

    def end(self):
        result_dir_hucluster = self.add_upload_dir(self.hcluster.output_dir)
        result_dir_hucluster.add_relpath_rules([
            [".", "", "层次聚类结果目录"],
            ["./hcluster.tre", "tre", "层次聚类树"]
            ])
        result_dir_distance = self.add_upload_dir(self.dist.output_dir)
        result_dir_distance.add_relpath_rules([
            [".", "", "距离矩阵计算结果输出目录"],
        ])
        result_dir_distance.add_regexp_rules([
            [r'%s.*\.xls' % self.option('dist_method'), 'xls', '样本距离矩阵文件']
        ])
        super(HclusterWorkflow, self).end()
