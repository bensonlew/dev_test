# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

""""""

import os
import re
from biocluster.workflow import Workflow
from bson import ObjectId
from mbio.packages.beta_diversity.filter_newick import get_level_newicktree
import datetime
import json
from collections import defaultdict


class PlotTreeWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(PlotTreeWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "level", "type": 'int', "default": 9},
            {"name": "otu_id", "type": 'string', "default": ''},
            {"name": "params", "type": 'string', "default": ''},
            {"name": "group_id", "type": 'string'},
            {"name": "group_detail", "type": 'string', "default": ""},
            {"name": "color_level_id", "type": 'int', "default": 0},
            {"name": "sample_group", "type": "infile", "format": "meta.otu.group_table"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def check(self):
        if self.option("level") >= self.option("color_level_id"):
            raise Exception("颜色设置id必须大于选择的分类水平")
        if not self.option("otu_id"):
            raise Exception("必须提供OTU的主表id")

    def run(self):
        print 'plot tree stat run'
        otu_format = self.work_dir + '/format_otu_table.xls'
        species_format = self.work_dir + '/species_group.xls'
        tree_file = self.work_dir + '/format.tre'
        if self.option('color_level_id'):
            self.format_otu_table(otu_format, species_format)
        else:
            self.format_otu_table(otu_format)
        self.get_newicktree(tree_file)
        options = {
            "abundance_table": otu_format,
            "newicktree": tree_file
        }
        if self.option("group_id") not in ['all', 'All', 'ALL', None]:
            options['sample_group'] = self.option("sample_group")
        if self.option("color_level_id"):
            options["leaves_group"] = species_format
        self.task = self.add_tool("graph.plot_tree")
        self.task.set_options(options)
        self.task.on('end', self.set_db)
        self.task.run()
        print 'task stat run plot tree tool'
        self.output_dir = self.task.output_dir
        super(PlotTreeWorkflow, self).run()

    def set_db(self):
        """
        """
        print 'stat set db'
        api_tree = self.api.tree_picture
        main_id = api_tree.add_tree_picture(self.output_dir, major=True,
                                            params=json.loads(self.option('params')),
                                            otu_id=self.option('otu_id'),
                                            level=self.option('level'),
                                            name='tree_{}'.format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")))
        self.add_return_mongo_id('sg_tree_picture', str(main_id))
        self.end()
        pass

    def format_otu_table(self, out_otu_file, out_species_group_file=None):
        """
        配合进化树，拆分otu表名称
        """
        self.rm_species = []
        species_dict = defaultdict(list)
        species_index = self.option("color_level_id") - 1
        with open(self.option("otu_table").path) as f, open(out_otu_file, 'w') as w:
            w.write(f.readline())
            for i in f:
                line_split = re.split('\t', i, maxsplit=1)
                name_split = line_split[0].split(';')
                new_name = name_split[-1].strip().replace(':', '-')
                if sum([int(i) for i in line_split[1].strip().split('\t')]) == 0:  # hesheng 20161115 去除所有样本为0的情况，目前to_file没有相关功能，暂时添加
                    self.rm_species.append(new_name)
                    continue
                if out_species_group_file:
                    species_dict[name_split[species_index]].append(new_name)
                w.write(new_name + '\t' + line_split[1])
        if out_species_group_file:
            with open(out_species_group_file, 'w') as w:
                w.write('#species/OTU\tGROUP\n')
                for m, n in species_dict.iteritems():
                    m = m.strip()
                    for i in n:
                        w.write(i + '\t' + m + '\n')

    def get_newicktree(self, output_file):
        tree = get_level_newicktree(self.option("otu_id"), level=self.option('level'), tempdir=self.work_dir)
        if '(' not in tree:
            raise Exception('进化树水平选择过高，导致没有树枝， 请选择较低的水平')

        def simple_name(name):
            name = name.group()
            return name.split(';')[-1].strip().replace(':', '-').strip('\'')  # replace用于去掉名称中带有冒号
        format_tree = re.sub(r'\'(.+?)\'', simple_name, tree)
        format_tree = re.sub(r'(\[)', '--temp_replace_left--', format_tree)  # 中括号在phylo中的读取会被特别识别，出现错误，后续对中括号进行暂时替换处理
        format_tree = re.sub(r'(\])', '--temp_replace_right--', format_tree)
        self.logger.info(format_tree)
        from Bio import Phylo
        open(output_file + '.temp', 'w').write(format_tree)
        newick_tree = Phylo.read(output_file + '.temp', 'newick')
        self.logger.info('移除物种/OTU:{}'.format(self.rm_species))
        for i in self.rm_species:
            newick_tree.prune(i)
        Phylo.write(newick_tree, output_file + '.temp2', 'newick')
        temp_tree = open(output_file + '.temp2').read()
        temp_tree = re.sub(r'(--temp_replace_left--)', '[', temp_tree)
        temp_tree = re.sub(r'(--temp_replace_right--)', ']', temp_tree)
        temp_tree = re.sub(r'\'', '', temp_tree)
        temp_tree = re.sub(r'\"', '', temp_tree)
        with open(output_file, 'w') as w:
            w.write(temp_tree)
        self.logger.info(open(output_file).read())


    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "距离矩阵计算结果输出目录"],
            ["fan.png", "png", "环形树图结果文件"],
            ["bar.png", "png", "带有bar图的树结果文件"],
            ["fan.pdf", "pdf", "环形树图结果文件"],
            ["bar.pdf", "pdf", "带有bar图的树结果文件"]
            ])
        print self.get_upload_files()
        super(PlotTreeWorkflow, self).end()



if __name__ == '__main__':
    a = get_level_newicktree('57fd7c2b17b2bf377d2d6dae', level=8)
    print a

    def simple(name):
        name = name.group()
        return name.split(';')[-1].strip().replace(':', '-')  # replace用于去掉名称中带有冒号
    b = re.sub(r'\'(.+?)\'', simple, a)
    print b
