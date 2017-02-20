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
            {"name": "main_id", "type": 'string', "default": ''},
            {"name": "params", "type": 'string', "default": ''},
            {"name": "group_id", "type": 'string'},
            {"name": "update_info", "type": 'string'},
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
        self.start_listener()
        self.fire("start")
        self.species = []
        otu_format = self.work_dir + '/format_otu_table.xls'
        species_format = self.work_dir + '/species_group.xls'
        tree_file = self.work_dir + '/format.tre'
        if self.option('color_level_id'):
            if self.option("group_id") not in ['all', 'All', 'ALL', None]:
                self.format_group_otu_table(otu_format, species_format)
            else:
                self.format_otu_table(otu_format, species_format)
        else:
            if self.option("group_id") not in ['all', 'All', 'ALL', None]:
                self.format_group_otu_table(otu_format)
            else:
                self.format_otu_table(otu_format)
        self.get_newicktree(tree_file)

        self.set_db()

    def format_group_otu_table(self, out_otu_file, out_species_group_file=None):
        """
        """
        species_dict = defaultdict(list)
        species_index = self.option("color_level_id") - 1
        with open(self.option('otu_table').path) as f, open(self.option("sample_group").path) as g, open(out_otu_file, 'w') as w:
            g.readline()
            group = {}  # 样本分组
            for i in g:
                split_i = i.strip().split('\t')
                group[split_i[0]] = split_i[1]
            group_names = list(set(group.values()))
            group_index = dict(zip(group_names, [[] for i in range(len(group_names))]))  # 样本index
            group_value = dict(zip(group_names, [[] for i in range(len(group_names))]))  # 包含样本值列表
            all_sample = f.readline().rstrip().split('\t')[1:]
            for m, n in enumerate(all_sample):
                group_index[group[n]].append(m + 1)
            w.write('ID\t' + '\t'.join(group_names) + '\n')
            for i in f:
                line_split = re.split('\t', i.strip())
                name_split = line_split[0].split(';')
                new_name = name_split[-1].strip().replace(':', '-')  # 协同树去除名称中有冒号的样本名
                self.species.append(new_name)
                if out_species_group_file:
                    species_dict[name_split[species_index]].append(new_name)
                for key, indexs in group_index.iteritems():
                    for index in indexs:
                        group_value[key].append(int(line_split[index]))
                new_line = new_name
                for group_name in group_names:
                    new_line += '\t' + str(sum(group_value[group_name]))
                    group_value[group_name] = []
                w.write(new_line + '\n')
            if out_species_group_file:
                with open(out_species_group_file, 'w') as w:
                    w.write('#species/OTU\tGROUP\n')
                    for m, n in species_dict.iteritems():
                        m = m.strip()
                        for i in n:
                            w.write(i + '\t' + m + '\n')


    def set_db(self):
        """
        """
        output_otu = self.output_dir + '/species_table.xls'
        output_tree = self.output_dir + '/phylo_tree.tre'
        output_species = self.output_dir + '/species_group.xls'
        if os.path.exists(output_otu):
            os.remove(output_otu)
        os.link(self.work_dir + '/format_otu_table.xls', self.output_dir + '/species_table.xls')
        if os.path.exists(output_tree):
            os.remove(output_tree)
        os.link(self.work_dir + '/format.tre', self.output_dir + '/phylo_tree.tre')
        if os.path.exists(self.work_dir + '/species_group.xls'):
            if os.path.exists(output_species):
                os.remove(output_species)
            os.link(self.work_dir + '/species_group.xls', self.output_dir + '/species_group.xls')
        api_tree = self.api.phylo_tree
        api_tree.add_phylo_tree_info(self.option('main_id'))
        self.end()

    def format_otu_table(self, out_otu_file, out_species_group_file=None):
        """
        配合进化树，拆分otu表名称
        """
        # self.rm_species = []
        species_dict = defaultdict(list)
        species_index = self.option("color_level_id") - 1
        with open(self.option("otu_table").path) as f, open(out_otu_file, 'w') as w:
            w.write(f.readline())
            for i in f:
                line_split = re.split('\t', i, maxsplit=1)
                name_split = line_split[0].split(';')
                new_name = name_split[-1].strip().replace(':', '-')
                if sum([int(i) for i in line_split[1].strip().split('\t')]) == 0:  # hesheng 20161115 去除所有样本为0的情况，目前to_file没有相关功能，暂时添加
                    # self.rm_species.append(new_name)
                    raise Exception('存在全部物种/OTU代表序列数量都为0的情况')
                    continue
                self.species.append(new_name)
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
        from Bio import Phylo
        open(output_file + '.temp', 'w').write(format_tree)
        newick_tree = Phylo.read(output_file + '.temp', 'newick')
        leaves = newick_tree.get_terminals()
        for i in leaves:
            i.name = i.name.replace('--temp_replace_left--', '[')
            i.name = i.name.replace('--temp_replace_right--', ']')
            if i.name not in self.species:
                newick_tree.prune(i.name)
                self.logger.info('移除物种/OTU:{}'.format(i.name))
            # newick_tree.prune(i)
        Phylo.write(newick_tree, output_file + '.temp2', 'newick')
        temp_tree = open(output_file + '.temp2').read()

        def replace_fun(matched):
            try:
                self.logger.info('shenghe_test logger: 存在引号')
            except:
                print 'logger:测试错误'
            return ''
        temp_tree = re.sub(r'\'', replace_fun, temp_tree)
        temp_tree = re.sub(r'\"', replace_fun, temp_tree)
        with open(output_file, 'w') as w:
            w.write(temp_tree)
        self.logger.info(open(output_file).read())


    def end(self):
        import time
        time.sleep(10)
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "距离矩阵计算结果输出目录"],
            ["species_table.xls", "txt", "物种样本统计表"],
            ["phylo_tree.tre", "tree", "进化树"],
            ["species_group.xls", "txt", "物种在高层级的分类表"]
            ])
        self._upload_result()
        self._import_report_data()
        self.step.finish()
        self.step.update()
        self.logger.info("运行结束!")
        self._update("end")
        self.set_end()
        self.fire('end')
        # super(PlotTreeWorkflow, self).end()



if __name__ == '__main__':
    a = get_level_newicktree('57fd7c2b17b2bf377d2d6dae', level=8)
    print a

    def simple(name):
        name = name.group()
        return name.split(';')[-1].strip().replace(':', '-')  # replace用于去掉名称中带有冒号
    b = re.sub(r'\'(.+?)\'', simple, a)
    print b
