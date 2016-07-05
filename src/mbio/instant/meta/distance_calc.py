# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# import os
import subprocess
# import shutil
import re
from biocluster.config import Config
from mbio.packages.beta_diversity.filter_newick import get_level_newicktree


class DistanceCalcInstant(object):
    def __init__(self, bindObject):
        self.logger = bindObject.logger
        self.option = bindObject.option
        self.work_dir = bindObject.work_dir
        # self.R_path = os.path.join(Config().SOFTWARE_DIR, "R-3.2.2/bin/R")
        self._version = 1.0
        self.success = False
        self.damageinfo = None
        self.output_file = None

    def run(self):
        self.distanc_calc()

    def distanc_calc(self):
        """
        """
        cmd = Config().SOFTWARE_DIR + '/Python/bin/python ' + Config().SOFTWARE_DIR + '/bioinfo/script/distance_calc.py'

        if 'unifrac' in self.option['method']:
            self.logger.info('获取进化树')
            otufile, treefile = self.newick_tree()
            self.logger.info('获取进化树结束')
            cmd += ' -i {} -o {} -m {} -t {}'.format(otufile, self.work_dir + '/output/distace.txt',
                                                     self.option['method'], treefile)
        else:
            cmd += ' -i {} -o {} -m {}'.format(self.option['otu_fp'].path,
                                               self.work_dir + '/output/distace.txt', self.option['method'])
        self.logger.info('开始运行距离计算')
        try:
            subprocess.check_call(cmd, shell=True)
            self.success = True
            self.output_file = self.work_dir + '/output/distace.txt'
        except Exception as e:
            self.success = False
            self.damageinfo = '距离矩阵计算出错：{}'.format(e)
        self.logger.info('距离计算结束')

    def newick_tree(self):
        """"""
        if self.option['level_id'] != 9:
            newicktree = get_level_newicktree(self.option['otu_id'], level=self.option['level_id'],
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
            otu_table = self.option['otu_fp'].path
            temp_otu_file = self.option['otu_fp'].path + '.temp'
            all_lines = open(otu_table, 'r').readlines()
            if len(all_lines) < 3:
                raise Exception('分类水平：%s,otu表数据少于2行：%s' % (self.option['level_id'], len(all_lines)))
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
            return temp_otu_file, temp_tree_file
        else:
            newicktree = get_level_newicktree(self.option['otu_id'], level=self.option['level_id'],
                                              tempdir=self.work_dir, return_file=False, bind_obj=self)
            temp_tree_file = self.work_dir + '/temp.tree'
            tempfile = open(temp_tree_file, 'w')
            tempfile.write(newicktree)
            tempfile.close()
            otu_table = self.option('otu_fp').path
            temp_otu_file = self.option('otu_fp').path + '.temp'
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
            return temp_otu_file, temp_tree_file
