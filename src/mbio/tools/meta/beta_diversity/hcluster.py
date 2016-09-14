# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import re
import subprocess
from biocluster.core.exceptions import OptionError


class HclusterAgent(Agent):
    """
    脚本plot-hcluster_tree.pl
    version v1.0
    author: shenghe
    last_modified:2016.3.24
    """

    def __init__(self, parent):
        super(HclusterAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
                "format": "meta.beta_diversity.distance_matrix"},
            {"name": "newicktree", "type": "outfile",
                "format": "meta.beta_diversity.newick_tree"},
            {"name": "linkage", "type": "string", "default": "average"}
        ]
        self.add_option(options)
        self.step.add_steps('hcluster')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.hcluster.start()
        self.step.update()

    def step_end(self):
        self.step.hcluster.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供输入距离矩阵表')
        else:
            self.option('dis_matrix').check()
        if self.option('linkage') not in ['average', 'single', 'complete']:
            raise OptionError('错误的层级聚类方式：%s' % self.option('linkage'))

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "层次聚类结果目录"],
            ["./hcluster.tre", "tre", "层次聚类树"]
        ])
        # print self.get_upload_files()
        super(HclusterAgent, self).end()


class HclusterTool(Tool):

    def __init__(self, config):
        super(HclusterTool, self).__init__(config)
        self._version = 'v2.1-20140214'  # plot-hcluster_tree.pl版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'bioinfo/statistical/scripts/plot-hcluster_tree.pl')

    def run(self):
        """
        运行
        """
        super(HclusterTool, self).run()
        self.run_hcluster()

    def run_hcluster(self):
        """
        运行plot-hcluster_tree.pl
        """
        real_dis_matrix = self.work_dir + '/distance_matrix.temp'
        self.newname_dict = self.change_sample_name(quotes=False, new_path=self.work_dir + '/distance_matrix.temp')  # 修改矩阵的样本名称为不含特殊符号的名称，返回一个旧名称对新名称的字典
        cmd = self.cmd_path
        cmd += ' -i %s -o %s -m %s' % (
            real_dis_matrix, self.work_dir, self.option('linkage'))
        self.logger.info('运行plot-hcluster_tree.pl程序计算Hcluster')
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 hc.cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 hc.cmd.r 文件失败')
            self.set_error('无法生成 hc.cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/hc.cmd.r' % self.work_dir, shell=True)
            self.logger.info('生成树文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成树文件失败')
            self.set_error('无法生成树文件')
        filename = self.work_dir + '/hcluster_tree_' + \
            os.path.basename(real_dis_matrix) + '_' + self.option('linkage') + '.tre'
        linkfile = self.output_dir + '/hcluster.tre'
        self.re_recover_name(self.newname_dict, filename, filename + '.temp')
        if os.path.exists(linkfile):
            os.remove(linkfile)
        os.link(filename + '.temp', linkfile)
        self.option('newicktree').set_path(linkfile)
        self.end()

    def change_sample_name(self, quotes=False, new_path=None):
        """
        修改矩阵的样本名称为不含特殊符号的名称，返回一个旧名称对新名称的字典
        """
        if not new_path:
            new_path = self.work_dir + '/distance_matrix.temp'
        old_matrix = open(self.option('dis_matrix').path, 'rb')
        name_dict = {}
        new_matrix = open(new_path, 'wb')
        frist_line = old_matrix.readline().rstrip().split('\t')[1:]
        if quotes:
            name_dict = {('\"' + name + '\"'): ('name' + str(frist_line.index(name))) for name in frist_line}
        else:
            name_dict = {name: ('name' + str(frist_line.index(name))) for name in frist_line}
        new_matrix.write('\t' + '\t'.join(name_dict.itervalues()) + '\n')
        for line in old_matrix:
            line_split = line.split('\t')
            if quotes:
                line_split[0] = name_dict['\"' + line_split[0] + '\"']
            else:
                line_split[0] = name_dict[line_split[0]]
            new_matrix.write('\t'.join(line_split))
        old_matrix.close()
        new_matrix.close()
        return name_dict

    def recover_name(self, namedict, treefile, newfile):
        """
        复原树文件中的名称
        """
        from Bio import Phylo
        from Bio.Phylo.NewickIO import NewickError
        if not isinstance(namedict, dict):
            raise Exception('复原树的枝名称需要旧名称和当前名称的字典')
        namedict = {item[1]: item[0] for item in namedict.iteritems()}
        if not isinstance(treefile, (str, unicode)):
            raise Exception('树文件的路径不是字符串')
        if not isinstance(newfile, (str, unicode)):
            raise Exception('新的树文件的路径不是字符串')
        try:
            tree = Phylo.read(treefile, 'newick')
        except IOError:
            raise Exception('复原树文件时找不到树文件：%s' % treefile)
        except NewickError:
            raise Exception('树文件无法用newick格式解析：%s' % treefile)
        terminals = tree.get_terminals()
        for terminal in terminals:
            if terminal.name not in namedict:
                raise Exception('树的枝名称：%s在旧名称和新名称字典中不存在' % terminal.name)
            terminal.name = namedict[terminal.name]
        Phylo.write(tree, newfile, 'newick')
        return True

    def re_recover_name(self, namedict, treefile, newfile):
        """
        用正则的方式替换复原树文件中的名称
        """
        if not isinstance(namedict, dict):
            raise Exception('复原树的枝名称需要旧名称和当前名称的字典')
        namedict = {item[1]: item[0] for item in namedict.iteritems()}
        if not isinstance(treefile, (str, unicode)):
            raise Exception('树文件的路径不是字符串')
        if not isinstance(newfile, (str, unicode)):
            raise Exception('新的树文件的路径不是字符串')
        try:
            with open(treefile, 'rb') as f, open(newfile, 'wb') as w:
                tree = f.readline()
                for item in namedict.iteritems():
                    tree = re.sub(item[0] + ':', item[1] + ':', tree)
                w.write(tree)
        except IOError, e:
                raise Exception('聚类树文件无法找到或者无法打开：%s' % e)
