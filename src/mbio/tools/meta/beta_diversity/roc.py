# -*- coding: utf-8 -*-
# __author__ = "JieYao"

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import string
import types
import subprocess
from biocluster.core.exceptions import OptionError


class RocAgent(Agent):
    """
    需要calc_roc.pl
    version v1.1
    author: JieYao
    last_modifued:2016.08.22
    """

    def __init__(self, parent):
        super(RocAgent, self).__init__(parent)
        options = [
            {"name": "mode", "type": "int", "default": 1},
            {"name": "genus_table", "type": "string"},
            {"name": "group_table", "type": "string"},
            {"name": "method", "type": "string", "default": ""},
            {"name": "name_table", "type": "string", "default": ""},
            {"name": "top_n", "type": "int", "default": 20}
        ]
        self.add_option(options)
        self.step.add_steps('RocAnalysis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.RocAnalysis.start()
        self.step.update()

    def step_end(self):
        self.step.RocAnalysis.finish()
        self.step.update()

    def check_options(self):
        if not os.path.exists(self.option('genus_table')):
            raise OptionError("必须提供Genus Table")
        if not os.path.exists(self.option('group_table')):
            raise OptionError("必须提供分组表格")
        if self.option('method'):
            if self.option('method') not in ['sum', 'average', 'median']:
                raise OptionError("丰度计算方法只能选择sum,average,median之一")
        if self.option('mode') == 2:
            if not os.path.exists(self.option('name_table')):
                raise OptionError("Mode 2 模式下必须提供物种名列表文件")
        os.system(
            'cat %s | awk -F "\t" \'{ print $1 }\' > tmp.txt' % (self.option('genus_table')))
        genus_data = open("tmp.txt", "r").readlines()[1:]
        os.remove('tmp.txt')
        genus_data = map(string.rstrip, genus_data)
        sample_data = open(self.option('genus_table'),
                           "r").readline().strip().split()[1:]

        group_data = open(self.option('group_table'), "r").readlines()[1:]
        group_data = map(string.strip, group_data)
        for s in group_data:
            if s.split()[0] not in sample_data:
                raise OptionError("物种%s不在Genus Table中" % s.split()[0])
            if s.split()[1] not in ['0', '1']:
                raise OptionError("物种分组只能有0和1！")

        if self.option('mode') == 2:
            name_data = open(self.option('name_table'), "r").readlines()[1:]
            name_data = map(string.strip, name_data)
            for s in name_data:
                if s not in genus_data:
                    raise OptionError("物种%s不在Genus Table中" % s)

        if self.option('mode') == 1:
            if self.option('top_n') > len(genus_data):
                raise OptionError("选择丰度前N高物种时，设定的N多于物种总数：%d>%d" %
                                  (self.option('top_n'), len(genus_data)))

        return True

    def set_resource(self):
        """
        """
        self._cpu = 2
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "ROC分析结果目录"],
            ["./roc_curve.pdf", "pdf", "ROC受试者工作特征曲线图"],
            ["./roc_auc.xls", "xls", "ROC受试者工作特征曲线-AUC VALUE"]
        ])
        print self.get_upload_files()
        super(RocAgent, self).end()


class RocTool(Tool):

    def __init__(self, config):
        super(RocTool, self).__init__(config)
        self._version = '1.0.1'

    def run(self):
        """
        运行
        """
        super(RocTool, self).run()
        self.run_roc_perl()

    def run_roc_perl(self):
        """
        运行calc_roc.perl
        """
        os.system(
            'export PATH=/mnt/ilustre/users/sanger-dev/app/gcc/5.1.0/bin:$PATH')
        os.system(
            'export LD_LIBRARY_PATH=/mnt/ilustre/users/sanger-dev/app/gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        cmd = self.config.SOFTWARE_DIR + '/program/perl/perls/perl-5.24.0/bin/perl ' + \
            self.config.SOFTWARE_DIR + '/bioinfo/meta/scripts/plot_roc.pl '
        cmd += '-o %s ' % (self.work_dir + '/ROC/')
        cmd += '-i %s ' % (self.option('genus_table'))
        cmd += '-mode %d ' % (self.option('mode'))
        cmd += '-group %s ' % (self.option('group_table'))
        if self.option('mode') == 2:
            cmd += '-name %s ' % (self.option('name_table'))
        if self.option('method'):
            cmd += '-method %s ' % (self.option('method'))
        if self.option('mode') == 1:
            cmd += '-n %d ' % (self.option('top_n'))
        cmd += '-labels F '
        self.logger.info('开始运行calc_roc.pl计算ROC相关数据')

        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 roc.cmd.r 成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 roc.cmd.r 失败')
            self.set_error('无法生成 roc.cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/roc.cmd.r' % (self.work_dir + '/ROC'), shell=True)
            self.logger.info('ROC计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('ROC计算失败')
            self.set_error('R运行计算ROC失败')
            raise "运行R脚本计算ROC相关数据失败"
        self.logger.info('运行calc_roc.pl程序进行ROC计算完成')
        allfiles = self.get_roc_filesname()
        self.linkfile(self.work_dir + '/ROC/' + allfiles[0], 'roc_curve.pdf')
        self.linkfile(self.work_dir + '/ROC/' + allfiles[1], 'roc_auc.xls')
        self.end()

    def linkfile(self, oldfile, newname):
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def get_roc_filesname(self):
        filelist = os.listdir(self.work_dir + '/ROC')
        roc_curve = None
        roc_auc = None
        for name in filelist:
            if 'roc_curve.pdf' in name:
                roc_curve = name
            elif 'roc_aucvalue.xls' in name:
                roc_auc = name
        if (roc_curve and roc_auc):
            return [roc_curve, roc_auc]
        else:
            self.set_error("未知原因，ROC计算结果丢失")
