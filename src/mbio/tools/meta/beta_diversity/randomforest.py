# -*- coding: utf-8 -*-
# __author__ = 'JieYao'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
import subprocess
from biocluster.core.exceptions import OptionError


class RandomforestAgent(Agent):
    """
    需要RandomForest.pl
    version v1.0
    author: JieYao
    last_modified:2016.07.18
    """

    def __init__(self, parent):
        super(RandomforestAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "grouptable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "ntree", "type": "int", "default": 500 },
            {"name": "problem_type", "type": "int", "default": 2 },
            {"name": "top_number", "type": "int", "default": 50}
            ]
        self.add_option(options)
        self.step.add_steps('RandomforestAnalysis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.RandomforestAnalysis.start()
        self.step.update()

    def step_end(self):
        self.step.RandomforestAnalysis.finish()
        self.step.update()

    def gettable(self):
        """
        根据输入的otu表和分类水平计算新的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']
        
    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        self.option('otutable').get_info()
        if self.option('otutable').prop['sample_num'] < 2:
            raise OptionError('otu表的样本数目少于2，不可进行随机森林特征分析')
        if self.option('grouptable').is_set:
            self.option('grouptable').get_info()
            if len(self.option('grouptable').prop['sample']) < 2:
                raise OptionError('分组表的样本数目少于2，不可进行随机森林特征分析')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('grouptable').is_set:
            self.option('grouptable').get_info()
            if len(self.option('grouptable').prop['sample']) > len(samplelist):
                raise OptionError('OTU表中的样本数量:%s少于分组表中的样本数量:%s' % (len(samplelist),
                                  len(self.option('grouptable').prop['sample'])))
            for sample in self.option('grouptable').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('分组表的样本中存在OTU表中未知的样本%s' % sample)
        table = open(self.gettable())
        if len(table.readlines()) < 4 :
            raise OptionError('数据表信息少于3行')
        table.close()
        if self.option('top_number') > self.option('otutable').prop['otu_num']:
            self.option('top_number', self.option('otutable').prop['otu_num'])
        return True
    
    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''
    
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "RandomForest分析结果输出目录"],
            ["./randomforest_confusion_table.xls", "xls", "RandomForest样本分组模拟结果"],
            ["./randomforest_mds_sites.xls", "xls", "样本点坐标表"],
            ["./randomforest_proximity_table.xls", "xls", "样本相似度临近矩阵"],
            ["./randomforest_topx_vimp.xls", "xls", "Top-X物种(环境因子)丰度表"],
            ["./randomforest_vimp_table.xls", "xls", "所有物种(环境因子)重要度表"],
            ["./randomforest_predicted_answer.xls", "xls", "随机森林预测分组结果表"],
            ["./randomforest_votes_probably.xls","xls", "随机森林各样本分组投票预测概率表"]
        ])
        print self.get_upload_files()
        super(RandomforestAgent, self).end()

        
class RandomforestTool(Tool):
    def __init__(self, config):
        super(RandomforestTool, self).__init__(config)
        self._version = '1.0.1'
        self.cmd_path = self.config.SOFTWARE_DIR + '/bioinfo/meta/scripts/RandomForest_perl.pl'
        if self.option('grouptable').is_set:
            self.group_table = self.option('grouptable').prop['path']
        self.otu_table = self.get_otu_table()
    
    def get_otu_table(self):
        """
        根据调用的level参数重构otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        return otu_path
   
    def run(self):
        """
        运行
        """
        super(RandomforestTool, self).run()
        self.run_RandomForest_perl()
    
    def formattable(self, tablepath):
        with open(tablepath) as table:
            if table.read(1) == '#':
                newtable = os.path.join(self.work_dir, 'temp_format.table')
                with open(newtable, 'w') as w:
                    w.write(table.read())
                return newtable
        return tablepath
    
    def run_RandomForest_perl(self):
        """
        运行RandomForest.pl
        """
        real_otu_path = self.formattable(self.otu_table)
        cmd = self.config.SOFTWARE_DIR + '/program/perl/perls/perl-5.24.0/bin/perl ' + self.cmd_path
        cmd += ' -i %s -o %s' % (real_otu_path, self.work_dir + '/RandomForest')
        if self.option('grouptable').is_set:
            cmd += ' -g %s -m %s' % (self.group_table, self.group_table)
        cmd += ' -ntree %s' % (str(self.option('ntree')))
        cmd += ' -type %s' % (str(self.option('problem_type')))
        cmd += ' -top %s' % (str(self.option('top_number')))
        self.logger.info('运行RandomForest_perl.pl程序进行RandomForest计算')
        
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR + 
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/cmd.r' % (self.work_dir + '/RandomForest'), shell=True)
            self.logger.info('RandomForest计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('RandomForest计算失败')
            self.set_error('R运行计算RandomForest失败')
        self.logger.info('运行RandomForest_perl.pl程序进行RandomForest计算完成')
        allfiles = self.get_filesname()        
        self.linkfile(self.work_dir + '/RandomForest/' + allfiles[1], 'randomforest_mds_sites.xls')
        self.linkfile(self.work_dir + '/RandomForest/' + allfiles[2], 'randomforest_proximity_table.xls')
        self.linkfile(self.work_dir + '/RandomForest/' + allfiles[3], 'randomforest_topx_vimp.xls')
        self.linkfile(self.work_dir + '/RandomForest/' + allfiles[4], 'randomforest_vimp_table.xls')
        if self.option('grouptable').is_set:
            if allfiles[0] and allfiles[5] and allfiles[6]:
                self.linkfile(self.work_dir + '/RandomForest/' + allfiles[0], 'randomforest_confusion_table.xls')
                self.linkfile(self.work_dir + '/RandomForest/' + allfiles[5], 'randomforest_predicted_answer.xls')
                self.linkfile(self.work_dir + '/RandomForest/' + allfiles[6], 'randomforest_votes_probably.xls')
            else:
                self.set_error('按分组计算的文件生成出错')
        self.end()
        
    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def get_filesname(self):
        filelist = os.listdir(self.work_dir + '/RandomForest')
        randomforest_confusion_table_file = None
        randomforest_mds_sites_file = None
        randomforest_proximity_table_file = None
        randomforest_topx_vimp_file = None
        randomforest_vimp_table_file = None
        randomforest_predicted_answer_file = None
        randomforest_votes_probably_file = None
        for name in filelist:
            if 'randomforest_confusion_table.xls' in name:
                randomforest_confusion_table_file = name
            elif 'randomforest_mds_sites.xls' in name:
                randomforest_mds_sites_file = name
            elif 'randomforest_proximity_table.xls' in name:
                randomforest_proximity_table_file = name
            elif 'randomforest_topx_vimp.xls' in name:
                randomforest_topx_vimp_file = name
            elif 'randomforest_vimp_table.xls' in name:
                randomforest_vimp_table_file = name
            elif 'randomforest_predicted_answer.xls' in name:
                randomforest_predicted_answer_file = name
            elif 'randomforest_votes_probably.xls' in name:
                randomforest_votes_probably_file = name
        if (randomforest_mds_sites_file and randomforest_proximity_table_file and 
            randomforest_topx_vimp_file and randomforest_vimp_table_file):
            if self.option('grouptable').is_set:
                if not randomforest_confusion_table_file:
                    self.set_error('未知原因，样本分组模拟结果丢失或未生成')
                if not randomforest_predicted_answer_file:
                    self.set_error('未知原因，样本分组预测结果文件丢失或未生成')
                if not randomforest_votes_probably_file:
                    self.set_error('未知原因，样本分组预测概率表丢失或未生成')
            return [randomforest_confusion_table_file, randomforest_mds_sites_file,
                    randomforest_proximity_table_file, randomforest_topx_vimp_file,
                    randomforest_vimp_table_file, randomforest_predicted_answer_file,
                    randomforest_votes_probably_file]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')

