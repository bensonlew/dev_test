# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
# import os
import types
from biocluster.core.exceptions import OptionError
from mbio.packages.beta_diversity.plsda_r import *


class PlsdaAgent(Agent):
    """
    plsda_r.py
    version v1.0
    author: shenghe
    last_modified:2016.3.24
    """
    def __init__(self, parent):
        super(PlsdaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "grouplabs", "type": "string", "default": ""}
        ]
        self.add_option(options)
        self.step.add_steps('plsda')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.plsda.start()
        self.step.update()

    def step_end(self):
        self.step.plsda.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        """
        if self.option('group').is_set:
            self.option('group').get_info()
            if self.option('group').prop['sample_number'] < 2:
                raise OptionError('分组文件的样本数目少于2，不可进行beta多元分析')
            if self.option('grouplabs'):
                if self.option('grouplabs') not in self.option('group').prop['group_scheme']:
                    raise OptionError('提供的分组方案名:%s不在分组文件:%s中'
                                      % (self.option('grouplabs'), self.option('group').prop['group_scheme']))
            else:
                pass
                # self.option('grouplabs', self.option('group').prop['group_scheme'][0])
            if not self.option('otutable').is_set:
                raise OptionError('没有提供otu表')
            self.option('otutable').get_info()
            if self.option('otutable').prop['sample_num'] < 2:
                raise OptionError('otu表的样本数目少于2，不可进行beta多元分析')
            otu_samplelist = open(self.option('otutable').path).readline().strip().split('\t')[1:]
            group_collection = set(self.option('group').prop['sample'])
            collection = group_collection & set(otu_samplelist)
            if group_collection == collection:
                pass
            else:
                raise OptionError('group文件中存在otu表中不存在的样本')
            table = open(self.option('otutable').path)
            if len(table.readlines()) < 4:
                raise OptionError('提供的otu表数据表信息少于3行')
            table.close()
        else:
            raise OptionError('没有提供分组信息表')
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
            [".", "", "plsda分析结果目录"],
            ["./plsda_sites.xls", "xls", "样本坐标表"],
            ["./plsda_rotation.xls", "xls", "物种主成分贡献度表"],
            ["./plsda_importance.xls", "xls", "主成分解释度表"],
        ])
        print self.get_upload_files()
        super(PlsdaAgent, self).end()


class PlsdaTool(Tool):
    def __init__(self, config):
        super(PlsdaTool, self).__init__(config)
        self._version = '1.0'
        self.cmd_path = 'mbio/packages/beta_diversity/plsda_r.py'
        self.grouplab = self.option('grouplabs') if self.option('grouplabs') else self.option('group').prop['group_scheme'][0]
        self.otu_table = self.get_otu_table()

    def get_otu_table(self):
        """
        根据level返回进行计算的otu表路径
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        # otu表对象没有样本列表属性
        return self.filter_otu_sample(otu_path, self.option('group').prop['sample'],
                                      os.path.join(self.work_dir + 'temp_filter.otutable'))

    def filter_otu_sample(self, otu_path, filter_samples, newfile):
        if not isinstance(filter_samples, types.ListType):
            raise Exception('过滤otu表样本的样本名称应为列表')
        try:
            with open(otu_path, 'rb') as f, open(newfile, 'wb') as w:
                one_line = f.readline()
                all_samples = one_line.rstrip().split('\t')[1:]
                if not ((set(all_samples) & set(filter_samples)) == set(filter_samples)):
                    raise Exception('提供的过滤样本存在otu表中不存在的样本all:%s,filter_samples:%s' % (all_samples, filter_samples))
                if len(all_samples) == len(filter_samples):
                    return otu_path
                samples_index = [all_samples.index(i) + 1 for i in filter_samples]
                w.write('#OTU\t' + '\t'.join(filter_samples) + '\n')
                for line in f:
                    all_values = line.rstrip().split('\t')
                    new_values = [all_values[0]] + [all_values[i] for i in samples_index]
                    w.write('\t'.join(new_values) + '\n')
                return newfile
        except IOError:
            raise Exception('无法打开OTU相关文件或者文件不存在')


    def run(self):
        """
        运行
        """
        super(PlsdaTool, self).run()
        self.run_plsda()

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

    def run_plsda(self):
        """
        运行plsda_r.py
        """
        self.logger.info('运行plsda_r.py程序计算PLSDA')
        return_mess = plsda(self.otu_table, self.option('group').path,
                            self.work_dir, self.grouplab)
        self.logger.info('运行plsda_r.py程序计算PLSDA成功')
        if return_mess == 0:
            self.linkfile(self.work_dir + '/plsda_sites.xls', 'plsda_sites.xls')
            self.linkfile(self.work_dir + '/plsda_rotation.xls', 'plsda_rotation.xls')
            self.linkfile(self.work_dir + '/plsda_importance.xls', 'plsda_importance.xls')
            self.logger.info('运行plsda_r.py程序计算PLSDA完成')
            self.end()
        else:
            self.set_error('运行plsda_r.py程序计算PLSDA出错')
