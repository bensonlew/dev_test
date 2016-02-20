# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
# import os
from biocluster.core.exceptions import OptionError
from mbio.packages.beta_diversity.dbrda_r import *


class DbrdaAgent(Agent):
    """
    dbrda_r.py
    version v1.0
    author: shenghe
    last_modified:2015.11.17
    """
    # METHOD = ["manhattan", "euclidean", "canberra", "bray", "kulczynski", "jaccard", "gower", "morisita", "horn",
    #           "mountford", "raup", "binomial", "chao"]
    METHOD = ["manhattan", "euclidean", "canberra", "bray", "kulczynski", "gower"]
    METHOD_DICT = {"manhattan": "manhattan", "euclidean": "euclidean", "canberra": "canberra",
                   "bray_curtis": "bray", "kulczynski": "kulczynski", "gower": "gower"}

    def __init__(self, parent):
        super(DbrdaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}
        ]
        self.add_option(options)
        self.step.add_steps('dbRDA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.dbRDA.start()
        self.step.update()

    def step_end(self):
        self.step.dbRDA.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + '/temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            self.option('envtable').set_path(new_path)
        else:
            pass

    def check_options(self):
        """
        重写参数检查
        """
        if self.option('method') not in DbrdaAgent.METHOD_DICT:
            raise OptionError('错误或者不支持的距离计算方法')
        self.option('method', DbrdaAgent.METHOD_DICT[self.option('method')])
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        # if self.option('method') not in DbrdaAgent.METHOD:
        #     raise OptionError('错误或者不支持的距离计算方法')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('envtable').is_set:
            self.get_new_env()
            self.option('envtable').get_info()
            if len(self.option('envtable').prop['sample']) != len(samplelist):
                raise OptionError('OTU表中的样本数量:%s与环境因子表中的样本数量:%s不一致' % (len(samplelist),
                                  len(self.option('envtable').prop['sample'])))
            for sample in self.option('envtable').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('环境因子中存在，OTU表中的未知样本%s' % sample)
        table = open(self.gettable())
        if len(table.readlines()) < 4:
            raise OptionError('提供的数据表信息少于3行')
        table.close()
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class DbrdaTool(Tool):
    def __init__(self, config):
        super(DbrdaTool, self).__init__(config)
        self._version = '1.0'
        self.cmd_path = 'mbio/packages/beta_diversity/dbrda_r.py'
        # 模块脚本路径，并不使用

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def run(self):
        """
        运行
        """
        super(DbrdaTool, self).run()
        self.run_dbrda()

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

    def run_dbrda(self):
        """
        运行dbrda.py
        """
        self.logger.info('运行dbrda_r.py程序计算Dbrda')
        return_mess = db_rda_new(self.gettable(), self.option('envtable').path, self.work_dir, self.option('method'))
        self.logger.info('运行dbrda_r.py程序计算Dbrda成功')
        if return_mess == 0:
            self.linkfile(self.work_dir + '/db_rda_sites.xls', 'db_rda_sites.xls')
            self.linkfile(self.work_dir + '/db_rda_species.xls', 'db_rda_species.xls')
            lines = open(self.work_dir + '/env_data.temp').readlines()
            if 'centroids:TRUE' in lines[0]:
                self.linkfile(self.work_dir + '/db_rda_centroids.xls', 'db_rda_centroids.xls')
            if 'biplot:TRUE' in lines[1]:
                self.linkfile(self.work_dir + '/db_rda_biplot.xls', 'db_rda_biplot.xls')
            self.logger.info('运行dbrda_r.py程序计算Dbrda完成')
            self.end()
        else:
            self.set_error('运行dbrda_r.py程序计算Dbrda出错')
