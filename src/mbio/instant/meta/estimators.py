# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import os
import subprocess
import shutil
from biocluster.config import Config
from mbio.packages.alpha_diversity.estimator_size import est_size
from mbio.packages.alpha_diversity.make_estimators_table import make_estimators_table


class EstimatorsInstant(object):
    def __init__(self, bindObject):
        self.logger = bindObject.logger
        self.options = bindObject.options
        self.work_dir = bindObject.work_dir
        self.shared_path = os.path.join(Config().SOFTWARE_DIR, "Perl/bin/perl")
        self.indices = '-'.join(self.options['indices'].split(','))
        self.special_est = ['boneh', 'efron', 'shen', 'solow']
        self._version = 1.0

    def run(self):
        self.make_shared()
        self.estimators()
        self.set_output()
        # self.make_est_table()

    def make_shared(self):
        """
        用脚本otu2shared,生成mothur计算多样性指数所需要的shared格式OTU表格
        """
        self.logger.info("开始生成shared")
        self.logger.info("转化otu_table({})为shared文件({})".format(self.options["otu_table"], "otu.shared"))
        try:
            subprocess.check_output(self.shared_path+" /mnt/ilustre/users/sanger/app/meta/scripts/otu2shared.pl "+" -i "
                                    + self.options["otu_table"] + " -l 0.97 -o " + os.path.join(self.work_dir, self.options["level"]+".shared"), shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("转化otu_table到shared文件出错")
            return False

    def estimators(self):
        """
        运行mothur计算多样性指数
        """
        self.logger.info("开始计算多样性指数")
        # os.system('/mnt/ilustre/users/sanger/app/meta/mothur.1.30  "#set.dir(output="{}")"'.format(self.work_dir))
        cmd = '/mnt/ilustre/users/sanger/app/meta/mothur.1.30 "#set.dir(output="%s");summary.single(shared=%s.shared,' \
              'groupmode=f,calc=%s)"' % (self.work_dir, self.options['level'], self.indices)
        self.logger.info(cmd)
        for index in self.indices.split('-'):
            if index in self.special_est:
                size = est_size(self.options["otu_table"])
                cmd = '/mnt/ilustre/users/sanger/app/meta/mothur.1.30 "#set.dir(output="%s");summary.single(shared=%s.' \
                      'shared,groupmode=f,calc=%s,size=%s)"' % (self.work_dir, self.options['level'], self.indices, size)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("生成多样性指数表")
            make_estimators_table(self.work_dir)
            self.logger.info("运行结束")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("运行出错")
            return False

    def set_output(self):
        est_path = os.path.join(self.work_dir, 'estimators.xls')
        output_dir = os.path.join(self.work_dir, "output", "estimators.xls")
        if os.path.exists(output_dir):
            os.remove(est_path)
        shutil.copy2(est_path, output_dir)
