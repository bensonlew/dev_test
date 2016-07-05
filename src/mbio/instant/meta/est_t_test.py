# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import os
import shutil
# from biocluster.config import Config
from mbio.packages.statistical.metastat import est_ttest
from mbio.packages.alpha_diversity.group_file_split import group_file_spilt


class EstTTestInstant(object):
    def __init__(self, bindObject):
        self.logger = bindObject.logger
        self.option = bindObject.options
        self.work_dir = bindObject.work_dir
        self._version = 1.0

    def est_ttest(self):
        """
        调用package计算alpha多样性指数T检验
        """
        self.logger.info("开始计算T检验")
        self.logger.info(os.path.join(self.work_dir, "est_t_test.xls"))
        group_files = group_file_spilt(self.option["groupPath"], os.path.join(self.work_dir, "group_files"))
        gfilelist = os.listdir(group_files)
        self.logger.info(gfilelist)
        i = 1
        for group in gfilelist:
            self.logger.info(os.path.join(self.work_dir, "group_files", group))
            self.logger.info("开始运行est_T检验")
            est_ttest(self.option["est_table"], self.work_dir + '/est_result%s.xls' % i,
                      os.path.join(self.work_dir, "group_files", group))
            cmd = "/mnt/ilustre/users/sanger/app/R-3.2.2/bin/Rscript run_est_ttest.r"
            os.system(cmd)
            self.logger.info("运行结束")
            self.logger.info("set_output")
            est_t_path = os.path.join(self.work_dir, self.work_dir + '/est_result%s.xls' % i)
            output_dir = os.path.join(self.work_dir, "output", 'est_result%s.xls' % i)
            if os.path.exists(output_dir):
                os.remove(est_t_path)
            shutil.copy2(est_t_path, output_dir)
        # est_ttest(self.option["est_table"], os.path.join(self.work_dir, "est_t_test.xls"), self.option["groupPath"])
        self.logger.info("完成")

    def run(self):
        self.est_ttest()
    #     self.set_output()
    #
    # def set_output(self):
    #     est_t_path = os.path.join(self.work_dir, "est_t_test.xls")
    #     output_dir = os.path.join(self.work_dir, "output", "est_t_test.xls")
    #     if os.path.exists(output_dir):
    #         os.remove(est_t_path)
    #     shutil.copy2(est_t_path, output_dir)
