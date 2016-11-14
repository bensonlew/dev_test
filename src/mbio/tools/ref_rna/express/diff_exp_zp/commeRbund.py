## !/mnt/ilustre/users/sanger/app/program/Anaconda2/bin/python
# -*- coding: utf-8 -*-
# __author__ = "zhangpeng"
#last_modify:20160908

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.tools.ref_rna.express.diff_exp_zp.commeRbund_expr import *
import os
import re


class CommerbundAgent(Agent):
    """
    version v1.0
    author: zhangpeng
    last_modify: 2016.09.08
    """
    def __init__(self, parent):
        super(CommerbundAgent, self).__init__(parent)
        options = [
            {"name": "diff_fpkm", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  #输入文件，差异基因表达量矩阵
            {"name": "feature", "type": "string", "default": "gene"},  # 选择分析对象
            
        ]
        self.add_option(options)
        self.step.add_steps("commeRbound")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.commeRbound.start()
        self.step.update()

    def stepfinish(self):
        self.step.commeRbound.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option("diff_fpkm").is_set:
            raise OptionError("必须设置输入文件:差异基因fpkm表")
        if self.option("feature") not in ("gene", "isofrom"):
            raise OptionError("所选分析类型不在提供的范围内")


    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '4G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        if self.option('feature') in ('gene', 'isofrom'):
            result_dir.add_relpath_rules([
                [".", "", "结果输出目录"],
                ["./expr/", "", "gene分析结果目录"],
                ["expr/gene_fpkm.xls", "xls", "fpkm"],
				["expr/gene_count.xls",'xls',"count"],
				["expr/isoform_fpkm.xls","xls","isoform"],
                ["diff/diff_expr.xls", "xls", "差异分析"]
            ])
        super(CommerbundAgent, self).end()


class CommerbundTool(Tool):
    """
    表达量差异检测tool
    """
    def __init__(self, config):
        super(CommerbundTool, self).__init__(config)
        self._version = '1.0.1'
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.r_path = '/program/R-3.3.1/bin/Rscript'

    def run_cluster(self):
        clust(input_matrix=self.option('diff_fpkm').prop['path'],feature=self.option('feature'))
        clust_cmd = self.r_path + " commeRbund_R"
        self.logger.info("开始运行clust_cmd")
        cmd = self.add_command("clust_cmd", clust_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行clust_cmd成功")
        else:
            self.logger.info("运行clust_cmd出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        #dir_path =os.path.join(self.work_dir,"expr")
        #os.mkdir(dir_path)
        #dir_path =os.path.join(self.work_dir,"diff")
        #os.mkdir(dir_path)
        try:
            if self.option('feature') in ('gene', 'isofrom'):
                os.system('cp -r %s/expr/ %s/' % (self.work_dir, self.output_dir))
                self.logger.info("设置expr结果目录成功")
        except Exception as e:
            self.logger.info("设置结果目录失败{}".format(e))

    def run(self):
        super(CommerbundTool, self).run()
        self.run_cluster()
        self.set_output()
        self.end()
