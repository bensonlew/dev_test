# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os


class KeggRichAgent(Agent):
    """
    Kegg富集分析
    version v1.0.1
    author: qiuping
    last_modify: 2016.11.23
    """
    def __init__(self, parent):
        super(KeggRichAgent, self).__init__(parent)
        options = [
            {"name": "kegg_table", "type": "infile", "format": "annotation.kegg.kegg_table"},  # 只含有基因的kegg table结果文件
            # {"name": "all_list", "type": "infile", "format": "rna.gene_list"},  # gene名字文件
            {"name": "diff_stat", "type": "infile", "format": "rna.diff_stat_table"},  # 改为输入状态表文件
            {"name": "correct", "type": "string", "default": "BH"}  # 多重检验校正方法
        ]
        self.add_option(options)
        self.step.add_steps("kegg_rich")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.kegg_rich.start()
        self.step.update()

    def stepfinish(self):
        self.step.kegg_rich.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('kegg_table').is_set:
            raise OptionError('必须设置kegg的pathway输入文件')
        if self.option('correct') not in ['BY', 'BH', 'None', 'QVALUE']:
            raise OptionError('多重检验校正的方法不在提供的范围内')
        if not self.option("diff_stat").is_set:
            raise OptionError("必须设置输入文件diff_stat")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '4G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"kegg_enrichment.xls$", "xls", "kegg富集分析结果"]
        ])
        super(KeggRichAgent, self).end()


class KeggRichTool(Tool):
    def __init__(self, config):
        super(KeggRichTool, self).__init__(config)
        self._version = "v1.0.1"
        # self.kobas = '/bioinfo/annotation/kobas-2.1.1/src/kobas/scripts/'
        # self.kobas_path = self.config.SOFTWARE_DIR + '/bioinfo/annotation/kobas-2.1.1/src/'
        # self.set_environ(PYTHONPATH=self.kobas_path)
        # self.r_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin:$PATH"
        # self._r_home = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/"
        # self._LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + "/program/R-3.3.1/lib64/R/lib:$LD_LIBRARY_PATH"
        # self.set_environ(PATH=self.r_path, R_HOME=self._r_home, LD_LIBRARY_PATH=self._LD_LIBRARY_PATH)
        self.python = '/program/Python/bin/'
        # self.all_list = self.option('all_list').prop['gene_list']
        # self.diff_list = self.option('diff_list').prop['gene_list']
        self.script_path = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/"
        self.k2e = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/K2enzyme.tab"
        self.brite = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/br08901.txt"

    def run(self):
        """
        运行
        :return:
        """
        super(KeggRichTool, self).run()
        self.run_kegg_rich()

    def run_kegg_rich(self):
        """
        运行kobas软件，进行kegg富集分析
        """
        try:
            # self.option('kegg_table').get_kegg_list(self.work_dir, self.all_list, self.diff_list)
            # self.logger.info("kegg富集第一步运行完成")
            self.logger.info("准备gene path:gene_Knumber G2K文件")
            self.option("kegg_table").get_gene2K(self.work_dir)
            self.logger.info("准备gene path:konumber G2K文件")
            self.option("kegg_table").get_gene2path(self.work_dir)
            self.logger.info("准备差异文件")
            # diff_gene, regulate_dict = self.option("diff_stat").get_table_info()
            self.option("diff_stat").get_stat_file(self.work_dir, self.work_dir + "/gene2K.info")
            self.logger.info("统计背景数量")
            length = os.popen("less {}|wc -l".format(self.work_dir + "/gene2K.info"))
            line_number = int(length.read().strip("\n"))
            self.bgn = line_number - 1  # 去掉头文件
            # self.run_identify()
        except Exception as e:
            self.set_error("kegg富集第一步运行出错:{}".format(e))
            self.logger.info("kegg富集第一步运行出错:{}".format(e))
        self.run_identify()

    def run_identify(self):
        deg_path = self.work_dir + "/" + os.path.basename(self.option('diff_stat').prop["path"]).split("_edgr_stat.xls")[0] + ".DE.list"
        # kofile = os.path.splitext(os.path.basename(self.option('diff_list').prop['path']))[0]
        kofile = os.path.basename(self.option('diff_stat').prop['path']).split("_edgr_stat.xls")[0] + ".DE.list"
        g2p_path = self.work_dir + "/gene2path.info"
        g2k_path = self.work_dir + "/gene2K.info"
        bgn = self.bgn
        k2e = self.k2e
        brite = self.brite
        cmd_2 = self.python + 'python {}kegg_enrichment.py -deg {} -g2p {} -g2k {} -bgn {} -k2e {} -brite {} --FDR -dn 20'.format(self.script_path, deg_path, g2p_path, g2k_path, bgn, k2e, brite)
        self.logger.info('开始运行kegg富集第二步：进行kegg富集分析')
        command_2 = self.add_command("cmd_2", cmd_2).run()
        self.wait(command_2)
        if command_2.return_code == 0:
            self.logger.info("kegg富集分析运行完成")
            self.set_output(kofile + '.kegg_enrichment.xls')
            self.end()
        else:
            self.set_error("kegg富集分析运行出错!")
            raise Exception("kegg富集分析运行出错")

    def set_output(self, linkfile):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        try:
            os.link(linkfile, self.output_dir + '/{}'.format(linkfile))
            self.logger.info("设置kegg富集分析结果目录成功")
        except Exception as e:
            self.logger.info("设置kegg富集分析结果目录失败{}".format(e))
            self.set_error("设置kegg富集分析结果目录失败{}".format(e))
