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
    last_modify: 2016.08.09
    """
    def __init__(self, parent):
        super(KeggRichAgent, self).__init__(parent)
        options = [
            {"name": "kegg_path", "type": "infile", "format": "denovo_rna.annotation.kegg.kegg_list"},  # KEGG的pathway文件
            {"name": "diff_list", "type": "infile", "format": "denovo_rna.express.gene_list"},  # 两两样本/分组的差异基因文件
            {"name": "correct", "type": "string", "default": "BH"}  # 多重检验校正方法
        ]
        self.add_option(options)
        self.step.add_steps("kegg_path")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.kegg_path.start()
        self.step.update()

    def stepfinish(self):
        self.step.kegg_path.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('kegg_path').is_set:
            raise OptionError('必须设置kegg的pathway输入文件')
        if self.option('correct') not in ['BY', 'BH', 'None', 'QVALUE']:
            raise OptionError('多重检验校正的方法不在提供的范围内')
        if not self.option("diff_list").is_set:
            raise OptionError("必须设置输入文件diff_list")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''

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
        self.kobas = '/bioinfo/annotation/kobas-2.1.1/src/kobas/scripts/'
        self.kobas_path = self.config.SOFTWARE_DIR + '/bioinfo/annotation/kobas-2.1.1/src/'
        self.set_environ(PYTHONPATH=self.kobas_path)
        self.python = '/program/Python/bin/'

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
        cmd_1 = self.kobas + 'diff_ko_select.pl -g {} -k {} -o {}'.format(self.option('diff_list').prop['path'], self.option('kegg_path').prop['path'], self.work_dir + '/kofile')
        self.logger.info('开始运行kegg富集第一步：合成差异基因kegg文件')
        command_1 = self.add_command("cmd_1", cmd_1).run()
        self.wait(command_1)
        if command_1.return_code == 0:
            self.logger.info("kegg富集第一步运行完成")
            self.run_identify()
        else:
            self.set_error("kegg富集第一步运行出错!")

    def run_identify(self):
        kofile = os.path.basename(self.option('diff_list').prop['path'])
        cmd_2 = self.python + 'python {}identify.py -f {} -n {} -b {} -o {}.kegg_enrichment.xls'.format(self.config.SOFTWARE_DIR + self.kobas, self.work_dir + '/kofile', self.option('correct'), self.option('kegg_path').prop['path'], kofile)
        self.logger.info('开始运行kegg富集第二步：进行kegg富集分析')
        command_2 = self.add_command("cmd_2", cmd_2).run()
        self.wait(command_2)
        if command_2.return_code == 0:
            self.logger.info("kegg富集分析运行完成")
            self.set_output(kofile+'.kegg_enrichment.xls')
        else:
            self.set_error("kegg富集分析运行出错!")

    def set_output(self,linkfile):
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
            self.end()
        except Exception as e:
            self.logger.info("设置kegg富集分析结果目录失败{}".format(e))
            self.set_error("设置kegg富集分析结果目录失败{}".format(e))
