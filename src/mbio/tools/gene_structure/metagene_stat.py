# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'

import os
import re
from biocluster.core.exceptions import OptionError
from biocluster.agent import Agent
from biocluster.tool import Tool
from mbio.packages.ref_rna.trans_step import step_count


class MetageneStatAgent(Agent):
    """
    统计基因预测结果
    version: 1
    author: guhaidong
    last_modify: 2017.09.12
    """

    def __init__(self, parent):
        super(MetageneStatAgent, self).__init__(parent)
        options = [
            {"name": "contig_dir", "type": "infile", "format": "sequence.fasta_dir"},
            # 输入文件，预测后的序列路径
            {"name": "sample_stat", "type": "outfile", "format": "sequence.profile_table"},  # 输出文件，对各基因预测结果进行统计
            {"name": "fasta", "type": "outfile", "format": "sequence.fasta"},  # 输出文件，输出序列用于构建非冗余基因集
        ]
        self.add_option(options)
        self.step.add_steps("MetageneStat")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.MetageneStat.start()
        self.step.update()

    def stepfinish(self):
        self.step.MetageneStat.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        :return:
        """
        if not self.option('contig_dir'):
            raise OptionError('必须输入基因预测结果文件路径')
        if not os.path.exists(self.option('contig_dir').prop['path']):
            raise OptionError('基因预测结果文件夹不存在')
        if not os.listdir(self.option('contig_dir').prop['path']):
            raise OptionError('基因预测结果文件夹为空')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 1
        self._memory = "1G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["", "", ""]
        ])
        super(MetageneStatAgent, self).end()


class MetageneStatTool(Tool):
    def __init__(self, config):
        super(MetageneStatTool, self).__init__(config)
        self._version = "1"
        self.python_path = '/program/Python/bin/python '
        self.gene_stat_path = self.config.SOFTWARE_DIR + '/bioinfo/metaGenomic/scripts/gene_stat.py'

    def run(self):
        """
        运行
        :return:
        """
        super(MetageneStatTool, self).run()
        self.run_metagenestat()
        self.set_output()
        self.end()

    def run_metagenestat(self):
        """
        gene_stat -gene_dir  gene.directory -output_stat  stat_file -output_fa  fasta_file
        :return:
        """
        cmd = self.python_path + ' %s -gene_dir %s -output_stat %s -output_fa %s' % (self.gene_stat_path,
                                                                                     self.option('contig_dir').prop[
                                                                                         'path'],
                                                                                     self.work_dir +
                                                                                     '/sample.metagene.stat',
                                                                                     self.work_dir + '/metagene.fa')
        command = self.add_command("metagenestat", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行metagenestat的cmd完成")
        else:
            self.set_error("运行metagenestat的cmd运行出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        if os.path.exists(self.output_dir + '/sample.metagene.stat'):
            os.remove(self.output_dir + '/sample.metagene.stat')
        if os.path.exists(self.output_dir + '/metagene.fa'):
            os.remove(self.output_dir + '/metagene.fa')
        os.link(self.work_dir + '/sample.metagene.stat', self.output_dir + '/sample.metagene.stat')
        os.link(self.work_dir + '/metagene.fa', self.output_dir + '/metagene.fa')
        self.option('sample_stat').set_path(self.output_dir + '/sample.metagene.stat')
        self.option('fasta').set_path(self.output_dir + '/metagene.fa')
        self.logger.info("设置Metagene分析结果目录成功")
