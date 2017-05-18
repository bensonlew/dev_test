# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import shutil
import re


class NewTranscriptsAgent(Agent):
    """
    新转录本预测
    version v1.0.1
    author: wangzhaoyue
    last_modify: 2016.09.14
    """
    def __init__(self, parent):
        super(NewTranscriptsAgent, self).__init__(parent)
        options = [
            {"name": "tmap", "type": "infile", "format": "assembly.tmap"},  # compare后的tmap文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "merged_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 拼接后的注释文件
            {"name": "new_trans_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新转录本注释文件
            {"name": "new_genes_gtf", "type": "outfile", "format": "gene_structure.gtf"},  # 新基因gtf文件
            {"name": "new_trans_fa", "type": "outfile", "format": "sequence.fasta"},  # 新转录本注释文件
            {"name": "new_genes_fa", "type": "outfile", "format": "sequence.fasta"}  # 新基因注释文件
        ]
        self.add_option(options)
        self.step.add_steps("newtranscripts")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.newtranscripts.start()
        self.step.update()

    def stepfinish(self):
        self.step.newtranscripts.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        if not self.option('tmap'):
            raise OptionError('必须输入compare后tmap文件')
        if not self.option('ref_fa'):
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('merged_gtf'):
            raise OptionError('必须输入参考序列merged_gtf')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "3G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["new_transcripts.gtf", "gtf", "新转录本注释文件"],
            ["new_genes.gtf", "gtf", "新基因注释文件"],
            ["new_transcripts.fa", "fa", "新转录本序列文件"],
            ["new_genes.fa", "fa", "新基因序列文件"],
        ])
        super(NewTranscriptsAgent, self).end()


class NewTranscriptsTool(Tool):
    def __init__(self, config):
        super(NewTranscriptsTool, self).__init__(config)
        self._version = "v1.0.1"
        self.Python_path = 'program/Python/bin/python '
        self.newtranscripts_gtf_path = self.config.SOFTWARE_DIR + '/bioinfo/rna/scripts/assembly_stat.py'
        self.gffread_path = "bioinfo/rna/cufflinks-2.2.1/"


    def run(self):
        """
        运行
        :return:
        """
        super(NewTranscriptsTool, self).run()
        self.run_newtranscripts_gtf()
        self.run_newtranscripts_fa()
        self.run_gene_fa()
        self.set_output()
        self.end()

    def run_newtranscripts_gtf(self):
        """
        运行python，挑出新转录本gtf文件
        """
        cmd = self.Python_path + self.newtranscripts_gtf_path \
            + " -tmapfile %s -transcript_file %s -out_new_trans %snew_transcripts.gtf -out_new_genes %snew_genes.gtf -out_old_trans %sold_trans.gtf -out_old_genes %sold_genes.gtf" % (
                self.option('tmap').prop['path'], self.option('merged_gtf').prop['path'],
                self.work_dir+"/", self.work_dir+"/", self.work_dir+"/", self.work_dir+"/")
        self.logger.info('运行python，挑出新转录本gtf文件')
        command = self.add_command("newtranscripts_gtf_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("python运行完成")
        else:
            self.set_error("python运行出错!")

    def run_newtranscripts_fa(self):
        """
        运行python，挑出新转录本gtf文件
        """
        cmd = self.gffread_path + "gffread %s -g %s -w new_transcripts.fa" % (
            self.work_dir + "/" + "new_transcripts.gtf", self.option('ref_fa').prop['path'])
        self.logger.info('运行gtf_to_fasta，形成新转录本fasta文件')
        command = self.add_command("newtranscripts_fa_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("gtf_to_fasta运行完成")
        else:
            self.set_error("gtf_to_fasta运行出错!")

    def run_gene_fa(self):
        """
        运行python，挑出新转录本gtf文件
        """
        cmd = self.gffread_path + "gffread %s -g %s -w new_genes.fa" % (
            self.work_dir + "/" + "new_genes.gtf", self.option('ref_fa').prop['path'])
        self.logger.info('运行gtf_to_fasta，形成新基因fasta文件')
        command = self.add_command("genes_fa_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("gtf_to_fasta运行完成")
        else:
            self.set_error("gtf_to_fasta运行出错!")

    def set_output(self):
        """
        将结果文件复制到output文件夹下面
        :return:
        """
        self.logger.info("设置结果目录")
        try:
            shutil.copy2(self.work_dir + "/new_transcripts.gtf", self.output_dir + "/new_transcripts.gtf")
            shutil.copy2(self.work_dir + "/new_genes.gtf", self.output_dir + "/new_genes.gtf")
            shutil.copy2(self.work_dir + "/old_trans.gtf", self.output_dir + "/old_trans.gtf")
            shutil.copy2(self.work_dir + "/old_genes.gtf", self.output_dir + "/old_genes.gtf")
            shutil.copy2(self.work_dir + "/new_transcripts.fa", self.output_dir + "/new_transcripts.fa")
            shutil.copy2(self.work_dir + "/new_genes.fa", self.output_dir + "/new_genes.fa")
            self.option('new_trans_gtf').set_path(self.output_dir + "/new_transcripts.gtf")
            self.option('new_genes_gtf').set_path(self.output_dir + "/new_genes.gtf")
            self.option('new_trans_fa').set_path(self.output_dir + "/new_transcripts.fa")
            self.option('new_genes_fa').set_path(self.output_dir + "/new_genes.fa")
            self.logger.info("设置拼接比较结果目录成功")

        except Exception as e:
            self.logger.info("设置拼接比较分析结果目录失败{}".format(e))
            self.set_error("设置拼接比较分析结果目录失败{}".format(e))
