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
            {"name": "tmap", "type": "infile","format":"ref_rna.assembly.tmap"},#compare后的tmap文件
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考基因文件
            {"name": "merged.gtf", "type": "infile", "format": "ref_rna.assembly.gtf"},  # 拼接后的注释文件
            {"name": "new_gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"}, #新转录本注释文件
            {"name": "new_genes_gtf", "type": "outfile", "format": "ref_rna.assembly.gtf"},  # 新基因gtf文件
            {"name": "new_fa", "type": "outfile", "format": "sequence.fasta"}  # 新转录本注释文件
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
        if not self.option('ref_fa') :
            raise OptionError('必须输入参考序列ref.fa')
        if not self.option('merged.gtf'):
            raise OptionError('必须输入参考序列merged.gtf')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = "100G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["new_gtf", "gtf", "新转录本注释文件"],
            ["new_genes_gtf", "gtf", "新转录本注释文件"],
            ["new_fa", "fa", "新转录本序列文件"],
        ])
        super(NewTranscriptsAgent, self).end()


class NewTranscriptsTool(Tool):
    def __init__(self, config):
        super(NewTranscriptsTool, self).__init__(config)
        self._version = "v1.0.1"
        self.Python_path ='program/Python/bin/python '
        self.newtranscripts_gtf_path =self.config.SOFTWARE_DIR+ '/bioinfo/rna/scripts/assembly_stat.py'
        self.newtranscripts_fa_path =  'bioinfo/rna/scripts/gtf_to_fasta'


    def run(self):
        """
        运行
        :return:
        """
        super(NewTranscriptsTool, self).run()
        self.run_newtranscripts_gtf()
        self.run_newtranscripts_fa()
        self.set_output()
        self.end()

    def run_newtranscripts_gtf(self):
        """
        运行python，挑出新转录本gtf文件
        """
        cmd = self.Python_path +self.newtranscripts_gtf_path +  " -tmapfile %s -transcript_file %s -o1 %snew_transcripts.gtf -o2 %snew_genes.gtf" %(self.option('tmap').prop['path'],self.option('merged.gtf').prop['path'],self.work_dir+"/",self.work_dir+"/")
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
        gtf_path = os.path.join(self.work_dir, "new_transcripts.gtf")
        cmd = self.newtranscripts_fa_path + " %s %s %snew_transcripts.fa" %(gtf_path,self.option('ref_fa').prop['path'],self.work_dir+"/")
        self.logger.info('运行gtf_to_fasta，形成新转录本fasta文件')
        command = self.add_command("newtranscripts_fa_cmd", cmd).run()
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
            shutil.copy2(self.work_dir + "/new_transcripts.gtf",self.output_dir + "/new_transcripts.gtf")
            shutil.copy2(self.work_dir + "/new_genes.gtf", self.output_dir + "/new_genes.gtf")
            shutil.copy2(self.work_dir + "/new_transcripts.fa", self.output_dir + "/new_transcripts.fa")
            self.option('new_gtf').set_path(self.work_dir + "/new_transcripts.gtf")
            self.option('new_genes_gtf').set_path(self.work_dir + "/new_transcripts.gtf")
            self.option('new_fa').set_path(self.work_dir + "/new_transcripts.fa")
            self.logger.info("设置拼接比较结果目录成功")

        except Exception as e:
            self.logger.info("设置拼接比较分析结果目录失败{}".format(e))
            self.set_error("设置拼接比较分析结果目录失败{}".format(e))
