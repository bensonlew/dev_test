# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""RDP taxon 物种分类工具"""

from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError
import os


class QiimeAssignAgent(Agent):
    """
    Qiime taxon_assign.py
    version v1.7
    """
    def __init__(self, parent=None):
        """
        """
        super(QiimeAssignAgent, self).__init__(parent)
        options = [
            {'name': 'fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 输入fasta文件
            {'name': 'revcomp', 'type': 'bool'},  # 序列是否翻转
            {'name': 'confidence', 'type': 'float', 'default': 0.7},  # 置信度值
            {"name": "customer_mode", "type": "bool", "default": False},  # customer 自定义数据库
            {'name': 'database', 'type': 'str'},  # 数据库选择
            {'name': 'ref_fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 参考fasta序列
            {'name': 'ref_taxon', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 参考taxon文件
            {'name': 'taxon_file', 'type': 'outfile', 'format': 'taxon.seq_taxon'}  # 输出序列的分类信息文件
        ]
        self.add_options(options)

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option("fasta").is_set:
            raise OptionError("必须设置参数fasta")
        if not self.option("revcomp").is_set:
            raise OptionError("必须设置参数revcomp")
        if self.option("database") not in ['silva119/16s_bacteria', 'silva119/16s_archaea', 'silva119/18s_eukaryota', 'unite6.0/its_fungi', 'fgr/amoA', 'fgr/nosZ', 'fgr/nirK', 'fgr/nirS', 'fgr/nifH', 'fgr/pmoA', 'fgr/mmoX']:
            raise OptionError("数据库{}不被支持".fomat(self.option("database")))
        if self.option("customer_mode"):
            if not self.option("ref_fasta").is_set or not self.option("ref_taxon").is_set:
                raise OptionError("数据库自定义模式必须设置ref_fasta和ref_taxon参数")

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = ''


class QiimeAssignTool(Tool):
    """
    Qiime Taxon Classify tool
    """
    def __init__(self, config):
        super(QiimeAssignTool, self).__init__(config)
        self.script_path = "meta/scripts/"

    def run_prepare(self):
        cmd = ''
        if self.option('revcomp'):
            cmd = "revcomp "+self.option('fasta').prop['path']+" > seqs.fasta"
        else:
            cmd = "ln -s "+self.option('fasta').prop['path']+" seqs.fasta"
        prepare = self.add_command("prepare", cmd)
        self.logger.info("开始运行prepare")
        prepare.run()
        self.wait(prepare)
        if prepare.return_code == 0:
            self.logger.info("prepare运行完成")
        else:
            self.set_error("prepare运行出错!")
        return prepare.return_code

    def run_assign(self):
        ref_fas = self.config.SOFTWARE_DIR+"/taxon_db/"+self.option('database')+'.fas'
        ref_tax = self.config.SOFTWARE_DIR+"/taxon_db/"+self.option('database')+'.tax'
        if self.option("customer_mode"):
            ref_fas = self.option('ref_fasta').prop['path']
            ref_tax = self.option('ref_taxon').prop['path']
        cmd = self.script_path+"assign_taxonomy.py  -m rdp -i seqs.fasta -c "+self.option('confidence')+"  -r "+ref_fas+" -t "+ref_tax+" -o .  --rdp_max_memory 50000"
        assign = self.add_command("assign", cmd)
        self.logger.info("开始运行assign")
        assign.run()
        self.wait(assign)
        if assign.return_code == 0:
            self.logger.info(u"assign运行完成")
            os.link(self.work_dir+'seqs_tax_assignments.txt', self.output_dir+'seqs_tax_assignments.txt')
            self.option('taxon_file', value=self.output_dir+'seqs_tax_assignments.txt')
            self.set_end()
        else:
            self.set_error("assign运行出错!")

    def run(self):
        super(QiimeAssignTool, self).run()
        if self.run_prepare() == 0:
            self.run_assign()
