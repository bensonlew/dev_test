# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.gene_structure.pfam_domtblout import pfam_out


class OrfAgent(Agent):
    """
    transdecoder:orf预测软件
    hmmscan：Pfam数据库比对工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.11
    """

    def __init__(self, parent):
        super(OrfAgent, self).__init__(parent)
        options = [
            {"name": "fasta", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "search_pfam", "type": "bool", "default": True},  # 是否比对Pfam数据库
            {"name": "p_length", "type": "int", "default": 100},  # 最小蛋白长度
            {"name": "Markov_length", "type": "int", "default": 3000},  # 马尔科夫训练长度
            {"name": "bed", "type": "outfile", "format": "denovo_rna.gene_structure.bed"},  # 输出结果
            {"name": "cds", "type": "outfile", "format": "sequence.fasta"},  # 输出结果
            {"name": "pep", "type": "outfile", "format": "sequence.fasta"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fasta").is_set:
            raise OptionError("请传入fasta序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 20
        self._memory = ''


class OrfTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(OrfTool, self).__init__(config)
        self.transdecoder_path = "rna/TransDecoder-2.0.1/"
        self.hmmscan_path = "rna/hmmer-3.1b2/src/"
        self.pfam_db = "/mnt/ilustre/users/sanger/app/rna/PfamScan/db/Pfam-A.hmm"
        self.fasta_name = self.option("fasta").prop["path"].split("/")[-1]

    def td_longorfs(self):
        self.logger.info(self.option("p_length"))
        cmd = "{}TransDecoder.LongOrfs -t {} -m {}".format(self.transdecoder_path, self.option("fasta").prop["path"],
                                                           self.option("p_length"))
        print(cmd)
        self.logger.info("开始提取长orf")
        command = self.add_command("transdecoder_longorfs", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("提取结束！")
        else:
            self.set_error("提取orf过程出错")

    def td_predict(self, hmm_out=None):
        if hmm_out is None:
            hmm_out = ""
        else:
            hmm_out = "--retain_pfam_hits {}".format(hmm_out)
        cmd = "{}TransDecoder.Predict -t {} -T {} {}".format(
            self.transdecoder_path, self.option("fasta").prop["path"], self.option("Markov_length"), hmm_out)
        print(cmd)
        self.logger.info("开始预测编码区域")
        command = self.add_command("transdecoder_predict", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("预测编码区域完成！")
        else:
            self.set_error("预测过程过程出错")
        self.set_output()

    def hmmscan(self, pep):
        cmd = "{}hmmscan --cpu 20 --noali --cut_nc --acc --notextw --domtblout {} {} {}".format(
            self.hmmscan_path, "pfam.domtblout", self.pfam_db, pep)
        print(cmd)
        self.logger.info("开始运行hmmscan")
        command = self.add_command("hmmscan", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行hmmscan结束")
        else:
            self.set_error("运行hmmscan出错")
        pfam_out("pfam.domtblout")

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        pep = '{}.transdecoder.pep'.format(self.fasta_name)
        bed = '{}.transdecoder.bed'.format(self.fasta_name)
        cds = '{}.transdecoder.cds'.format(self.fasta_name)
        os.link(self.work_dir+"/"+pep, self.output_dir+"/"+pep)
        self.option('pep').set_path(self.output_dir+"/"+pep)
        os.link(self.work_dir+"/"+bed, self.output_dir+"/"+bed)
        self.option('bed').set_path(self.output_dir+"/"+bed)
        os.link(self.work_dir+"/"+cds, self.output_dir+"/"+cds)
        self.option('cds').set_path(self.output_dir+"/"+cds)
        os.link(self.work_dir+"/"+"pfam_domain", self.output_dir+"/"+"pfam_domain")
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(OrfTool, self).run()
        if self.option("search_pfam") is True:
            self.td_longorfs()
            self.hmmscan("{}.transdecoder_dir/longest_orfs.pep".format(self.fasta_name))
            self.td_predict("pfam.domtblout")
        else:
            self.td_longorfs()
            # pfam_out("pfam.domtblout")
            # self.set_output()
            self.td_predict()
        self.end()
