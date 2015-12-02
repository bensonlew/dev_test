# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.module import Module
from biocluster.core.exceptions import OptionError
import os


class OtuAnalysis(Module):
    def __init__(self, work_id):
        super(OtuAnalysis,self).__init__(work_id)
        options = [
            {'name': 'fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 输入fasta文件，序列名称格式为'sampleID_seqID'.
            {'name': 'identity', 'type': 'float', 'default': 0.97},  # 相似性值，范围0-1.
            {'name': 'revcomp', 'type': 'bool'},  # 序列是否翻转
            {'name': 'confidence', 'type': 'float', 'default': 0.7},  # 置信度值
            {"name": "customer_mode", "type": "bool", "default": False},  # OTU分类分析的时候自定义数据库
            {'name': 'database', 'type': 'string'},  # 数据库选择
            {'name': 'ref_fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 参考fasta序列
            {'name': 'ref_taxon', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 参考taxon文件
            
            {'name': 'otu_seqids', 'type': 'outfile', 'format': 'meta.otu.otu_seqids'},  # 输出结果otu中包含序列列表
            {'name': 'otu_biom', 'type': 'outfile', 'format': 'meta.otu.biom'}  # 输出结果biom格式otu表
            {'name': 'otu_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出结果otu表
            {'name': 'otu_rep', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出结果otu代表序列
            {'name': 'taxon_file', 'type': 'outfile', 'format': 'taxon.seq_taxon'}  # 输出序列的分类信息文件
            {'name': 'otu_taxon_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 带分类信息的biom文件
            {'name': 'otu_taxon_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 待分类信息的otu表文件
            {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'meta.otu.tax_summary_abs_dir'}]  #  输出的otu_taxon_dir文件夹
        self.add_option(options)
        self.usearch = self.add_tool('meta.otu.usearch')
        self.qiimeassign = self.add_tool('taxon.qiime_assign')
        self.otutaxonstat = self.add_tool('meta.otu.otu_taxon_stat')

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option("fasta").is_set:
            raise OptionError("必须设置输入fasta文件.")
        if self.option("identity") < 0 or self.option("identity") > 1:
            raise OptionError("identity值必须在0-1范围内.")
        if self.option("revcomp") not in [True, False]:
            raise OptionError("必须设置参数revcomp")
        if self.option("customer_mode"):
            if not self.option("ref_fasta").is_set or not self.option("ref_taxon").is_set:
                raise OptionError("数据库自定义模式必须设置ref_fasta和ref_taxon参数")
        else:
            if self.option("database") not in ['silva119/16s_bacteria', 'silva119/16s_archaea', 'silva119/18s_eukaryota', 'unite6.0/its_fungi', 'fgr/amoA', 'fgr/nosZ', 'fgr/nirK', 'fgr/nirS', 'fgr/nifH', 'fgr/pmoA', 'fgr/mmoX']:
                raise OptionError("数据库{}不被支持".fomat(self.option("database")))

    def usearch_run(self):
        """
        运行Usearch，生成OTU表
        """
        myopt = {
                'fasta': self.option('fasta').prop['path'],
                'identity': self.option('confidence')
                }
        self.usearch.set_option(myopt)
        self.on_rely(self.usearch, qiimeassign_run)
        usearch.run()

    def qiimeassign_run(self):
        """
        运行Qiime Assign,获取OTU的分类信息
        """
        qiimeassign = self.add_tool('taxon.qiime_assign')
        myopt = {
                'fasta': relyobj.rely[0].option('otu_rep').prop['path'],
                'revcomp': self.option('revcomp'),
                'confidence': self.option('confidence'),
                'customer_mode': self.option('customer_mode'),
                'database': self.option('database'),
                'ref_fasta': self.option('ref_fasta'),
                'ref_taxon': self.option('ref_taxon')
                }
        qiimeassign.set_option(myopt)
        self.on_rely([self.usearch, self.qiimeassign], otutaxonstat_run)
        qiimeassign.run()

    def otutaxonstat_run(self):
        """
        进行分类学统计，产生不同分类水平上OTU统计表
        """
        myopt = {
                'otu_seqids': relyobj.rely[0].option('otu_seqids'),
                'taxon_file': relyobj.rely[1].option('taxon_file')
                }
        otutaxonstat.set_option(myopt)
        otutaxonstat.run()

    def run(self):
        self.usearch_run()
        super(OtuAnalysis, self).run()
