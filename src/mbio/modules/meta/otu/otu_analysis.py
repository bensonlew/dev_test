# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.module import Module
from biocluster.core.exceptions import OptionError
import os
import shutil
import re


class OtuAnalysisModule(Module):
    def __init__(self, work_id):
        super(OtuAnalysisModule, self).__init__(work_id)
        options = [
            {'name': 'fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 输入fasta文件，序列名称格式为'sampleID_seqID'.
            {'name': 'identity', 'type': 'float', 'default': 0.97},  # 相似性值，范围0-1.
            {'name': 'revcomp', 'type': 'bool'},  # 序列是否翻转
            {'name': 'confidence', 'type': 'float', 'default': 0.7},  # 置信度值
            {'name': 'database', 'type': 'string'},  # 数据库选择
            {'name': 'ref_fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 参考fasta序列
            {'name': 'ref_taxon', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 参考taxon文件
            {'name': 'subsample', 'type': 'bool', 'default': False},  # 是否进行抽平

            {'name': 'otu_seqids', 'type': 'outfile', 'format': 'meta.otu.otu_seqids'},  # 输出结果otu中包含序列列表
            {'name': 'otu_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 输出结果biom格式otu表
            {'name': 'otu_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出结果otu表
            {'name': 'otu_rep', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出结果otu代表序列
            {'name': 'taxon_file', 'type': 'outfile', 'format': 'taxon.seq_taxon'},  # 输出序列的分类信息文件
            {'name': 'otu_taxon_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 带分类信息的biom文件
            {'name': 'otu_taxon_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 待分类信息的otu表文件
            {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'meta.otu.tax_summary_dir'}]  # 输出的otu_taxon_dir文件夹
        self.add_option(options)
        self.usearch = self.add_tool('meta.otu.usearch_otu')
        self.qiimeassign = self.add_tool('taxon.qiime_assign')
        self.otutaxonstat = self.add_tool('meta.otu.otu_taxon_stat')
        self.subsample = self.add_tool('meta.otu.sub_sample')

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
        if self.option('database') == "custom_mode":
            if not self.option("ref_fasta").is_set or not self.option("ref_taxon").is_set:
                raise OptionError("数据库自定义模式必须设置ref_fasta和ref_taxon参数")
        else:
            if self.option("database") not in ['silva119/16s_bacteria', 'silva119/16s_archaea',
                                               'silva119/18s_eukaryota', 'unite6.0/its_fungi', 'fgr/amoA', 'fgr/nosZ',
                                               'fgr/nirK', 'fgr/nirS', 'fgr/nifH', 'fgr/pmoA', 'fgr/mmoX']:
                raise OptionError("数据库{}不被支持".format(self.option("database")))

    def usearch_run(self):
        """
        运行Usearch，生成OTU表
        """
        myopt = {
            'fasta': self.option('fasta'),
            'identity': self.option('confidence')
        }
        self.usearch.set_options(myopt)
        self.on_rely(self.usearch, self.qiimeassign_run)
        self.usearch.run()

    def qiimeassign_run(self, relyobj):
        """
        运行Qiime Assign,获取OTU的分类信息
        """
        myopt = dict()
        if self.option('database') == "customer_mode":
            myopt = {
                'fasta': relyobj.rely[0].option('otu_rep'),
                'revcomp': self.option('revcomp'),
                'confidence': self.option('confidence'),
                'database': self.option('database'),
                'ref_fasta': self.option('ref_fasta'),
                'ref_taxon': self.option('ref_taxon')
            }
        else:
            myopt = {
                'fasta': relyobj.rely[0].option('otu_rep'),
                'revcomp': self.option('revcomp'),
                'confidence': self.option('confidence'),
                'database': self.option('database')
            }
        self.qiimeassign.set_options(myopt)
        if self.option('subsample'):
            self.on_rely(self.usearch, self.subsample_run)
        else:
            self.on_rely([self.usearch, self.qiimeassign], self.otutaxonstat_run)
        self.qiimeassign.run()

    def subsample_run(self, relyobj):
        """
        运行mothur的subsample，进行抽平
        """
        myopt = dict()
        myopt = {
            'in_otu_table': relyobj.rely[0].option('otu_table')
        }
        self.subsample.set_options(myopt)
        self.on_rely([self.qiimeassign, self.subsample], self.otutaxonstat_run)
        self.subsample.on('end', self.set_subsample)
        self.subsample.run()

    def otutaxonstat_run(self, relyobj):
        """
        进行分类学统计，产生不同分类水平上OTU统计表
        """
        myopt = dict()
        if self.option('subsample'):
            myopt = {
                'in_otu_table': relyobj.rely[1].option('out_otu_table'),
                'taxon_file': relyobj.rely[0].option('taxon_file')
            }
        else:
            myopt = {
                'in_otu_table': relyobj.rely[0].option('otu_table'),
                'taxon_file': relyobj.rely[1].option('taxon_file')
            }
        self.otutaxonstat.set_options(myopt)
        self.otutaxonstat.on('end', self.set_output)
        self.otutaxonstat.run()

    def set_subsample(self):
        """
        运行subsample后，设置out_otu的路径
        """
        os.system('cp -r %s/SubSample/output %s/SubSample' % (self.work_dir, self.output_dir))
        self.subsample.option("in_otu_table").get_info()
        match = re.search(r"(^.+)(\..+$)", self.subsample.option("in_otu_table").prop['basename'])
        prefix = match.group(1)
        suffix = match.group(2)
        sub_sampled_otu = os.path.join(self.output_dir, "SubSample", prefix + ".subsample" + suffix)
        self.option("otu_table").set_path(sub_sampled_otu)

    def set_output(self):
        self.logger.info('set output')
        for root, dirs, files in os.walk(self.output_dir):
            for d in dirs:
                shutil.rmtree(os.path.join(self.output_dir, d))
            for f in files:
                os.remove(os.path.join(self.output_dir, f))
        os.system('cp -r %s/UsearchOtu/output %s/UsearchOtu' % (self.work_dir, self.output_dir))
        os.system('cp -r %s/QiimeAssign/output %s/QiimeAssign' % (self.work_dir, self.output_dir))
        if not self.option('subsample'):
            self.option('otu_table').set_path(self.output_dir + '/UsearchOtu/otu_table.xls')
        else:
            os.system('cp -r %s/SubSample/output %s/SubSample' % (self.work_dir, self.output_dir))
        os.system('cp -r %s/OtuTaxonStat/output %s/OtuTaxonStat' % (self.work_dir, self.output_dir))
        self.option('otu_seqids').set_path(self.output_dir + '/UsearchOtu/otu_seqids.txt')
        self.option('otu_biom').set_path(self.output_dir + '/UsearchOtu/otu_table.biom')
        self.option('otu_rep').set_path(self.output_dir + '/UsearchOtu/otu_reps.fasta')
        self.option('taxon_file').set_path(self.output_dir + '/QiimeAssign/seqs_tax_assignments.txt')
        self.option('otu_taxon_biom').set_path(self.output_dir + '/OtuTaxonStat/otu_taxon.biom')
        self.option('otu_taxon_table').set_path(self.output_dir + '/OtuTaxonStat/otu_taxon.xls')
        self.option('otu_taxon_dir').set_path(self.output_dir + '/OtuTaxonStat/tax_summary_a')
        self.logger.info('done')

    def run(self):
        super(OtuAnalysisModule, self).run()
        self.usearch_run()
        self.on_rely(self.otutaxonstat, self.end)
