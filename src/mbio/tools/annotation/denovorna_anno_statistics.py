# -*- coding: utf-8 -*-
# __author__ = 'wangbixuan'
# __modified__ = 'hesheng'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import subprocess
import Bio.SeqIO


class DenovornaAnnoStatisticsAgent(Agent):
    """
    to perform annotation statistics
    this tool can only be called by denovorna module
    author:wangbixuan
    last_modified:20160831
    """

    def __init__(self, parent):
        super(DenovornaAnnoStatisticsAgent, self).__init__(parent)
        options = [
            {"name": "trinity_fasta", "type": "infile", "format": "sequence.fasta"},
            {"name": "gene_fasta", "type": "infile", "format": "sequence.fasta"},
            {"name": "nr_blast_out", "type": "infile",
                "format": "align.blast.blast_xml"},
            {"name": "swiss_blast_out", "type": "infile",
                "format": "align.blast.blast_xml"},
            {"name": "string_blast_out", "type": "infile",
                "format": "align.blast.blast_xml"},
            {"name": "kegg_blast_out", "type": "infile",
                "format": "align.blast.blast_xml"},
            {"name": "ncbi_taxonomy_output_dir", "type": "string"},
            {"name": "go_output_dir", "type": "string"},
            {"name": "cog_output_dir", "type": "string"},
            {"name": "kegg_output_dir", "type": "string"},
            {"name": "blast_stat_output_dir", "type": "string"},
            {"name": "unigene", "type": "bool"},  # 当gene fasta被提供时可用
            {"name": 'uni_nr', "type": "outfile", "format": "align.blast.blast_xml"},
            {"name": 'uni_st', "type": "outfile", "format": "align.blast.blast_xml"},
            {"name": 'uni_ke', "type": "outfile", "format": "align.blast.blast_xml"}

        ]
        self.add_option(options)
        self.step.add_steps('denovo_rna_statistics')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.denovo_rna_statistics.start()
        self.step.update()

    def step_end(self):
        self.step.denovo_rna_statistics.finish()
        self.step.update()

    def check_options(self):
        if self.option('unigene') is True:
            if not self.option('gene_fasta').is_set:
                raise OptionError("必须提供unigene的fasta文件")
        if not self.option('trinity_fasta').is_set:
            raise OptionError("必须提供trinity标准fasta文件")
        else:
            if self.option('gene_fasta').is_set:
                trinity = []
                for seq_record in Bio.SeqIO.parse(open(self.option('trinity_fasta').prop['path']), 'fasta'):
                    trinity.append(seq_record.id)
                gene = []
                for gene_seq_record in Bio.SeqIO.parse(open(self.option('gene_fasta').prop['path']), 'fasta'):
                    gene.append(gene_seq_record.id)
                for gid in gene:
                    if gid not in trinity:
                        raise OptionError(
                            "unigene_fasta文件存在trinity_fasta文件中不存在的序列")
                        break
        if not self.option('nr_blast_out').is_set:
            raise OptionError("必须提供nr对比blast结果xml文件")
        # if not self.option('swiss_blast_out').is_set:
            # raise OptionError("必须提供swissprot对比blast结果xml文件")
        if not self.option('string_blast_out').is_set:
            raise OptionError("必须提供string对比blast结果xml文件")
        if not self.option('kegg_blast_out').is_set:
            raise OptionError("必须提供kegg对比blast结果xml文件")
        if self.option('ncbi_taxonomy_output_dir').is_set:
            if not os.path.exists(self.option('ncbi_taxonomy_output_dir') + '/query_taxons_detail.xls'):
                raise OptionError(
                    "ncbi_taxonomy输出文件缺少'query_taxons_detail.xls'文件")
        else:
            raise OptionError("必须提供ncbi taxon输出结果文件夹路径")
        if self.option('go_output_dir').is_set:
            for gofile in ['blast2go.annot', 'query_gos.list', 'go1234level_statistics.xls',
                           'go2level.xls', 'go3level.xls', 'go4level.xls']:
                if not os.path.exists(self.option('go_output_dir') + '/' + gofile):
                    raise OptionError("go annotation输出文件缺少'%s'文件" % gofile)
        else:
            raise OptionError("必须提供go annotation输出结果文件夹路径")
        if self.option('cog_output_dir').is_set:
            for cogfile in ['cog_list.xls', 'cog_summary.xls', 'cog_table.xls']:
                if not os.path.exists(self.option('cog_output_dir') + '/' + cogfile):
                    raise OptionError("string2cog输出文件缺少'%s'文件" % cogfile)
        else:
            raise OptionError("必须提供string2cog输出结果文件夹路径")
        if self.option('kegg_output_dir').is_set:
            for keggfile in ['kegg_table.xls', 'pathway_table.xls', 'kegg_taxonomy.xls']:
                if not os.path.exists(self.option('kegg_output_dir') + '/' + keggfile):
                    raise OptionError("KEGG annotation输出文件缺少\'%s\'文件" % keggfile)
        else:
            raise OptionError("必须提供KEGG annotation输出结果文件夹路径")
        if self.option('blast_stat_output_dir').is_set:
            # !important: change file names if needed!
            for bstatfile in ['evalue_statistics.xls', 'similarity_statistics.xls']:
                if not os.path.exists(self.option('blast_stat_output_dir') + '/' + bstatfile):
                    raise OptionError("blast stat输出文件缺少'%s'文件" % bstatfile)
        else:
            raise OptionError("必须提供blast stat输出结果文件夹路径")

    def set_resource(self):
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./all_annotation.xls", "xls", "注释综合表"],
            ["./all_annotation_statistics.xls", "xls", "注释综合统计表"],
            ["./venn_table.xls", "xls", "文氏图参考表"],
            ["./unigene/blast/unigene_nr.xls", "xls", "unigene blast result on nr"],
            ["./unigene/blast/unigene_nr.xml", "xml", "unigene blast result on nr"],
            ["./unigene/blast/unigene_string.xml",
                "xml", "unigene blast result on string"],
            ["./unigene/blast/unigene_string.xls",
                "xls", "unigene blast result on string"],
            ["./unigene/blast/unigene_kegg.xml",
                "xml", "unigene blast result on kegg"],
            ["./unigene/blast/unigene_kegg.xls",
                "xls", "unigene blast result on kegg"],
            ["./unigene/blast_nr_statistics/evalue_statistics.xls",
                "xls", "blast nr evalue 统计结果"],
            ["./unigene/blast_nr_statistics/similarity_statistics.xls",
                "xls", "blast nr similarity 统计结果"],
            ["./unigene/ncbi_taxonomy/unigene_query_taxons_statistics.xls",
                "xls", "unigene NCBI物种分类统计"],
            ["./unigene/go/unigene_blast2go.annot", "annot", "unigene blast2go结果"],
            ["./unigene/go/unigene_query_gos.list", "list", "unigene GO列表"],
            ["./unigene/go/unigene_go1234level_statistics.xls", "xls", "GO逐层统计表"],
            ["./unigene/go/unigene_go2level.xls", "xls", "GO level2统计表"],
            ["./unigene/go/unigene_go3level.xls", "xls", "GO level3统计表"],
            ["./unigene/go/unigene_go4level.xls", "xls", "GO level4统计表"],
            ["./unigene/cog/unigene_cog_list.xls", "xls", "unigene COG id表"],
            ["./unigene/cog/unigene_cog_summary.xls", "xls", "unigene COG功能分类统计"],
            ["./unigene/cog/unigene_cog_table.xls", "xls", "unigene COG综合统计表"],
            ["./unigene/kegg/unigene_kegg_table.xls", "xls", "unigene KEGG ID表"],
            ["./unigene/kegg/unigene_pathway_table.xls",
                "xls", "unigene KEGG pathway表"],
            ["./unigene/kegg/unigene_kegg_taxonomy.xls",
                "xls", "unigene KEGG 二级分类统计表"]
        ])
        super(DenovornaAnnoStatisticsAgent, self).end()


class DenovornaAnnoStatisticsTool(Tool):

    def __init__(self, config):
        super(DenovornaAnnoStatisticsTool, self).__init__(config)
        self._version = "1.0"

    def run_annotStat(self):
        cmd = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/annotStat.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd += ' %s %s %s %s %s %s %s %s %s' % (self.option('trinity_fasta').prop['path'],
                                                self.option('nr_blast_out').prop['path'], self.option(
                                                    'swiss_blast_out').prop['path'],
                                                self.option('string_blast_out').prop[
            'path'], self.option('ncbi_taxonomy_output_dir'),
            self.option('go_output_dir').prop['path'], self.option(
                'cog_output_dir').prop['path'],
            self.option('kegg_output_dir').prop['path'], self.worl_dir)
        self.logger.info("运行annotStat.py")
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info("运行annotStat.py完成")
            for stat_file in ['all_annotation.xls', 'all_annotation_statistics.xls', 'venn_table.xls']:
                self.linkfile(self.work_dir, stat_file, self.output_dir)
        except subprocess.CalledProcessError:
            self.set_error("运行annotStat.py出错")
        self.end()

    def linkfile(self, workdir, filename, outdir):
        if os.path.exists(outdir + '/' + filename):
            os.remove(outdir + '/' + filename)
        os.link(workdir + '/' + filename, outdir + '/' + filename)

    def run_annotUnigene(self):
        if self.option('nr_blast_out').is_set:
            nr_blast = self.option('nr_blast_out').prop['path']
        else:
            nr_blast = '0'
        if self.option('swiss_blast_out').is_set:
            swiss_blast = self.option('swiss_blast_out').prop['path']
        else:
            swiss_blast = '0'
        if self.option('string_blast_out').is_set:
            string_blast = self.option('string_blast_out').prop['path']
        else:
            string_blast = '0'
        if self.option('kegg_blast_out').is_set:
            kegg_blast = self.option('kegg_blast_out').prop['path']
        else:
            kegg_blast = '0'
        if self.option('ncbi_taxonomy_output_dir').is_set:
            taxo_dir = self.option('ncbi_taxonomy_output_dir')
        else:
            taxo_dir = '0'
        if self.option('go_output_dir').is_set:
            go_dir = self.option('go_output_dir')
        else:
            go_dir = '0'
        if self.option('cog_output_dir').is_set:
            cog_dir = self.option('cog_output_dir')
        else:
            cog_dir = '0'
        if self.option('kegg_output_dir').is_set:
            kegg_dir = self.option('kegg_output_dir')
        else:
            kegg_dir = '0'
        if self.option('blast_stat_output_dir').is_set:
            blastat_dir = self.option('blast_stat_output_dir')
        else:
            blastat_dir = '0'
        if self.option('gene_fasta').is_set:
            gene_fasta = self.option('gene_fasta').prop['path']
        else:
            gene_fasta = '0'
        cmd1 = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/annotStatNew.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd1 += ' %s %s %s %s %s %s %s %s %s %s %s %s %s' % (self.option('trinity_fasta').prop['path'],
                                                             self.option('nr_blast_out').prop['path'], self.option(
                                                                 'swiss_blast_out').prop['path'],
                                                             self.option('string_blast_out').prop['path'], self.option(
                'kegg_blast_out').prop['path'],
            self.option('ncbi_taxonomy_output_dir'),
            self.option('go_output_dir').prop['path'], self.option(
                'cog_output_dir').prop['path'],
            self.option('kegg_output_dir').prop[
            'path'], self.work_dir + '/unigene',
            self.option('blast_stat_output_dir'), self.option('gene_fasta').prop['path'],
            self.work_dir)
        self.logger.info("运行annotUnigene.py")
        self.logger.info(cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
            self.logger.info("运行annotUnigene.py完成")
            for u_stat_file in ['all_annotation.xls', 'all_annotation_statistics.xls', 'venn_table.xls']:
                self.linkfile(self.work_dir, u_stat_file, self.output_dir)
            for blast_unigene_file in ['unigene_nr.xml', 'unigene_string.xml', 'unigene_kegg.xml']:
                self.linkfile(self.work_dir + '/unigene/blast',
                              blast_unigene_file, self.output_dir + '/unigene/blast')
            self.linkfile(self.work_dir + '/unigene/ncbi_taxonomy', 'unigene_query_taxons_detail.xls',
                          self.output_dir + '/unigene/ncbi_taxonomy')
            for go_unigene_file in ['unigene_blast2go.annot', 'unigene_query_gos.list',
                                    'unigene_go1234level_statistics.xls', 'unigene_go2level.xls',
                                    'unigene_go2level.xls', 'unigene_go4level.xls']:
                self.linkfile(self.work_dir + '/unigene/go', go_unigene_file, self.output_dir + '/unigene/go')
            for cog_unigene_file in ['unigene_cog_list.xls', 'unigene_cog_summary.xls', 'unigene_cog_table.xls']:
                self.linkfile(self.work_dir + '/unigene/cog', cog_unigene_file, self.output_dir + '/unigene/cog')
            for kegg_unigene_file in ['unigene_kegg_table.xls', 'unigene_pathway_table.xls',
                                      'unigene_kegg_taxonomy.xls']:
                self.linkfile(self.work_dir + '/unigene/kegg',
                              kegg_unigene_file, self.output_dir + '/unigene/kegg')
            self.option('uni_nr', self.work_dir + '/unigene/blast/unigene_nr.xml')
            self.option('uni_st', self.work_dir + '/unigene/blast/unigene_string.xml')
            self.option('uni_ke', self.work_dir + '/unigene/blast/unigene_kegg.xml')
        except subprocess.CalledProcessError:
            self.set_error("运行annotUnigene出错")
        self.run_xmltoxls()

    def run_xmltoxls(self):
        inputfile = self.option('uni_nr').prop['path']
        if self.option("uni_nr").format == "align.blast.blast_xml":
            self.logger.info(
                '程序输出结果为6(xml)，实际需要结果为5(xls)，开始调用程序xml2table转换')
            inputfile = inputfile + "tmp.xls"
            self.option('uni_nr').convert2table(inputfile)
            self.logger.info("nr格式转变完成")
            if os.path.exists(self.output_dir + '/unigene/blast/unigene_nr.xls'):
                os.remove(self.output_dir + '/unigene/blast/unigene_nr.xls')
            os.link(inputfile, self.output_dir + '/unigene/blast/unigene_nr.xls')
        inputfile2 = self.option('uni_st').prop['path']
        if self.option("uni_st").format == "align.blast.blast_xml":
            self.logger.info(
                '程序输出结果为6(xml)，实际需要结果为5(xls)，开始调用程序xml2table转换')
            inputfile2 = inputfile2 + "tmp.xls"
            self.option('uni_st').convert2table(inputfile2)
            self.logger.info("string格式转变完成")
            if os.path.exists(self.output_dir + '/unigene/blast/unigene_string.xls'):
                os.remove(self.output_dir + '/unigene/blast/unigene_string.xls')
            os.link(inputfile2, self.output_dir + '/unigene/blast/unigene_string.xls')
        inputfile3 = self.option('uni_ke').prop['path']
        if self.option("uni_ke").format == "align.blast.blast_xml":
            self.logger.info(
                '程序输出结果为6(xml)，实际需要结果为5(xls)，开始调用程序xml2table转换')
            inputfile3 = inputfile3 + "tmp.xls"
            self.option('uni_ke').convert2table(inputfile3)
            self.logger.info("kegg格式转变完成")
            if os.path.exists(self.output_dir + '/unigene/blast/unigene_kegg.xls'):
                os.remove(self.output_dir + '/unigene/blast/unigene_kegg.xls')
            os.link(inputfile3, self.output_dir + '/unigene/blast/unigene_kegg.xls')
        self.blastinfile = inputfile
        self.run_blast_stat()

    def run_blast_stat(self):
        cmd2 = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/blastout_statistics.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd2 += ' %s %s' % (self.blastinfile, self.work_dir + '/unigene/blast_nr_statistics/')
        self.logger.info("运行blastout_statistics.py")
        self.logger.info(cmd2)
        try:
            subprocess.check_output(cmd2, shell=True)
            self.logger.info("运行blastout_statistics.py完成")
            if os.path.exists(self.output_dir + '/unigene/blast_nr_statistics/unigene_evalue_statistics.xls'):
                os.remove(self.output_dir + '/unigene/blast_nr_statistics/unigene_evalue_statistics.xls')
            os.link(self.work_dir + '/unigene/blast_nr_statistics/output_evalue.xls',
                    self.output_dir + '/unigene/blast_nr_statistics/unigene_evalue_statistics.xls')
            if os.path.exists(self.output_dir + '/unigene/blast_nr_statistics/unigene_similarity_statistics.xls'):
                os.remove(self.output_dir + '/unigene/blast_nr_statistics/unigene_similarity_statistics.xls')
            os.link(self.work_dir + '/unigene/blast_nr_statistics/output_similar.xls',
                    self.output_dir + '/unigene/blast_nr_statistics/unigene_similarity_statistics.xls')
        except subprocess.CalledProcessError:
            self.set_error("运行blastout_statistics.py出错")
        self.end()

    def run(self):
        super(DenovornaAnnoStatisticsTool, self).run()
        self.run_annotUnigene()
