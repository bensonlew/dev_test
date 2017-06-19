# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from __future__ import division
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.align.blast.xml2table import xml2table
from mbio.packages.annotation.cog_stat import cog_stat
from mbio.packages.annotation.nr_stat import nr_stat
from mbio.packages.align.blast.blastout_statistics import *
from mbio.packages.annotation.transcript_gene import transcript_gene
import os
import re
import shutil
from biocluster.config import Config
import traceback
import subprocess
from collections import defaultdict


class RefAnnoStatAgent(Agent):
    """
    统计有参转录组注释模块NR、GO、COG、KEGG、SwissProt的相关信息
    version v1.0
    author: qiuping
    last_modify: 2016.10.20
    last_author:zengjing
    last_modify: 2017.02.14
    """
    def __init__(self, parent):
        super(RefAnnoStatAgent, self).__init__(parent)
        options = [
            {"name": "nr_xml", "type": "infile", "format": "align.blast.blast_xml"},  # blast比对到nr库的xml结果文件
            {"name": "blast2go_annot", "type": "infile", "format": "annotation.go.blast2go_annot"},
            {"name": "gos_list", "type": "infile", "format": "annotation.go.go_list"},  # go注释tool的结果文件
            {"name": "gos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},   # 客户上传go注释文件,用blast2go_annot、gos_list或gos_list_upload进行go注统计
            {"name": "kegg_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 客户上传kegg注释文件,kegg_xml或kos_list_upload进行kegg注释统计
            {"name": "string_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "string_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "cog_list", "type": "infile", "format": "annotation.cog.cog_list"},
            {"name": "cog_table", "type": "infile", "format": "annotation.cog.cog_table"},
            {"name": "pfam_domain", "type": "infile", "format": "annotation.kegg.kegg_list"},
            {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},
            {"name": "swissprot_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组gtf文件/新转录本gtf文件
            {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
            {"name": "database", "type": "string", "default": "nr,go,cog,pfam,kegg,swissprot"},
            {"name": "gene_nr_table", "type": "outfile", "format": "align.blast.blast_table"},
            {"name": "gene_string_table", "type": "outfile", "format": "align.blast.blast_table"},
            {"name": "gene_kegg_table", "type": "outfile", "format": "align.blast.blast_table"},
            {"name": "gene_swissprot_table", "type": "outfile", "format": "align.blast.blast_table"},
            {"name": "gene_go_level_2", "type": "outfile", "format": "annotation.go.level2"},
            {"name": "gene_go_list", "type": "outfile", "format": "annotation.go.go_list"},
            {"name": "gene_kegg_anno_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
            {"name": "gene_pfam_domain", "type": "outfile", "format": "annotation.kegg.kegg_list"},
        ]
        self.add_option(options)
        self.step.add_steps("denovo_anno_stat")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)
        self.queue = 'BLAST2GO'  # 投递到指定的队列BLAST2GO

    def stepstart(self):
        self.step.denovo_anno_stat.start()
        self.step.update()

    def stepfinish(self):
        self.step.denovo_anno_stat.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        self.anno_database = set(self.option('database').split(','))
        if len(self.anno_database) < 1:
            raise OptionError('至少选择一种注释库')
        if not self.option("ref_genome_gtf").is_set:
            raise OptionError("缺失参考基因组gtf文件")
        for i in self.anno_database:
            if i not in ['nr', 'go', 'cog', 'pfam', 'kegg', 'swissprot']:
                raise OptionError('需要注释的数据库不在支持范围内[nr, go, cog, pfam, kegg, swissprot]:{}'.format(i))
            if i == 'go':
                if self.option('gos_list').is_set and self.option('blast2go_annot').is_set:
                    pass
                elif self.option('gos_list_upload').is_set:
                    pass
                else:
                    raise OptionError('缺少go注释的输入文件')
            if i == 'cog':
                if self.option('string_xml').is_set:
                    pass
                elif self.option('string_table').is_set:
                    pass
                else:
                    raise OptionError('进行cog注释统计必须输入blast到string数据库的xml文件或table文件')
                if not self.option('cog_list').is_set and not self.option('cog_table').is_set:
                    raise OptionError('缺少cog注释的输入文件:cog_list、cog_table')
            if i == 'nr' and not self.option('nr_xml').is_set:
                raise OptionError('缺少nr注释的输入文件')
            if i == "pfam" and not self.option('pfam_domain').is_set:
                raise OptionError('缺少pfam注释的输入文件')
            if i == 'kegg':
                if self.option('kegg_xml').is_set:
                    pass
                elif self.option('kos_list_upload').is_set:
                    pass
                else:
                    raise OptionError('缺少kegg注释的输入文件')
            if i == 'swissprot' and not self.option('swissprot_xml').is_set:
                raise OptionError('缺少swissprot注释的输入文件')
        if not self.option('gene_file').is_set:
            raise OptionError('缺少gene输入文件')
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '30G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [
            [".", "", "denovo注释统计结果输出目录"],
            ["/blast_nr_statistics/", "dir", "blast比对nr库evalue等值统计目录"],
            ["/blast_swissprot_statistics/", "dir", "blast比对swissprot库evalue等值统计目录"],
            ["/cog_stat/", "dir", "cog统计结果目录"],
            ["/go_stat/", "dir", "go统计结果目录"],
            ["/kegg_stat/", "dir", "kegg统计结果目录"],
            ["/pfam_stat/", "dir", "pfam统计结果目录"],
            ["/blast/", "dir", "基因序列blast比对结果目录"],
            ["/blast/gene_kegg.xls", "xls", "基因序列blast比对kegg注释结果table"],
            ["/blast/gene_nr.xls", "xls", "基因序列blast比对nr注释结果table"],
            ["/blast/gene_string.xls", "xls", "基因序列blast比对string注释结果table"],
            ["/blast/gene_string.xml", "xml", "基因序列blast比对string注释结果xml"],
            ["/blast/gene_kegg.xml", "xml", "基因序列blast比对kegg注释结果xml"],
            ["/blast/gene_string.xml", "xml", "基因序列blast比对string注释结果xml"],
            ["/blast/gene_swissprot.xlm", "xml", "基因序列blast比对到swissprot注释结果xml"],  # 5
            ["/blast/gene_swissprot.xls", "xls", "基因序列blast比对到swissprot注释结果table"], # 6
            ["/cog_stat/gene_cog_list.xls", "xls", "基因序列cog_list统计结果"],
            ["/cog_stat/gene_cog_summary.xls", "xls", "基因序列cog_summary统计结果"],
            ["/cog_stat/gene_cog_table.xls", "xls", "基因序列cog_table统计结果"],
            ["/pfam_stat/gene_pfam_domain", "", "基因序列pfam_domain统计结果"],
            ["/go_stat/gene_blast2go.annot", "annot", "Go annotation based on blast output of gene"],
            ["/go_stat/gene_gos.list", "list", "Merged Go annotation of gene"],
            ["/go_stat/gene_go1234level_statistics.xls", "xls", "Go annotation on 4 levels of gene"],
            ["/go_stat/gene_go123level_statistics.xls", "xls", "Go annotation on 3 levels of gene"],
            ["/go_stat/gene_go12level_statistics.xls", "xls", "Go annotation on 2 levels of gene"],
            ["/go_stat/gene_go2level.xls", "xls", "Go annotation on level 2 of gene"],
            ["/go_stat/gene_go3level.xls", "xls", "Go annotation on level 3 of gene"],
            ["/go_stat/gene_go4level.xls", "xls", "Go annotation on level 4 of gene"],
            ["/kegg_stat/gene_kegg_table.xls", "xls", "KEGG annotation table of gene"],
            ["/kegg_stat/gene_pathway_table.xls", "xls", "Sorted pathway table of gene"],
            ["/kegg_stat/gene_kegg_taxonomy.xls", "xls", "KEGG taxonomy summary of gene"],
            ["/kegg_stat/gene_kegg_layer.xls", "xls", "KEGG taxonomy summary of gene"],
            ["/kegg_stat/gene_pathway/", "dir", "基因的标红pathway图"],
            ["/blast_nr_statistics/gene_nr_evalue.xls", "xls", "基因序列blast结果E-value统计"],
            ["/blast_nr_statistics/gene_nr_similar.xls", "xls", "基因序列blast结果similarity统计"],
            ["/blast_nr_statistics/nr_evalue.xls", "xls", "转录本序列blast结果E-value统计"],
            ["/blast_nr_statistics/nr_similar.xls", "xls", "转录本序列blast结果similarity统计"],
            ["/blast_swissprot_statistics/gene_swissprot_evalue.xls", "xls", "基因序列blast结果E-value统计"],
            ["/blast_swissprot_statistics/gene_swissprot_similar.xls", "xls", "基因序列blast结果similarity统计"],
            ["/blast_swissprot_statistics/swissprot_evalue.xls", "xls", "转录本序列blast结果E-value统计"],
            ["/blast_swissprot_statistics/swissprot_similar.xls", "xls", "转录本序列blast结果similarity统计"],
        ]
        result_dir.add_relpath_rules(relpath)
        result_dir.add_regexp_rules([
        ])
        super(RefAnnoStatAgent, self).end()


class RefAnnoStatTool(Tool):
    """
    有参注释统计tool
    """
    def __init__(self, config):
        super(RefAnnoStatTool, self).__init__(config)
        self._version = '1.0.1'
        self.b2g_user = "biocluster102"
        self.b2g_password = "sanger-dev-123"
        self.python_path = "/program/Python/bin/python"
        self.denovo_stat = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/denovo_stat/'
        self.go_annot = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/goAnnot.py'
        self.go_split = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/goSplit.py'
        self.kegg_path = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/kegg_annotation.py'
        self.cog_xml = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/string2cog_v9.py'
        self.cog_table = self.config.SOFTWARE_DIR + '/bioinfo/annotation/scripts/cog_annot.py'
        self.image_magick = self.config.SOFTWARE_DIR + "/program/ImageMagick/bin/convert"
        self.taxonomy_path = self.config.SOFTWARE_DIR + "/database/KEGG/species/{}.ko.txt".format(self.option("taxonomy"))
        self.gene_list = self.option('gene_file').prop['gene_list']
        self.gene_nr_xml = self.work_dir + '/blast/gene_nr.xml'
        if self.option('string_xml').is_set:
            self.gene_string_xml = self.work_dir + '/blast/gene_string.xml'
        else:
            self.gene_string_table = self.work_dir + '/blast/gene_string.xls'
        self.gene_swissprot_xml = self.work_dir + '/blast/gene_swissprot.xml'
        if self.option("kegg_xml").is_set:
            self.gene_kegg_xml = self.work_dir + '/blast/gene_kegg.xml'
        dir_list = ['/blast/', '/cog_stat/', '/go_stat/', '/pfam_stat/', '/kegg_stat/', '/blast_nr_statistics/', '/blast_swissprot_statistics/']
        for i in dir_list:
            if os.path.exists(self.work_dir + i):
                shutil.rmtree(self.work_dir + i)
            os.makedirs(self.work_dir + i)
        if not os.path.exists(self.output_dir + '/venn'):
            os.makedirs(self.output_dir + '/venn')
        tran_gene = transcript_gene().get_gene_transcript(gtf_path=self.option("ref_genome_gtf").prop["path"])
        self.tran_gene = tran_gene[0]
        self.tran_list = tran_gene[1]
        self.database = self.option('database').split(',')
        self.gene_anno_list = {}  # 注释到的基因序列名字
        self.anno_list = {}  # 注释到的转录本序列名字

    def run_nr_stat(self):
        # 筛选gene_nr.xml、gene_nr.xls
        self.logger.info("开始筛选gene_nr.xml、gene_nr.xls")
        self.option('nr_xml').sub_blast_xml(genes=self.gene_list, new_fp=self.gene_nr_xml, trinity_mode=False)
        transcript_gene().get_gene_blast_xml(tran_list=self.tran_list, tran_gene=self.tran_gene, xml_path=self.gene_nr_xml, gene_xml_path=self.gene_nr_xml)
        xml2table(self.gene_nr_xml, self.work_dir + '/blast/gene_nr.xls')
        xml2table(self.option('nr_xml').prop['path'], self.work_dir + '/blast/nr.xls')
        self.logger.info("完成筛选gene_nr.xml、gene_nr.xls")
        # stat gene_evalue and gene_simillar for NR
        try:
            blastout_statistics(blast_table=self.work_dir + '/blast/gene_nr.xls', evalue_path=self.work_dir + '/blast_nr_statistics/gene_nr_evalue.xls', similarity_path=self.work_dir + '/blast_nr_statistics/gene_nr_similar.xls')
            blastout_statistics(blast_table=self.work_dir + '/blast/nr.xls', evalue_path=self.work_dir + '/blast_nr_statistics/nr_evalue.xls', similarity_path=self.work_dir + '/blast_nr_statistics/nr_similar.xls')
            self.logger.info("End: evalue,similar for gene nr blast table ")
        except Exception as e:
            self.set_error("运行nr evalue,similar for gene nr blast table出错:{}".format(e))
            self.logger.info("Error: evalue,similar for gene nr blast table")

    def run_cog_stat(self):
        self.cog_stat_path = self.work_dir + '/cog_stat/'
        # 筛选gene_string.xml、gene_string.xls
        self.logger.info("开始筛选gene_string.xml、gene_string.xls")
        if self.option('string_xml').is_set:
            self.option('string_xml').sub_blast_xml(genes=self.gene_list, new_fp=self.gene_string_xml, trinity_mode=False)
            transcript_gene().get_gene_blast_xml(tran_list=self.tran_list, tran_gene=self.tran_gene, xml_path=self.gene_string_xml, gene_xml_path=self.gene_string_xml)
            xml2table(self.gene_string_xml, self.work_dir + '/blast/gene_string.xls')
            self.logger.info("完成筛选gene_string.xml、gene_string.xls")
            cmd = '{} {} {} {}'.format(self.python_path, self.cog_xml, self.gene_string_xml, self.cog_stat_path)
        else:
            self.option('string_table').sub_blast_table(genes=self.gene_list, new_fp=self.work_dir + '/gene_string.xls')
            transcript_gene().get_gene_blast_table(tran_list=self.tran_list, tran_gene=self.tran_gene, table_path=self.work_dir + '/gene_string.xls', gene_table_path=self.gene_string_table)
            cmd = '{} {} {} {}'.format(self.python_path, self.cog_table, self.gene_string_table, self.cog_stat_path)
        cog_cmd = self.add_command('cog_cmd', cmd).run()
        self.wait(cog_cmd)
        if cog_cmd.return_code == 0:
            self.logger.info("cog_cmd运行完成")
            outfiles = ['cog_list.xls', 'cog_summary.xls', 'cog_table.xls']
            for item in outfiles:
                f1 = self.cog_stat_path + item
                f2 = self.cog_stat_path + 'gene_' + item
                os.rename(f1, f2)
        else:
            self.set_error("cog_cmd运行出错!")

    def run_pfam_stat(self):
        self.pfam_stat_path = self.work_dir + '/pfam_stat/'

        def get_gene_pfam(pfam_domain, gene_list, outpath):
            """
            将pfam注释的结果文件pfam_domain筛选致函基因的结果信息
            """
            with open(pfam_domain, 'rb') as f, open(outpath, 'wb') as w:
                lines = f.readlines()
                w.write(lines[0])
                for line in lines:
                    item = line.strip().split('\t')
                    name = item[0]
                    if name in gene_list:
                        line = re.sub(r"{}".format(name), self.tran_gene[name], line)
                        w.write(line)
        get_gene_pfam(pfam_domain=self.option('pfam_domain').prop['path'], gene_list=self.gene_list, outpath=self.pfam_stat_path + 'gene_pfam_domain')
        self.option('gene_pfam_domain', self.pfam_stat_path + 'gene_pfam_domain')

    def run_kegg_stat(self):
        self.kegg_stat_path = self.work_dir + '/kegg_stat/'
        gene_pathway = self.kegg_stat_path + '/gene_pathway/'
        if os.path.exists(gene_pathway):
            shutil.rmtree(gene_pathway)
        os.makedirs(gene_pathway)
        if self.option("kegg_xml").is_set:
            # 筛选gene_kegg.xml、gene_kegg.xls
            self.logger.info("开始筛选gene_kegg.xml、gene_kegg.xls")
            self.option('kegg_xml').sub_blast_xml(genes=self.gene_list, new_fp=self.gene_kegg_xml, trinity_mode=False)
            transcript_gene().get_gene_blast_xml(tran_list=self.tran_list, tran_gene=self.tran_gene, xml_path=self.gene_kegg_xml, gene_xml_path=self.gene_kegg_xml)
            xml2table(self.gene_kegg_xml, self.work_dir + '/blast/gene_kegg.xls')
            self.logger.info("完成筛选gene_kegg.xml、gene_kegg.xls")
        kegg_table = self.kegg_stat_path + '/gene_kegg_table.xls'
        pidpath = self.work_dir + '/gene_pid.txt'
        pathway_table = self.kegg_stat_path + '/gene_pathway_table.xls'
        layerfile = self.kegg_stat_path + '/gene_kegg_layer.xls'
        taxonomyfile = self.kegg_stat_path + '/gene_kegg_taxonomy.xls'
        if self.option("taxonomy"):
            taxonomy = self.taxonomy_path
        else:
            taxonomy = None
        if self.option("kegg_xml").is_set:
            cmd = "{} {} {} {} {} {} {} {} {} {} {} {}".format(self.python_path, self.kegg_path, self.gene_kegg_xml, None, kegg_table, pidpath, gene_pathway, pathway_table, layerfile, taxonomyfile, taxonomy, self.image_magick)
        else:
            self.option("kos_list_upload").get_gene_anno(outdir=self.work_dir + "/gene_kegg.list")
            kegg_ids = self.work_dir + "/gene_kegg.list"
            cmd = "{} {} {} {} {} {} {} {} {} {} {} {}".format(self.python_path, self.kegg_path, None, kegg_ids, kegg_table, pidpath, gene_pathway, pathway_table, layerfile, taxonomyfile, taxonomy, self.image_magick)
        self.logger.info("开始运行kegg注释脚本")
        command = self.add_command("kegg_anno", cmd).run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行kegg注释脚本完成")
        else:
            self.set_error("运行kegg注释脚本出错")

    def run_go_stat(self):
        self.go_stat_path = self.work_dir + '/go_stat/'

        def get_gene_go(go_result, gene_list, outpath, trinity_mode=False):
            """
            将go_annotation注释的结果文件筛选只包含基因的结果信息,保留含有基因序列的行
            go_result:go_annotation tool运行得到的blast2go.annot或query_gos.list结果文件；
            gene_list: 只包含基因序列名字的列表
            """
            with open(go_result, 'rb') as c, open(outpath, 'wb') as w:
                for line in c:
                    line = line.strip('\n').split('\t')
                    name = line[0]
                    if name in gene_list:
                        if trinity_mode:
                            name = name.split('_i')[0]
                        line[0] = name
                        w_line = '\t'.join(line)
                        w.write(w_line + '\n')
        if self.option("blast2go_annot").is_set and self.option("gos_list").is_set:
            get_gene_go(go_result=self.option('blast2go_annot').prop['path'], gene_list=self.gene_list, outpath=self.go_stat_path + '/gene_blast2go.annot')
            get_gene_go(go_result=self.option('gos_list').prop['path'], gene_list=self.gene_list, outpath=self.go_stat_path + '/gene_gos.list')
            transcript_gene().get_gene_go_list(tran_list=self.tran_list, tran_gene=self.tran_gene, go_list=self.go_stat_path + '/gene_blast2go.annot', gene_go_list=self.go_stat_path + '/gene_blast2go.annot')
            transcript_gene().get_gene_go_list(tran_list=self.tran_list, tran_gene=self.tran_gene, go_list=self.go_stat_path + '/gene_gos.list', gene_go_list=self.go_stat_path + '/gene_gos.list')
        else:
            self.option("gos_list_upload").get_transcript_anno(outdir=self.work_dir + "/query_gos.list")
            self.option("gos_list_upload").get_gene_anno(outdir=self.go_stat_path + '/gene_gos.list')
            self.option("gos_list", self.work_dir + "/query_gos.list")
        self.option("gene_go_list", self.go_stat_path + '/gene_gos.list')
        go_cmd1 = '{} {} {} {} {} {}'.format(self.python_path, self.go_annot, self.go_stat_path + '/gene_gos.list', 'localhost', self.b2g_user, self.b2g_password)
        # go_cmd2 = '{} {} {}'.format(self.python_path, self.go_split, self.work_dir + '/go_detail.xls')
        go_annot_cmd = self.add_command('go_annot_cmd', go_cmd1).run()
        self.wait(go_annot_cmd)
        if go_annot_cmd.return_code == 0:
            self.logger.info("go_annot_cmd运行完成")
            # self.add_command('go_split_cmd', go_cmd2).run()
        else:
            self.set_error("go_annot_cmd运行出错!")

    def run_swissprot_stat(self):
        self.logger.info("开始筛选gene_swissprot.xml、gene_swissprot.xls")
        self.option('swissprot_xml').sub_blast_xml(genes=self.gene_list, new_fp=self.gene_swissprot_xml, trinity_mode=False)
        transcript_gene().get_gene_blast_xml(tran_list=self.tran_list, tran_gene=self.tran_gene, xml_path=self.gene_swissprot_xml, gene_xml_path=self.gene_swissprot_xml)
        xml2table(self.gene_swissprot_xml, self.work_dir + '/blast/gene_swissprot.xls')
        xml2table(self.option('swissprot_xml').prop['path'], self.work_dir + '/blast/swissprot.xls')
        self.logger.info("完成筛选gene_swissprot.xml、gene_swissprot.xls")
        try:
            blastout_statistics(blast_table=self.work_dir + '/blast/gene_swissprot.xls', evalue_path=self.work_dir + '/blast_swissprot_statistics/gene_swissprot_evalue.xls', similarity_path=self.work_dir + '/blast_swissprot_statistics/gene_swissprot_similar.xls')
            blastout_statistics(blast_table=self.work_dir + '/blast/swissprot.xls', evalue_path=self.work_dir + '/blast_swissprot_statistics/swissprot_evalue.xls', similarity_path=self.work_dir + '/blast_swissprot_statistics/swissprot_similar.xls')
            self.logger.info("End: evalue,similar for gene nr blast table ")
        except Exception as e:
            self.set_error("运行swissprot evalue,similar for gene swissprot blast table出错:{}".format(e))
            self.logger.info("Error: evalue,similar for gene swissprot blast table")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        try:
            self.logger.info("设置注释统计结果目录")
            self.movedir2output(self.work_dir + '/blast/', 'blast')
            for db in self.database:
                if db == 'cog':
                    self.movedir2output(self.cog_stat_path, 'cog_stat')
                    self.option('gene_string_table', self.output_dir + '/blast/gene_string.xls')
                    # string_venn_stat
                    if self.option('string_xml').is_set:
                        self.option('string_xml').get_info()
                        string_venn = self.option('string_xml').prop['hit_query_list']
                    else:
                        string_venn = self.option('string_table').prop['query_list']
                    string_gene_venn = self.option('gene_string_table').prop['query_list']
                    self.get_venn(venn_list=string_venn, output=self.output_dir + '/venn/string_venn.txt')
                    self.get_venn(venn_list=string_gene_venn, output=self.output_dir + '/venn/gene_string_venn.txt')
                    self.anno_list['string'] = string_venn
                    self.gene_anno_list['string'] = string_gene_venn
                    # cog_venn_stat
                    cog_venn = self.list_num(self.option("cog_list").prop["path"])
                    cog_gene_venn = self.list_num(self.work_dir + '/cog_stat/' + 'gene_cog_list.xls')
                    self.get_venn(venn_list=cog_venn, output=self.output_dir + '/venn/cog_venn.txt')
                    self.get_venn(venn_list=cog_gene_venn, output=self.output_dir + '/venn/gene_cog_venn.txt')
                    self.anno_list['cog'] = cog_venn
                    self.gene_anno_list['cog'] = cog_gene_venn
                if db == 'nr':
                    self.option('gene_nr_table', self.output_dir + '/blast/gene_nr.xls')
                    self.movedir2output(self.work_dir + '/blast_nr_statistics/', 'blast_nr_statistics')
                    # venn_stat
                    self.option('nr_xml').get_info()
                    nr_venn = self.option('nr_xml').prop['hit_query_list']
                    nr_gene_venn = self.option('gene_nr_table').prop['query_list']
                    self.get_venn(venn_list=nr_venn, output=self.output_dir + '/venn/nr_venn.txt')
                    self.get_venn(venn_list=nr_gene_venn, output=self.output_dir + '/venn/gene_nr_venn.txt')
                    self.anno_list['nr'] = nr_venn
                    self.gene_anno_list['nr'] = nr_gene_venn
                if db == 'kegg':
                    self.movedir2output(self.kegg_stat_path, 'kegg_stat')
                    if self.option("kegg_xml").is_set:
                        self.option('gene_kegg_table', self.output_dir + '/blast/gene_kegg.xls')
                        self.option('kegg_xml').get_info()
                        kegg_venn = self.option('kegg_xml').prop['hit_query_list']
                        kegg_gene_venn = self.option('gene_kegg_table').prop['query_list']
                    else:
                        self.option("kos_list_upload").get_transcript_anno(outdir=self.work_dir + "/kegg.list")
                        kegg_venn = self.list_num(self.work_dir + "/kegg.list")
                        kegg_gene_venn = self.list_num(self.work_dir + "/gene_kegg.list")
                    self.option('gene_kegg_anno_table', self.output_dir + '/kegg_stat/gene_kegg_table.xls')
                    self.get_venn(venn_list=kegg_venn, output=self.output_dir + '/venn/kegg_venn.txt')
                    self.get_venn(venn_list=kegg_gene_venn, output=self.output_dir + '/venn/gene_kegg_venn.txt')
                    self.anno_list['kegg'] = kegg_venn
                    self.gene_anno_list['kegg'] = kegg_gene_venn
                if db == 'pfam':
                    self.movedir2output(self.pfam_stat_path, 'pfam_stat')
                    pfam_venn = self.list_num(self.option('pfam_domain').prop['path'])
                    gene_pfam_venn = self.list_num(self.option('gene_pfam_domain').prop['path'])
                    self.get_venn(venn_list=pfam_venn, output=self.output_dir + '/venn/pfam_venn.txt')
                    self.get_venn(venn_list=gene_pfam_venn, output=self.output_dir + '/venn/gene_pfam_venn.txt')
                    self.anno_list['pfam'] = pfam_venn
                    self.gene_anno_list['pfam'] = gene_pfam_venn
                if db == 'go':
                    self.movedir2output(self.go_stat_path, 'go_stat')
                    files = os.listdir(self.work_dir)
                    for f in files:
                        if re.search(r'level_statistics\.xls$', f):
                            if os.path.exists(self.output_dir + '/go_stat/gene_{}'.format(f)):
                                os.remove(self.output_dir + '/go_stat/gene_{}'.format(f))
                            os.link(self.work_dir + '/' + f, self.output_dir + '/go_stat/gene_{}'.format(f))
                        if re.search(r'level.xls$', f):
                            if os.path.exists(self.output_dir + '/go_stat/gene_{}'.format(f)):
                                os.remove(self.output_dir + '/go_stat/gene_{}'.format(f))
                            os.link(self.work_dir + '/' + f, self.output_dir + '/go_stat/gene_{}'.format(f))
                    self.option('gene_go_level_2', self.output_dir + '/go_stat/gene_go12level_statistics.xls')
                    # venn_stat
                    go_venn = self.list_num(self.option("gos_list").prop["path"])
                    go_gene_venn = self.list_num(self.option("gene_go_list").prop["path"])
                    self.get_venn(venn_list=go_venn, output=self.output_dir + '/venn/go_venn.txt')
                    self.get_venn(venn_list=go_gene_venn, output=self.output_dir + '/venn/gene_go_venn.txt')
                    self.anno_list['go'] = go_venn
                    self.gene_anno_list['go'] = go_gene_venn
                if db == 'swissprot':
                    self.option('gene_swissprot_table', self.output_dir + '/blast/gene_swissprot.xls')
                    self.movedir2output(self.work_dir + '/blast_swissprot_statistics/', 'blast_swissprot_statistics')
                    # venn_stat
                    self.option('swissprot_xml').get_info()   #
                    swissprot_venn = self.option('swissprot_xml').prop['hit_query_list']
                    swissprot_gene_venn = self.option('gene_swissprot_table').prop['query_list']
                    self.get_venn(venn_list=swissprot_venn, output=self.output_dir + '/venn/swissprot_venn.txt')
                    self.get_venn(venn_list=swissprot_gene_venn, output=self.output_dir + '/venn/gene_swissprot_venn.txt')
                    self.anno_list['swissprot'] = swissprot_venn
                    self.gene_anno_list['swissprot'] = swissprot_gene_venn
        except Exception as e:
            print traceback.format_exc()
            self.set_error("设置注释统计分析结果目录失败{}".format(e))
            self.logger.info("设置注释统计分析结果目录失败{}".format(e))

    def movedir2output(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        self.logger.info(newdir)
        if os.path.exists(newdir):
            shutil.rmtree(newdir)
        os.mkdir(newdir)
        self.logger.info(newdir)
        allfiles = os.listdir(olddir)
        oldfiles = [os.path.join(olddir, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            else:
                newdir = os.path.join(newdir, os.path.basename(oldfiles[i]))
                if not os.path.exists(newdir):
                    os.mkdir(newdir)
                for f in os.listdir(oldfiles[i]):
                    old = os.path.join(oldfiles[i], f)
                    new = os.path.join(newdir, f)
                    if os.path.exists(new):
                        os.remove(new)
                    os.link(old, new)

    def run(self):
        super(RefAnnoStatTool, self).run()
        for db in self.database:
            if db == 'cog':
                self.run_cog_stat()
            if db == 'nr':
                self.run_nr_stat()
            if db == 'go':
                self.run_go_stat()
            if db == 'kegg':
                self.run_kegg_stat()
            if db == 'swissprot':
                self.run_swissprot_stat()
            if db == 'pfam':
                self.run_pfam_stat()
        if 'go' in self.database:
            self.wait()
            self.logger.info('end: go stat')
        self.set_output()
        self.get_all_anno_stat()
        self.end()

    def get_venn(self, venn_list, output):
        with open(output, 'wb') as w:
            for i in venn_list:
                w.write(i + '\n')

    def get_all_anno_stat(self):
        # stat all_annotation_statistics.xls
        all_anno_stat = self.output_dir + '/all_annotation_statistics.xls'
        anno_num = defaultdict(dict)
        with open(all_anno_stat, 'wb') as w:
            w.write('type\ttranscripts\tgenes\ttranscripts_percent\tgenes_percent\n')
            tmp = []
            tmp_gene = []
            anno_num['total']['gene'] = len(self.option('gene_file').prop['gene_list'])
            anno_num['total']['tran'] = len(self.tran_list)
            for db in self.anno_list:
                tran_db_percent = '%0.4g' % (len(self.anno_list[db]) / anno_num['total']['tran'])
                tran_db_percent = float(tran_db_percent) * 100
                gene_db_percent = '%0.4g' % (len(self.gene_anno_list[db]) / anno_num['total']['gene'])
                gene_db_percent = float(gene_db_percent) * 100
                w.write('{}\t{}\t{}\t{}\t{}\n'.format(db, len(self.anno_list[db]), len(self.gene_anno_list[db]),  str(tran_db_percent) + '%', str(gene_db_percent) + '%'))
                tmp += self.anno_list[db]
                tmp_gene += self.gene_anno_list[db]
            anno_num['total_anno']['gene'] = len(set(tmp_gene))
            anno_num['total_anno']['tran'] = len(set(tmp))
            tran_total_percent = '%0.4g' % (anno_num['total_anno']['tran'] / anno_num['total']['tran'])
            tran_total_percent = float(tran_total_percent) * 100
            gene_total_percent = '%0.4g' % (anno_num['total_anno']['gene'] / anno_num['total']['gene'])
            gene_total_percent = float(gene_total_percent) * 100
            w.write('total_anno\t{}\t{}\t{}\t{}\n'.format(anno_num['total_anno']['tran'], anno_num['total_anno']['gene'], str(tran_total_percent) + '%',  str(gene_total_percent) + '%'))
            w.write('total\t{}\t{}\t100%\t100%\n'.format(anno_num['total']['tran'], anno_num['total']['gene']))

    def list_num(self, list_file):
        with open(list_file, "rb") as f:
            lines = f.readlines()
            ids = []
            for line in lines:
                line = line.strip().split("\t")
                if line[0] not in ['Query_name', 'Seq_id']:
                    if line[0] not in ids:
                        ids.append(line[0])
        return ids
