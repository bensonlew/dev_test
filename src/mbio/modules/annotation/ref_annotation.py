# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from __future__ import division
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.packages.annotation.ref_annotation_query import AllAnnoStat
import os
import shutil


class RefAnnotationModule(Module):
    """
    module for denovorna annotation
    """
    def __init__(self, work_id):
        super(RefAnnotationModule, self).__init__(work_id)
        options = [
            {"name": "blast_nr_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "blast_string_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "blast_kegg_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "blast_swissprot_xml", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "blast_nr_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "blast_string_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "blast_kegg_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "blast_swissprot_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "pfam_domain", "type": "infile", "format": "annotation.kegg.kegg_list"},
            {"name": "gos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},   # 客户上传go注释文件
            {"name": "kos_list_upload", "type": "infile", "format": "annotation.upload.anno_upload"},  # 客户上传kegg注释文件
            {"name": "gene_file", "type": "infile", "format": "rna.gene_list"},
            {"name": "ref_genome_gtf", "type": "infile", "format": "gene_structure.gtf"},  # 参考基因组gtf文件/新基因gtf文件，功能:将参考基因组转录本ID替换成gene ID
            {"name": "anno_statistics", "type": "bool", "default": True},
            {"name": "go_annot", "type": "bool", "default": True},
            {"name": "nr_annot", "type": "bool", "default": False},  # 参考基因组注释不提供缺少nr_xml文件，因此将默认值改为False
            {"name": "taxonomy", "type": "string", "default": None},   # kegg数据库物种分类, Animals/Plants/Fungi/Protists/Archaea/Bacteria
            {"name": "gene_go_list", "type": "outfile", "format": "annotation.go.go_list"},
            {"name": "gene_kegg_table", "type": "outfile", "format": "annotation.kegg.kegg_table"},
            {"name": "gene_go_level_2", "type": "outfile", "format": "annotation.go.level2"},
        ]
        self.add_option(options)
        self.swissprot_annot = self.add_tool('align.xml2table')
        self.nr_annot = self.add_tool('align.xml2table')
        self.go_annot = self.add_tool('annotation.go.go_annotation')
        self.go_upload = self.add_tool('annotation.go.go_upload')
        self.string_cog = self.add_tool('annotation.cog.string2cogv9')
        self.kegg_annot = self.add_tool('annotation.kegg.kegg_annotation')
        self.swissprot_annot = self.add_tool("annotation.swissprot")
        self.kegg_upload = self.add_tool('annotation.kegg.kegg_upload')
        self.anno_stat = self.add_tool('rna.ref_anno_stat')
        self.step.add_steps('blast_statistics', 'nr_annot', 'go_annot', 'go_upload', 'kegg_annot', 'kegg_upload', 'cog_annot', 'anno_stat', 'swissprot_annot')

    def check_options(self):
        if self.option('anno_statistics'):
            if not self.option('gene_file').is_set:
                raise OptionError('运行注释统计的tool必须要设置gene_file')
            if not self.option('ref_genome_gtf').is_set:
                raise OptionError('缺少gtf文件')

    def set_step(self, event):
        if 'start' in event['data']:
            event['data']['start'].start()
        if 'end' in event['data']:
            event['data']['end'].finish()
        self.step.update()

    def run_annot_stat(self):
        """
        """
        opts = {'gene_file': self.option('gene_file'), 'database': ','.join(self.anno_database)}
        if 'kegg' in self.anno_database:
            opts['kegg_xml'] = self.option('blast_kegg_xml')
            opts['kos_list_upload'] = self.option('kos_list_upload')
        if 'go' in self.anno_database:
            opts['gos_list'] = self.go_annot.option('golist_out')
            opts['blast2go_annot'] = self.go_annot.option('blast2go_annot')
            opts['gos_list_upload'] = self.option('gos_list_upload')
        if 'cog' in self.anno_database:
            opts['string_xml'] = self.option('blast_string_xml')
            opts['string_table'] = self.option('blast_string_table')
            opts['cog_list'] = self.string_cog.option('cog_list')
            opts['cog_table'] = self.string_cog.option('cog_table')
        if 'nr' in self.anno_database:
            opts['nr_xml'] = self.option('blast_nr_xml')
        opts['swissprot_xml'] = self.option('blast_swissprot_xml')
        opts['pfam_domain'] = self.option('pfam_domain')
        opts['ref_genome_gtf'] = self.option('ref_genome_gtf')
        self.anno_stat.set_options(opts)
        self.anno_stat.on('start', self.set_step, {'start': self.step.anno_stat})
        self.anno_stat.on('end', self.set_step, {'end': self.step.anno_stat})
        self.anno_stat.on('end', self.set_output, 'anno_stat')
        self.anno_stat.run()

    def run_kegg_anno(self):
        """
        """
        options = {
            'blastout': self.option('blast_kegg_xml'),
            'taxonomy': self.option('taxonomy')
        }
        self.kegg_annot.set_options(options)
        self.kegg_annot.on('start', self.set_step, {'start': self.step.kegg_annot})
        self.kegg_annot.on('end', self.set_step, {'end': self.step.kegg_annot})
        self.kegg_annot.on('end', self.set_output, 'kegg_annot')
        self.kegg_annot.run()

    def run_kegg_upload(self):
        options = {
            'kos_list_upload': self.option('kos_list_upload'),
            'taxonomy': self.option('taxonomy')
        }
        self.kegg_upload.set_options(options)
        self.kegg_upload.on('start', self.set_step, {'start': self.step.kegg_upload})
        self.kegg_upload.on('end', self.set_step, {'end': self.step.kegg_upload})
        self.kegg_upload.on('end', self.set_output, 'kegg_annot')
        self.kegg_upload.run()

    def run_string2cog(self):
        options = {
            'blastout': self.option('blast_string_xml'),
            'string_table': self.option('blast_string_table')
        }
        self.string_cog.set_options(options)
        self.string_cog.on('start', self.set_step, {'start': self.step.cog_annot})
        self.string_cog.on('end', self.set_step, {'end': self.step.cog_annot})
        self.string_cog.on('end', self.set_output, 'string_cog')
        self.string_cog.run()

    def run_go_anno(self):
        """
        """
        options = {
            'blastout': self.option('blast_nr_xml')
        }
        self.go_annot.set_options(options)
        self.go_annot.on('start', self.set_step, {'start': self.step.go_annot})
        self.go_annot.on('end', self.set_step, {'end': self.step.go_annot})
        self.go_annot.on('end', self.set_output, 'go_annot')
        self.go_annot.run()

    def run_go_upload(self):
        options = {
            'gos_list_upload': self.option('gos_list_upload')
        }
        self.go_upload.set_options(options)
        self.go_upload.on('start', self.set_step, {'start': self.step.go_upload})
        self.go_upload.on('end', self.set_step, {'end': self.step.go_upload})
        self.go_upload.on('end', self.set_output, 'go_annot')
        self.go_upload.run()

    def run_swissprot_anno(self):
        options = {
            'blastout': self.option('blast_swissprot_xml')
        }
        self.swissprot_annot.set_options(options)
        self.swissprot_annot.on('start', self.set_step, {'start': self.step.swissprot_annot})
        self.swissprot_annot.on('end', self.set_step, {'end': self.step.swissprot_annot})
        self.swissprot_annot.on('end', self.set_output, 'swissprot_annot')
        self.swissprot_annot.run()

    def run_nr_anno(self):
        options = {
            'blastout': self.option('blast_swissprot_xml')
        }
        self.nr_annot.set_options(options)
        self.nr_annot.on('start', self.set_step, {'start': self.step.nr_annot})
        self.nr_annot.on('end', self.set_step, {'end': self.step.nr_annot})
        self.nr_annot.on('end', self.set_output, 'nr_annot')
        self.nr_annot.run()

    def run(self):
        super(RefAnnotationModule, self).run()
        self.all_end_tool = []  # 所有尾部注释模块，全部结束后运行整体统计
        self.anno_database = []
        # if self.option('nr_annot'):
        #     self.anno_database.append('nr')
        #     self.run_nr_anno()
        if self.option('blast_nr_xml').is_set:
            self.anno_database.append('nr')
            self.all_end_tool.append(self.nr_annot)
            self.run_nr_anno()
        if self.option('go_annot'):
            self.anno_database.append('go')
            if self.option("gos_list_upload").is_set:
                self.all_end_tool.append(self.go_upload)
                self.run_go_upload()
            else:
                self.all_end_tool.append(self.go_annot)
                self.run_go_anno()
        if self.option('blast_string_xml').is_set or self.option('blast_string_table').is_set:
            self.anno_database.append('cog')
            self.all_end_tool.append(self.string_cog)
            self.run_string2cog()
        if self.option('blast_kegg_xml').is_set:
            self.anno_database.append('kegg')
            self.all_end_tool.append(self.kegg_annot)
            self.run_kegg_anno()
        if self.option('kos_list_upload').is_set:
            self.anno_database.append('kegg')
            self.all_end_tool.append(self.kegg_upload)
            self.run_kegg_upload()
        if self.option("blast_swissprot_xml").is_set:
            self.anno_database.append('swissprot')
            self.run_swissprot_anno()
        if self.option("pfam_domain").is_set:
            self.anno_database.append('pfam')
        if len(self.all_end_tool) > 1:
            self.on_rely(self.all_end_tool, self.run_annot_stat)
        elif len(self.all_end_tool) == 1:
            self.all_end_tool[0].on('end', self.run_annot_stat)
        else:
            raise Exception('不需要进行任何注释工作')
            self.logger.info('NEVER HERE')

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'blast_stat':
            self.linkdir(obj.output_dir, 'blast_nr_statistics')
        elif event['data'] == 'go_annot':
            self.linkdir(obj.output_dir, 'go')
        elif event['data'] == 'string_cog':
            self.linkdir(obj.output_dir, 'cog')
        elif event['data'] == 'kegg_annot':
            self.linkdir(obj.output_dir, 'kegg')
        elif event['data'] == 'anno_stat':
            self.linkdir(obj.output_dir, 'anno_stat')
            if 'kegg' in self.anno_database:
                self.option('gene_kegg_table', obj.option('gene_kegg_anno_table').prop['path'])
            if 'go' in self.anno_database:
                self.option('gene_go_list', obj.option('gene_go_list').prop['path'])
                self.option('gene_go_level_2', obj.option('gene_go_level_2').prop['path'])
            try:
                self.logger.info("进行注释查询的统计")
                self.get_all_anno_stat(self.output_dir + '/anno_stat/all_annotation.xls')
            except Exception as e:
                self.logger.info("统计all_annotation出错：{}".format(e))
            self.end()
        else:
            pass

    def get_all_anno_stat(self, all_anno_path):
        # stat all_annotation.xls
        kwargs = {'outpath': all_anno_path, 'gtf_path': self.option('ref_genome_gtf').prop['path']}
        for db in self.anno_database:
            if db == 'cog':
                kwargs['cog_list'] = self.string_cog.option('cog_list').prop['path']
            if db == 'go':
                kwargs['gos_list'] = self.go_annot.option('golist_out').prop['path']
            if db == 'kegg':
                kwargs['kegg_table'] = self.kegg_annot.option('kegg_table').prop['path']
            if db == 'nr':
                kwargs['blast_nr_table'] = self.nr_annot.option('blast_table').prop['path']
            if db == 'swissprot':
                kwargs['blast_swissprot_table'] = self.swissprot_annot.option('blast_table').prop['path']
            if db == 'pfam':
                kwargs['pfam_domain'] = self.option('pfam_domain').prop['path']
        allstat = AllAnnoStat()
        allstat.get_anno_stat(**kwargs)

    def linkdir(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            if mode == 'link':
                shutil.copytree(olddir, newdir, symlinks=True)
            elif mode == 'copy':
                shutil.copytree(olddir, newdir)
            else:
                raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')
        else:
            allfiles = os.listdir(olddir)
            oldfiles = [os.path.join(olddir, i) for i in allfiles]
            newfiles = [os.path.join(newdir, i) for i in allfiles]
            for newfile in newfiles:
                if os.path.isfile(newfile) and os.path.exists(newfile):
                    os.remove(newfile)
                elif os.path.isdir(newfile) and os.path.exists(newfile):
                    shutil.rmtree(newfile)
            for i in range(len(allfiles)):
                if os.path.isfile(oldfiles[i]):
                    os.system('cp {} {}'.format(oldfiles[i], newfiles[i]))
                else:
                    os.system('cp -r {} {}'.format(oldfiles[i], newdir))

    def end(self):
        repaths = [
            [".", "", "DENOVO_RNA结果文件目录"],
            ["blast_nr_statistics/output_evalue.xls", "xls", "blast结果E-value统计"],
            ["blast_nr_statistics/output_similar.xls", "xls", "blast结果similarity统计"],
            ["kegg/kegg_table.xls", "xls", "KEGG annotation table"],
            ["kegg/pathway_table.xls", "xls", "Sorted pathway table"],
            ["kegg/kegg_taxonomy.xls", "xls", "KEGG taxonomy summary"],
            ["go/blast2go.annot", "annot", "Go annotation based on blast output"],
            ["go/query_gos.list", "list", "Merged Go annotation"],
            ["go/go1234level_statistics.xls", "xls", "Go annotation on 4 levels"],
            ["go/go2level.xls", "xls", "Go annotation on level 2"],
            ["go/go3level.xls", "xls", "Go annotation on level 3"],
            ["go/go4level.xls", "xls", "Go annotation on level 4"],
            ["cog/cog_list.xls", "xls", "COG编号表"],
            ["cog/cog_summary.xls", "xls", "COG注释二级统计表"],
            ["cog/cog_table.xls", "xls", "序列COG注释详细表"],
            ["/anno_stat", "", "denovo注释统计结果输出目录"],
            ["/anno_stat/cog_stat/", "dir", "cog统计结果目录"],
            ["/anno_stat/go_stat/", "dir", "go统计结果目录"],
            ["/anno_stat/kegg_stat/", "dir", "kegg统计结果目录"],
            ["/anno_stat/blast/", "dir", "基因序列blast比对结果目录"],
            ["/anno_stat/blast_nr_statistics/", "dir", "基因序列blast比对nr库统计结果目录"],
            ["/anno_stat/blast/gene_kegg.xls", "xls", "基因序列blast比对kegg注释结果table"],
            ["/anno_stat/blast/gene_nr.xls", "xls", "基因序列blast比对nr注释结果table"],
            ["/anno_stat/blast/gene_nr.xls", "xls", "基因序列blast比对nr注释结果table"],
            ["/anno_stat/blast/gene_swissprot.xls", "xls", "基因序列blast比对到swissprot注释结果table"],
            ["/anno_stat/blast/gene_string.xml", "xml", "基因序列blast比对string注释结果xml"],
            ["/anno_stat/blast/gene_kegg.xml", "xml", "基因序列blast比对kegg注释结果xml"],
            ["/anno_stat/blast/gene_string.xml", "xml", "基因序列blast比对string注释结果xml"],
            ["/anno_stat/blast/gene_swissprot.xlm", "xml", "基因序列blast比对到swissprot注释结果xml"],
            ["/anno_stat/cog_stat/gene_cog_list.xls", "xls", "基因序列cog_list统计结果"],
            ["/anno_stat/cog_stat/gene_cog_summary.xls", "xls", "基因序列cog_summary统计结果"],
            ["/anno_stat/cog_stat/gene_cog_table.xls", "xls", "基因序列cog_table统计结果"],
            ["/anno_stat/cog_stat/gene_cog_table.xls", "xls", "基因序列cog_table统计结果"],
            ["/anno_stat/cog_stat/gene_cog_table.xls", "xls", "基因序列cog_table统计结果"],
            ["/anno_stat/cog_stat/gene_cog_table.xls", "xls", "基因序列cog_table统计结果"],
            ["/anno_stat/go_stat/gene_blast2go.annot", "annot", "Go annotation based on blast output of gene"],
            ["/anno_stat/go_stat/gene_gos.list", "list", "Merged Go annotation of gene"],
            ["/anno_stat/go_stat/gene_go1234level_statistics.xls", "xls", "Go annotation on 4 levels of gene"],
            ["/anno_stat/go_stat/gene_go2level.xls", "xls", "Go annotation on level 2 of gene"],
            ["/anno_stat/go_stat/gene_go3level.xls", "xls", "Go annotation on level 3 of gene"],
            ["/anno_stat/go_stat/gene_go4level.xls", "xls", "Go annotation on level 4 of gene"],
            ["/anno_stat/kegg_stat/gene_kegg_table.xls", "xls", "KEGG annotation table of gene"],
            ["/anno_stat/kegg_stat/gene_pathway_table.xls", "xls", "Sorted pathway table of gene"],
            ["/anno_stat/kegg_stat/gene_kegg_taxonomy.xls", "xls", "KEGG taxonomy summary of gene"],
            ["/anno_stat/kegg_stat/gene_kegg_layer.xls", "xls", "KEGG taxonomy summary of gene"],
            ["/anno_stat/kegg_stat/gene_pathway/", "dir", "基因的标红pathway图"],
            ["/anno_stat/blast_nr_statistics/gene_nr_evalue.xls", "xls", "基因序列blast结果E-value统计"],
            ["/anno_stat/blast_nr_statistics/gene_nr_similar.xls", "xls", "基因序列blast结果similarity统计"],
            ["/anno_stat/all_annotation_statistics.xls", "xls", "注释统计总览表"],
            ["/anno_stat/all_annotation.xls", "xls", "注释统计表"],
        ]
        regexps = [
            [r"kegg/pathways/ko.\d+", 'pdf', '标红pathway图'],
            [r"/blast_nr_statistics/.*_evalue\.xls", "xls", "比对结果E-value分布图"],
            [r"/blast_nr_statistics/.*_similar\.xls", "xls", "比对结果相似度分布图"],
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(RefAnnotationModule, self).end()
