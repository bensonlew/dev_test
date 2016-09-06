# -*- coding: utf-8 -*-
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
import Bio.SeqIO
import os


class DenovoAnnotationModule(Module):
    """
    module for denovorna annotation
    last modified:20160829
    author: wangbixuan
    """
    def __init__(self, work_id):
        super(DenovoAnnotationModule, self).__init__(work_id)
        options = [
            {"name": "query", "type": "infile", "format": "sequence.fasta"},
            {"name": "database", "type": "string", "default": 'nr,go,cog,kegg'},  # 默认全部四个注释
            {"name": "blast_evalue", "type": "float", "default": 1e-5},
            {"name": "blast_threads", "type": "int", "default": 10},
            {"name": "anno_statistics", "type": "bool", "default": True},
            {"name": "trans_gene", "type": "infile", "format": "sequence.fasta"}
        ]
        self.add_option(options)
        self.blast_nr = self.add_tool('align.ncbi.blast')
        self.blast_string = self.add_tool('align.ncbi.blast')
        self.blast_kegg = self.add_tool('align.ncbi.blast')
        self.blast_stat = self.add_tool('align.ncbi.blaststat')
        self.blast_gi_go = self.add_tool('align.ncbi.blast')
        self.ncbi_taxon = self.add_tool('taxon.ncbi_taxon')
        self.go_annot = self.add_tool('annotation.go_annotation')
        self.string_cog = self.add_tool('annotation.string2cog')
        self.kegg_annot = self.add_tool('annotation.kegg_annotation')
        self.anno_stat = self.add_tool('annot.denovorna_anno_statistics')
        self.step.add_steps('blast_nr', 'blast_string', 'blast_kegg', 'blast_statistics',
                            'go_annot', 'kegg_annot', 'cog_annot', 'taxon_annot', 'anno_stat')

    def check_options(self):
        if not self.option("query").is_set:
            raise OptionError("必须设置参数query")
        else:
            if self.option('query').prop['seq_type'] != 'DNA':
                raise OptionError('传入查询序列必须是核酸序列')
        anno_database = self.option('database').split(',')
        for i in anno_database:
            if i not in ['nr', 'go', 'cog', 'kegg']:
                raise OptionError('需要注释的数据库不在支持范围内[\'nr\', \'go\', \'cog\', \'kegg\']:{}'.format(i))
        elif self.option("database").lower() not in ["nt", "nr", "string", 'kegg', 'swissprot']:
            raise OptionError("数据库%s不被支持" % self.option("database"))
        else:
            self.option('reference_type', self._database_type[
                        self.option("database").lower()])
        if not 1 > self.option('evalue') >= 0:
            raise OptionError(
                'E-value值设定必须为[0-1)之间：{}'.format(self.option('evalue')))
        if self.option('unigene') == True:
            if self.option('query_gene').is_set:
                seqid = []
                f = open(self.option('query_gene').prop['path'])
                for seq_record in Bio.SeqIO.parse(f, 'fasta'):
                    seqid.append(str(seq_record.id))
                f.close()
                for item in seqid:
                    if not item.startswith('TRINITY'):
                        raise OptionError("输入文件不是Trinity标准结果文件")
                        break
#            else:
#                raise OptionError("Unigene文件不存在")

#    def blast_run(self):
#        if self.option('reference').is_set:
#            self.dbtype = self.option('reference_type')
#        else:
#            self.dbtype = self._database_type[self.option('database')]
#        self.checktype = {'nucl': {'nucl': 'blastn', 'prot': 'blastx'}, 'prot': {
#            'nucl': 'tblastn', 'prot': 'blastp'}}
#        self.blast.set_options({
#            'query': self.option('query'),
#            'query_type': self.option('query_type'),
#            'database': self.option('database'),
#            'outfmt': 6,
#            'blast': self.checktype[self.option('query_type')][self.dbtype],
#            'reference': self.option('reference'),
#            'reference_type': self.option('reference_type'),
#            'evalue': self.option('evalue'),
#            'num_threads': self.option('threads'),
#            'num_alignment': 500
#        })
#        self.step.blast.start()
#        self.blast.on("end", self.set_output, 'blast')
#        self.blast.run()

    def blast_gi_go_run(self):
        self.blast_gi_go.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': 'nr',
            'outfmt': 6,
            'blast': self._blast_type[self.option('query_type')]['prot'][0],
            'evalue': self.option('nrevalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_gi_go.start()
        self.blast_gi_go.on("end", self.set_output, 'nrblast')
        self.blast_gi_go.run()

    def blast_stat_run(self):
        #        if self.option('database') == 'nr':
        #            blastfile = self.blast.option('outxml')
        #        else:
        blastfile = self.blast_gi_go.option('outxml')
        self.blast_stat.set_options({
            'in_stat': blastfile
        })
        self.step.blast_stat.start()
        self.blast_stat.on("end", self.set_output, 'blast_stat')
        self.blast_stat.run()

    def ncbi_taxon_run(self):
        #        if self.option('database') == 'nr':
        #            blastfile = self.blast.option('outxml')
        #        else:
        blastfile = self.blast_gi_go.option('outxml')
        self.ncbi_taxon.set_options({
            'blastout': blastfile,
            'blastdb': 'nr'
        })
        self.step.ncbi_taxon.start()
        self.ncbi_taxon.on("end", self.set_output, 'ncbi_taxon')
        self.ncbi_taxon.run()

    def go_annot_run(self):
        #        if self.option('database') == 'nr':
        #            blastfile = self.blast.option('outxml')
        #        else:
        blastfile = self.blast_gi_go.option('outxml')
        self.go_annot.set_options({
            'blastout': blastfile
        })
        self.step.go_annot.start()
        self.go_annot.on("end", self.set_output, 'go_annot')
        self.go_annot.run()

    def blast_string_run(self):
        self.blast_string.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': 'string',
            'outfmt': 6,
            'blast': self._blast_type[self.option('query_type')]['prot'][0],
            'evalue': self.option('stringevalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_string.start()
        self.blast_string.on("end", self.set_output, 'stringblast')
        self.blast_string.run()

    def string_cog_run(self):
        #        if self.option('database') == 'string':
        #            blastfile = self.blast.option('outxml')
        #        else:
        blastfile = self.blast_string.option('outxml')
        self.string_cog.set_options({
            'blastout': blastfile,
        })
        self.string_cog.start()
        self.string_cog.on("end", self.set_output, 'string_cog')
        self.string_cog.run()

    def blast_kegg_run(self):
        self.blast_kegg.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': 'kegg',
            'outfmt': 6,
            'blast': self._blast_type[self.option('query_type')]['prot'][0],
            'evalue': self.option('keggevalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_kegg.start()
        self.blast_kegg.on("end", self.set_output, 'keggblast')
        self.blast_kegg.run()

    def kegg_annot_run(self):
        #        if self.option('database') == 'kegg':
        #            blastfile = self.blast.option('outxml')
        #        else:
        blastfile = self.blast_kegg.option('outxml')
        self.kegg_annot.set_options({
            'blastout': blastfile
        })
        self.kegg_annot.start()
        self.kegg_annot.on("end", self.set_output, 'kegg_annot')
        self.kegg_annot.run()

    def blast_swiss_run(self):
        self.blast_swiss.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': 'swissprot',
            'outfmt': 6,
            'blast': self._blast_type[self.option('query_type')]['prot'][0],
            'evalue': self.option('swissevalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_kegg.start()
        self.blast_swiss.on("end", self.set_output, 'swissblast')
        self.blast_kegg.run()

    def anno_stat_run(self):
        if self.option('nrblast') or self.option('blast_stat') or self.option('gi_taxon') or self.option('go_annot'):
            nrblast = self.blast_gi_go.option('outxml')
        else:
            nrblast = '0'
        if self.option('swissblast'):
            swissblast = self.blast_swiss.option('outxml')
        else:
            swissblast = '0'
        if self.option('stringblast') or self.option('cog_annot'):
            stringblast = self.blast_string.option('outxml')
        else:
            stringblast = '0'
        if self.option('keggblast') or self.option('kegg_annot'):
            keggblast = self.blast_kegg.option('outxml')
        else:
            keggblast = '0'
        if self.option('gi_taxon'):
            ncbitaxon = self.ncbi_taxon.output_dir
        else:
            ncbitaxon == '0'
        if self.option('go_annot'):
            godir = self.go_annot.output_dir
        else:
            godir = '0'
        if self.option('cog_annot'):
            cogdir = self.string_cog.output_dir
        else:
            cogdir = '0'
        if self.option('kegg_annot'):
            keggdir = self.kegg_annot.output_dir
        else:
            keggdir = '0'
        if self.option('blast_stat'):
            bstatdir = self.blast_stat.output_dir
        else:
            bstatdir = '0'
        self.anno_stat.set_options({
            'trinity_fasta': self.option('query'),
            'gene_fasta': self.option('query_gene'),
            'nr_blast_output': nrblast,
            'swiss_blast_out': swissblast,
            'string_blast_out': stringblast,
            'kegg_blast_out': keggblast,
            'ncbi_taxonomy_output_dir': ncbitaxon,
            'go_output_dir': godir,
            'cog_output_dir': cogdir,
            'kegg_output_dir': keggdir,
            'blast_stat_output_dir': bstatdir,
            'unigene': self.option('unigene')
        })
        self.anno_stat.start()
        self.anno_stat.on("end", self.set_output, 'anno_stat')

    def run(self):
        super(DenovoAnnotationModule, self).run()
        # self.blast_run()
        # self.step.update()
        #self.on_rely(self.blast, self.blast_stat_run)
        s = set()
        '''run blast'''
        if self.option('nrblast'):
            s.add(self.blast_gi_go)
            self.blast_gi_go_run()
            self.step.update()
        if self.option('stringblast'):
            s.add(self.blast_string)
            self.blast_string_run()
            self.step.update()
        if self.option('keggblast'):
            s.add(self.blast_kegg)
            self.blast_kegg_run()
            self.step.update()
        if self.option('swissblast'):
            s.add(self.blast_swiss)
            self.blast_swiss_run()
            self.step.update()
        ''' run annotation '''
        if self.option('blast_stat'):
            if self.option('nrblast'):
                s.add(self.blast_stat)
                self.on(self.blast_gi_go, self.blast_stat_run)
                self.step.update()
            else:
                s.add(self.blast_gi_go)
                s.add(self.blast_stat)
                self.blast_gi_go_run()
                self.step.update()
                self.on(self.blast_gi_go, self.blast_stat_run)
                self.step.update()
        if self.option('gi_taxon'):
            s.add(self.blast_gi_go)
            s.add(self.ncbi_taxon)
            if self.option('nrblast'):
                self.on(self.blast_gi_go, self.ncbi_taxon_run)
                self.step.update()
            else:
                self.blast_gi_go_run()
                self.step.update()
                self.on(self.blast_gi_go, self.ncbi_taxon_run)
                self.step.update()
        if self.option('go_annot'):
            s.add(self.blast_gi_go)
            s.add(self.go_annot)
            if self.option('nrblast'):
                self.on(self.blast_gi_go, self.go_annot_run)
                self.step.update()
            else:
                self.blast_gi_go_run()
                self.step.update()
                self.on(self.blast_gi_go, self.go_annot_run)
                self.step.update()
        if self.option('cog_annot'):
            s.add(self.blast_string)
            s.add(self.string_cog)
            if self.option('stringblast'):
                self.on(self.blast_string, self.string_cog_run)
                self.step.update()
            else:
                self.blast_string_run()
                self.step.update()
                self.on(self.blast_string, self.string_cog_run)
                self.step.update()
        if self.option('kegg_annot'):
            s.add(self.blast_kegg)
            s.add(self.kegg_annot)
            if self.option('keggblast'):
                self.on(self.blast_kegg, self.kegg_annot_run)
                self.step.update()
            else:
                self.blast_kegg_run()
                self.step.update()
                self.on(self.blast_kegg, self.kegg_annot_run)
                self.step.update()
        l = list(s)
        if self.option('anno_statistics'):
            self.on_rely(l, self.anno_stat)
            self.on(self.anno_stat, self.end)
        else:
            self.on_rely(l, self.end)

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'nrblast':
            self.linkdir(obj.output_dir, 'nrblast')
            self.step.blast_gi_go.finish()
        elif event['data'] == 'stringblast':
            self.linkdir(obj.output_dir, 'stringblast')
            self.step.blast_string.finish()
        elif event['data'] == 'keggblast':
            self.linkdir(obj.output_dir, 'keggblast')
            self.step.blast_kegg.finish()
        elif event['data'] == 'swissblast':
            self.linkdir(obj.output_dir, 'swissblast')
            self.step.blast_swiss.finish()
        elif event['data'] == 'blast_stat':
            self.linkdir(obj.output_dir, 'blast_nr_statistics')
            self.step.blast_stat.finish()
        elif event['data'] == 'ncbi_taxon':
            self.linkdir(obj.output_dir, 'ncbi_taxonomy')
            self.step.ncbi_taxon.finish()
        elif event['data'] == 'go_annot':
            self.linkdir(obj.output_dir, 'go')
            self.step.go_annot.finish()

        elif event['data'] == 'string_cog':
            self.linkdir(obj.output_dir, 'cog')
            self.step.string_cog.finish()
        elif event['data'] == 'kegg_annot':
            self.linkdir(obj.output_dir, 'kegg')
            self.step.kegg_annot.finish()
        elif event['data'] == 'anno_stat':
            self.linkdir(obj.output_dir, 'anno_stat')
        else:
            pass

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def stepend(self):
        self.step.update()
        self.end()

    def end(self):
        repaths = [
            [".", "", "DENOVO_RNA结果文件目录"]，
            ['ncbi_taxonomy/query_taxons_detail.xls', 'xls', '序列详细物种分类文件'],
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
            ["anno_stat/all_annotation.xls", "xls", "综合注释表"],
            ["anno_stat/all_annotation_statistics.xls", "xls", "综合注释统计表"],
            ["anno_stat/venn_table.xls", "xls", "文氏图参考表"],
            ["anno_stat/unigene/blast/unigene_nr.xml", "xml", "nr_blast_xml"],
            ["anno_stat/unigene/blast/unigene_nr.xls", "xls", "nr_blast_xls"],
            ["anno_stat/unigene/blast/unigene_string.xml", "xml", "string_blast_xml"],
            ["anno_stat/unigene/blast/unigene_string.xls", "xls", "string_blast_xls"],
            ["anno_stat/unigene/blast/unigene_kegg.xml", "xml", "kegg_blast_xml"],
            ["anno_stat/unigene/blast/unigene_kegg.xls", "xls", "kegg_blast_xls"],
            ["anno_stat/unigene/blast_nr_statistics/unigene_evalue_statistics.xls",
                "xls", "nr_blast_evalue统计表"],
            ["anno_stat/unigene/blast_nr_statistics/unigene_similarity_statistics.xls",
                "xls", "nr_blast_similarity统计表"],
            ["anno_stat/unigene/ncbi_taxonomy/unigene_query_taxons_detail.xls",
                "xls", "物种分类统计表"],
            [".anno_stat/unigene/go/unigene_blast2go.annot",
                "annot", "unigene blast2go结果"],
            [".anno_stat/unigene/go/unigene_query_gos.list", "list", "unigene GO列表"],
            [".anno_stat/unigene/go/unigene_go1234level_statistics.xls", "xls", "GO逐层统计表"],
            [".anno_stat/unigene/go/unigene_go2level.xls", "xls", "GO level2统计表"],
            [".anno_stat/unigene/go/unigene_go3level.xls", "xls", "GO level3统计表"],
            [".anno_stat/unigene/go/unigene_go4level.xls", "xls", "GO level4统计表"],
            [".anno_stat/unigene/cog/unigene_cog_list.xls", "xls", "unigene COG id表"],
            [".anno_stat/unigene/cog/unigene_cog_summary.xls",
                "xls", "unigene COG功能分类统计"],
            [".anno_stat/unigene/cog/unigene_cog_table.xls",
                "xls", "unigene COG综合统计表"],
            [".anno_stat/unigene/kegg/unigene_kegg_table.xls",
                "xls", "unigene KEGG ID表"],
            [".anno_stat/unigene/kegg/unigene_pathway_table.xls",
                "xls", "unigene KEGG pathway表"],
            [".anno_stat/unigene/kegg/unigene_kegg_taxonomy.xls",
                "xls", "unigene KEGG 二级分类统计表"]
        ]
        regexps = [
            [r"blast/.+_vs_.+\.xml", "xml", "blast比对输出结果，xml格式"],
            [r"blast/.+_vs_.+\.xls", "xls", "blast比对输出结果，表格(制表符分隔)格式"],
            [r"blast/.+_vs_.+\.txt", "txt", "blast比对输出结果，非xml和表格(制表符分隔)格式"],
            [r"blast/.+_vs_.+\.txt_\d+\.xml", "xml",
                "Blast比对输出多xml结果，输出格式为14的单个比对结果文件,主结果文件在txt文件中"],
            [r"blast/.+_vs_.+\.txt_\d+\.json", "json",
                "Blast比输出对多json结果，输出格式为13的单个比对结果文件,主结果文件在txt文件中"],
            [r"kegg/pathways/ko.\d+", 'pdf', '标红pathway图'],
            [r"/blast_nr_statistics/.*_evalue\.xls", "xls", "比对结果E-value分布图"],
            [r"/blast_nr_statistics/.*_similar\.xls", "xls", "比对结果相似度分布图"]
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(DenovoAnnotationModule, self).end()
