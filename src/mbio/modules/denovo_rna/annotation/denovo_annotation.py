# -*- coding: utf-8 -*-
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
import Bio.SeqIO
import os


class DenovoAnnotationModule(Module):
    """
    module for denovorna annotation
    last modified:20160808
    author: wangbixuan
    """

    def __init__(self, work_id):
        super(DenovoAnnotationModule, self).__init__(work_id)
        self._fasta_type = {'Protein': 'prot', 'DNA': 'nucl'}
        self._blast_type = {'nucl': {'nucl': ['blastn', 'tblastn'],
                                     'prot': ['blastx']},
                            'prot': {'nucl': [],
                                     'prot': ['blastp']}}
        self._database_type = {'nt': 'nucl', 'nr': 'prot',
                               'kegg': 'prot', 'swissprot': 'prot', 'string': 'prot'}
        options = [
            {"name": "query", "type": "infile", "format": "sequence.fasta"},
            {"name": "query_type", "type": "string"},
            {"name": "database", "type": "string", "default": "nr"},
            {"name": "reference", "type": "infile",
                "format": "sequence.fasta"},  # 参考序列，选择customer时启动
            {"name": "reference_type", "type": "string"},  # 参考序列类型，nucl或prot
            {"name": "evalue", "type": "float", "default": 1e-5},
            {"name": "threads", "type": "int", "default"：10}，
            {"name": "anno_statistics", "type": "bool", "default": False},
            {"name": "unigene", "type": "bool", "default": False}
            {"name": "query_gene", "type": "infile", "format": "sequence.fasta"}
        ]
        self.blast = self.add_tool('align.ncbi.blast')
        self.blast_stat = self.add_tool('align.ncbi.blaststat')
        self.blast_gi_go = self.add_tool('align.ncbi.blast')  # blast nr/nt
        self.ncbi_taxon = self.add_tool('taxon.ncbi_taxon')
        self.go_annot = self.add_tool('annotation.go_annotation')
        self.blast_string = self.add_tool('align.ncbi.blast')  # blast string
        self.string_cog = self.add_tool('annotation.string2cog')
        self.blast_kegg = self.add_tool('align.ncbi.blast')  # blast kegg
        self.kegg_annot = self.add_tool('annotation.kegg_annotation')
        self.blast_swiss = self.add_tool('align.ncbi.blast')  # blast swiss
        self.anno_stat = self.add_tool('annot.denovorna_anno_statistics')
        self.add_option(options)
        self.step_add_steps('blast', 'blast_stat', 'blast_gi_go', 'ncbi_taxon', 'blast_swiss',
                            'go_annot', 'blast_string', 'blast_kegg', 'kegg_annot', 'anno_stat', 'string_cog')

    def check_options(self):
        if not self.option("query").is_set:
            raise OptionError("必须设置参数query")
        if self.option('query_type') not in ['nucl', 'prot']:
            raise OptionError('query_type查询序列的类型为nucl(核酸)或者prot(蛋白):{}'.format(
                self.option('query_type')))
        else:
            if self._fasta_type[self.option('query').prop['seq_type']] != self.option('query_type'):
                raise OptionError(
                    '文件检查发现查询序列为:{}, 而说明的文件类型为:{}'.format(
                        self._fasta_type[self.option('query').prop['seq_type'], self.option('query_type')]))
        if self.option("database") == 'customer_mode':
            if not self.option("reference").is_set:
                raise OptionError("使用自定义数据库模式时必须设置reference")
            if self.option('reference_type') not in ['nucl', 'prot']:
                raise OptionError('reference_type参考序列的类型为nucl(核酸)或者prot(蛋白):{}'.format(
                    self.option('query_type')))
            else:
                if self._fasta_type[self.option('reference').prop['seq_type']] != self.option('reference_type'):
                    raise OptionError(
                        '文件检查发现参考序列为:{}, 而说明的文件类型为:{}'.format(
                            self._fasta_type[self.option('reference').prop['seq_type'], self.option('reference_type')]))
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
            else:
                raise OptionError("Unigene文件不存在")

    def blast_run(self):
        if self.option('reference').is_set:
            self.dbtype = self.option('reference_type')
        else:
            self.dbtype = self._database_type[self.option('database')]
        self.checktype = {'nucl': {'nucl': 'blastn', 'prot': 'blastx'}, 'prot': {
            'nucl': 'tblastn', 'prot': 'blastp'}}
        self.blast.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': self.option('database'),
            'outfmt': 6,
            'blast': self.checktype[self.option('query_type')][self.dbtype],
            'reference': self.option('reference'),
            'reference_type': self.option('reference_type'),
            'evalue': self.option('evalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast.start()
        self.blast.on("end", self.set_output, 'blast')
        self.blast.run()

    def blast_gi_go_run(self):
        self.blast_gi_go.set_options({
            'query': self.option('query'),
            'query_type': self.option('query_type'),
            'database': 'nr',
            'outfmt': 6,
            'blast': self._blast_type[self.option('query_type')]['prot'][0],
            'evalue': self.option('evalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_gi_go.start()
        self.blast_gi_go.run()

    def blast_stat_run(self):
        if self.option('database') == 'nr':
            blastfile = self.blast.option('outxml')
        else:
            blastfile = self.blast_gi_go.option('outxml')
        self.blast_stat.set_options({
            'in_stat': blastfile
        })
        self.step.blast_stat.start()
        self.blast_stat.on("end", self.set_output, 'blast_stat')
        self.blast_stat.run()

    def ncbi_taxon_run(self):
        if self.option('database') == 'nr':
            blastfile = self.blast.option('outxml')
        else:
            blastfile = self.blast_gi_go.option('outxml')
        self.ncbi_taxon.set_options({
            'blastout': blastfile,
            'blastdb': 'nr'
        })
        self.step.ncbi_taxon.start()
        self.ncbi_taxon.on("end", self.set_output, 'ncbi_taxon')
        self.ncbi_taxon.run()

    def go_annot_run(self):
        if self.option('database') == 'nr':
            blastfile = self.blast.option('outxml')
        else:
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
            'evalue': self.option('evalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_string.start()
        self.blast_string.run()

    def string_cog_run(self):
        if self.option('database') == 'string':
            blastfile = self.blast.option('outxml')
        else:
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
            'evalue': self.option('evalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_kegg.start()
        self.blast_kegg.run()

    def kegg_annot_run(self):
        if self.option('database') == 'kegg':
            blastfile = self.blast.option('outxml')
        else:
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
            'evalue': self.option('evalue'),
            'num_threads': self.option('threads'),
            'num_alignment': 500
        })
        self.step.blast_kegg.start()
        self.blast_kegg.run()

    def anno_stat_run(self):
        if self.option('database') == 'nr':
            nr_blastfile = self.blast.option('outxml')
        else:
            nr_blastfile = self.blast_gi_go.option('outxml')
        if self.option('database') == 'string':
            string_blastfile = self.blast.option('outxml')
        else:
            string_blastfile = self.blast_string.option('outxml')
        if self.option('database') == 'kegg':
            kegg_blastfile = self.blast.option('outxml')
        else:
            kegg_blastfile = self.blast_kegg.option('outxml')
        self.anno_stat.set_options({
            'trinity_fasta': self.option('query'),
            'gene_fasta': self.option('query_gene'),
            'nr_blast_output': nr_blastfile,
            'swiss_blast_out': self.blast_swiss.option('outxml'),
            'string_blast_out': string_blastfile,
            'kegg_blast_out': kegg_blastfile,
            'ncbi_taxonomy_output_dir': self.ncbi_taxon.output_dir,
            'go_output_dir': self.go_annot.output_dir,
            'cog_output_dir': self.string_cog.output_dir,
            'kegg_output_dir': self.kegg_annot.output_dir,
            'blast_stat_output_dir': self.blast_stat.output_dir,
            'unigene': self.option('unigene')
        })
        self.anno_stat.start()
        self.anno_stat.on("end",self.set_output,'anno_stat')

    def run(self):
        super(DenovoAnnotationModule, self).run()
        self.blast_run()
        # self.step.update()
        #self.on_rely(self.blast, self.blast_stat_run)
        self.step.update()
        if self.option('database') == 'nr':
            self.on_rely(
                self.blast, [self.blast_stat_run, self.ncbi_taxon_run, self.go_annot_run])
        else:
            self.blast_gi_go_run()
            self.on_rely(self.blast_gi_go, [
                         self.ncbi_taxon_run, self.go_annot_run, self.blast_stat_run])
        self.step.update()
        if self.option('database') == 'string':
            self.on_rely(self.blast, self.string_cog_run)
        else:
            self.blast_string_run()
            self.on_rely(self.blast_string, self.string_cog_run)

        if self.option('database') == 'kegg':
            self.on_rely(self.blast, self.kegg_annot_run)
        else:
            self.blast_kegg_run()
            self.on_rely(self.blast_kegg, self.kegg_annot_run)
        self.step.update()
        if not self.option('database') == 'swissprot':
            self.blast_swiss_run()
            self.step.update()
        if self.option('anno_statistics') == True:
            self.on_rely([self.blast_stat, self.blast_swiss, self.ncbi_taxon,
                          self.go_annot, self.kegg_annot], self.anno_stat)
        else:
            self.on_rely([self.blast_stat, self.blast_swiss,
                          self.ncbi_taxon, self.go_annot, self.kegg_annot], self.end)
        # add annotation next time

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'blast':
            self.linkdir(obj.output_dir, 'blast')
            self.step.blast.finish()
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
        elif event[]
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
            ["cog/cog_table.xls", "xls", "序列COG注释详细表"]
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
