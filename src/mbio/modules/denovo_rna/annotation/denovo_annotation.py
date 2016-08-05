# -*- coding: utf-8 -*-
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
import Bio.SeqIO
import os


class DenovoAnnotationModule(Module):
    """
    module for denovorna annotation
    last modified:20160803
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
        self.gi_taxonomy = self.add_tool('taxon.ncbi_taxon')
        self.go_annot = self.add_tool('annotation.go_annotation')
        self.blast_string = self.add_tool('align.ncbi.blast')  # blast string
        #self.string_cog = self.add_tool('annotation.string2cog')
        self.blast_kegg = self.add_tool('align.ncbi.blast')  # blast kegg
        self.kegg_annot = self.add_tool('annotation.kegg_annotation')
        self.blast_swiss = self.add_tool('align.ncbi.blast')  # blast swiss
        #self.anno_stat = self.add_tool('annot.denovorna_anno_statistics')
        self.add_option(options)
        self.step_add_steps('blast', 'blast_stat', 'blast_gi_go', 'gi_taxonomy', 'blast_swiss',
                            'go_annot', 'blast_string', 'blast_kegg', 'kegg_annot')  # 'anno_stat' 'string_cog'

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

    def blast_stat_run(self):
        self.blast_stat.set_options({
            'in_stat': self.blast.option('outxml')
        })
        self.step.blast_stat.start()
        self.blast_stat.on("end", self.set_output, 'blast_stat')
        self.blast_stat.run()

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

    def gi_taxonomy_run(self):
        if self.option('database') == 'nr':
            blastfile = self.blast.option('outxml')
        else:
            blastfile = self.blast_gi_go.option('outxml')
        self.gi_taxonomy.set_options({
            'blastout': blastfile,
            'blastdb': 'nr'
        })
        self.step.gi_taxonomy.start()
        self.gi_taxonomy.on("end", self.set_output, 'gi_taxonomy')
        self.gi_taxonomy.run()

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
    '''
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
    '''

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

    def run(self):
        super(DenovoAnnotationModule, self).run()
        self.blast_run()
        self.step.update()
        self.on_rely(self.blast, self.blast_stat_run)
        self.step.update()
        if self.option('database') == 'nr':
            self.on_rely(self.blast, [self.gi_taxonomy_run, self.go_annot_run])
        else:
            self.blast_gi_go_run()
            self.on_rely(self.blast_gi_go, [
                         self.gi_taxonomy_run, self.go_annot_run])
        '''
        if self.option('database')=='string':
            self.on_rely(self.blast,self.string_cog_run)
        else:
            self.blast_string_run()
            self.on_rely(self.blast_string,self.string_cog_run)
        '''
        if self.option('database') == 'kegg':
            self.on_rely(self.blast, self.kegg_annot_run)
        else:
            self.blast_kegg_run()
            self.on_rely(self.blast_kegg, self.kegg_annot_run)
        if not self.option('database') == 'swissprot':
            self.blast_swiss_run()
        self.on_rely([self.blast_stat, self.blast_swiss,
                      self.gi_taxonomy, self.go_annot, self.kegg_annot],self.end)
        #add annotation next time

    def set_output(self,event):
        obj=event['bind_object']
        if event['data']=='blast':
            self.linkdir(obj.output_dir,'Blast')
            self.step.blast.finish()
        elif event['data']=='blast_stat':
            self.linkdir(obj.output_dir,'Blast_Statistics')
            self.step.blast_stat.finish()
        elif event['data']=='gi_taxonomy':
            self.linkdir(obj.output_dir,'NCBI_taxon')
            self.step.gi_taxonomy.finish()
        elif event['data']=='go_annot':
            self.linkdir(obj.output_dir,'GO_annotation')
            self.step.go_annot.finish()
        '''
        elif event['data']=='string_cog':
            self.linkdir(obj.output_dir,'String_to_COG')
            self.step.string_cog.finish()
        '''
        elif event['data']=='kegg_annot':
            self.linkdir(obj.output_dir,'KEGG_annotation')
            self.step.kegg_annot.finish()
        else:
            pass

    def linkdir(self,dirpath,dirname):
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
        repaths=[
            [".","","DENOVO_RNA结果文件目录"]，
            ['query_taxons_detail.xls', 'xls', '序列详细物种分类文件'],
            ["./kegg_table.xls", "xls", "KEGG annotation table"],
            ["./pathway_table.xls", "xls", "Sorted pathway table"],
            ["./kegg_taxonomy.xls", "xls", "KEGG taxonomy summary"],
            ["./blast2go.annot", "annot", "Go annotation based on blast output"],
            ["./query_gos.list", "list", "Merged Go annotation"],
            ["./go1234level_statistics.xls", "xls", "Go annotation on 4 levels"],
            ["./go2level.xls", "xls", "Go annotation on level 2"],
            ["./go3level.xls", "xls", "Go annotation on level 3"],
            ["./go4level.xls", "xls", "Go annotation on level 4"]
        ]
        regexps=[
            [r".+_vs_.+\.xml", "xml", "blast比对输出结果，xml格式"],
            [r".+_vs_.+\.xls", "xls", "blast比对输出结果，表格(制表符分隔)格式"],
            [r".+_vs_.+\.txt", "txt", "blast比对输出结果，非xml和表格(制表符分隔)格式"],
            [r".+_vs_.+\.txt_\d+\.xml", "xml", "Blast比对输出多xml结果，输出格式为14的单个比对结果文件,主结果文件在txt文件中"],
            [r".+_vs_.+\.txt_\d+\.json", "json", "Blast比输出对多json结果，输出格式为13的单个比对结果文件,主结果文件在txt文件中"],
            [r"pathways/ko\d+", 'pdf' , '标红pathway图']
        ]
        sdir=self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(DenovoAnnotationModule,self).end()