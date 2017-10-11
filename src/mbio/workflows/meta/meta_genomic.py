# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'

"""宏基因组分析工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
from bson import ObjectId
import os
import json
import shutil
import time
import functools


def time_count(func):  # 统计导表时间
    @functools.wraps(func)
    def wrapper(*args, **kw):
        start = time.time()
        func(*args, **kw)
        end = time.time()
        print("{}函数执行完毕，该阶段导表已进行{}s".format(func.__name__, end - start))
        return wrapper


class MetaGenomicWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        宏基因组workflow option参数设置
        """
        self._sheet = wsheet_object
        super(MetaGenomicWorkflow, self).__init__(wsheet_object)
        options = [
            {'name': 'test', 'type': 'bool', 'default': False},  # 是否为测试workflow
            {'name': 'main_id', 'type': 'string'},  # 原始序列主表_id
            {'name': 'in_fastq', 'type': 'infile', 'format': 'sequence.fastq_dir'},  # 输入的fq文件或fq文件夹
            # {'name': 'fq_type', 'type': 'string', 'default': 'PE'},  # PE OR SE
            {'name': 'speciman_info', 'type': 'infile', 'format': 'sequence.profile_table'},  #样本集信息表
            {'name': 'insertsize', 'type': 'infile', 'format': 'sample.insertsize_table'},  # 插入片段长度表
            {'name': 'qc', 'type': 'bool', 'default': False},  # 是否需要质控
            {'name': 'qc_quality', 'type': 'int', 'default': 20},  # 质控质量值标准
            {'name': 'qc_length', 'type': 'int', 'default': 30},  # 质控最短序列长度
            {'name': 'rm_host', 'type': 'bool', 'default': False},  # 是否需要去除宿主
            {'name': 'ref_database', 'type': 'string', 'default': ''},  # 宿主参考序列库中对应的物种名，eg：E.coli ,B.taurus
            {'name': 'ref_undefined', "type": 'infile', 'format': 'sequence.fasta_dir'},
            # 未定义的宿主序列所在文件夹，多个宿主cat到一个文件，并作为tool:align.bwa的输入文件，可不提供
            # {'name': 'assemble_tool', 'type': 'string', 'default': 'idba'},  # 选择拼接工具，soapdenovo OR idba
            {'name': 'assemble_type', 'type': 'string', 'default': 'simple'},
            # 选择拼接策略，soapdenovo OR idba OR megahit OR multiple
            {'name': 'min_contig', 'type': 'int', 'default': 300},  # 拼接序列最短长度
            {'name': 'min_gene', 'type': 'int', 'default': 100},  # 预测基因最短长度
            {'name': 'cdhit_identity', 'type': 'float', 'default': 0.95},  # 基因序列聚类相似度
            {'name': 'cdhit_coverage', 'type': 'float', 'default': 0.9},  # 基因序列聚类覆盖度
            {'name': 'soap_identity', 'type': 'float', 'default': 0.95},  # 基因丰度计算相似度
            {'name': 'nr', 'type': 'bool', 'default': True},  # 是否进行nr注释
            {'name': 'cog', 'type': 'bool', 'default': True},  # 是否进行cog注释
            {'name': 'kegg', 'type': 'bool', 'default': True},  # 是否进行kegg注释
            {'name': 'cazy', 'type': 'bool', 'default': True},  # 是否进行cazy注释
            {'name': 'ardb', 'type': 'bool', 'default': True},  # 是否进行ardb注释
            {'name': 'card', 'type': 'bool', 'default': True},  # 是否进行card注释
            {'name': 'vfdb', 'type': 'bool', 'default': True},  # 是否进行vfdb注释
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},  # 物种/功能分析输入环境因子表
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},  # 物种/功能分析输入group表
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        '''获取数据库信息'''
        self.json_path = self.config.SOFTWARE_DIR + "/database/Genome_DB_finish/annot_species.json"
        self.json_dict = self.get_json()
        '''初始化module/tool'''
        self.sequence = self.add_module('sequence.meta_genomic')
        self.qc = self.add_module('meta.qc.qc_and_stat')
        self.rm_host = self.add_module('meta.qc.bwa_remove_host')
        self.assem_soapdenovo = self.add_module('assemble.mg_ass_soapdenovo')
        self.assem_idba = self.add_module('assemble.mg_ass_idba')
        self.gene_predict = self.add_module('gene_structure.gene_predict')
        self.gene_set = self.add_module('cluster.uni_gene')
        self.nr = self.add_module('align.meta_diamond')
        self.cog = self.add_module('align.meta_diamond')
        self.kegg = self.add_module('align.meta_diamond')
        self.anno = self.add_module('annotation.mg_common_anno_stat')
        self.cazy = self.add_module('annotation.cazy_align_anno')
        self.ardb = self.add_module('annotation.ardb_annotation')
        self.card = self.add_module('annotation.card_annotation')
        self.vfdb = self.add_module('annotation.vfdb_annotation')
        self.table = self.add_tool('meta.create_abund_table')
        self.composition = self.add_module('meta.composition.composition_analysis')
        self.compare = self.add_module('meta.beta_diversity.beta_diversity')
        self.correlation = self.add_tool('statistical.pearsons_correlation')
        # self.XXX = self.add_module("XXX")
        # self.XXX = self.add_tool("XXX")
        '''add_steps'''
        self.step.add_steps('sequence', 'qc_', 'rm_host', 'assem', 'gene_predict', 'gene_set', 'nr_', 'cog',
                            'kegg', 'anno', 'cazy', 'vfdb', 'ardb', 'card', 'table', 'composition', 'compare',
                            'correlation')
        '''初始化自定义变量'''
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        self.anno_tool = []  # nr/kegg/cog注释记录
        self.all_anno = []  # 全部的注释记录(用于依赖关系)
        self.choose_anno = []  # 全部注释记录(字符型，用于物种与功能分析, 不含基因集)
        self.new_table = []  # 构建新丰度表模块
        self.analysis = []  # 分析模块
        self.nr_dir = ''  # 结果文件路径，导表时用
        self.cog_dir = ''
        self.kegg_dir = ''
        self.anno_table = dict()  # 注释结果表(含所有注释水平，含丰度)
        self.profile_table1 = dict()  # 注释丰度表(用于组成分析，相关性heatmap图)
        self.profile_table2 = dict()  # 注释丰度表(用于样品比较分析、rda、cca、db_rda分析)
        self.default_level1 = {
            'nr': 'Genus',
            'cog': 'Function',
            'kegg': 'Level1',
            'cazy': 'Class',
            'vfdb': 'Level1',  # 预测 vfs对应VfdbGene？
            'ardb': 'Type',  #anno表：Antibiotic type、ARG改名
            'card': 'Class',  #ARO对应什么？
        }
        # 每个数据库默认做的分析水平, 表格中对应head名称待确认
        self.default_level2 = {
            'nr': 'Genus',
            'cog': 'NOG',
            'kegg': 'Pathway',
            'cazy': 'Family',
            'vfdb': 'VFs',
            'ardb': 'ARG',
            'card': 'ARO_accession',
        }
        if self.option('test'):
            self.option('main_id', '111111111111111111111111')
            self.choose_anno = ['ardb', 'card', 'vfdb']
            self.anno_table = {
                'geneset': '/mnt/ilustre/users/sanger-dev/workspace/20170928/MetaGenomic_metagenome/output/geneset/gene_profile/RPKM.xls',
                'ardb': '/mnt/ilustre/users/sanger-dev/workspace/20170928/MetaGenomic_metagenome/output/ardb/gene_ardb_anno.xls',
                'card': '/mnt/ilustre/users/sanger-dev/workspace/20170928/MetaGenomic_metagenome/output/card/gene_card_anno.xls',
                'vfdb': '/mnt/ilustre/users/sanger-dev/workspace/20170928/MetaGenomic_metagenome/output/vfdb/gene_vfdb_predict_anno.xls',
            }
            #self.qc_fastq = self.qc.option('in_fastq')  # 暂未加入质控步骤，输入质控序列

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('main_id') and self.option('test') == False:
            raise OptionError('缺少主表id')
        if not self.option('in_fastq'):
            raise OptionError('需要输入原始fastq序列')
        # if not self.option('fq_type') in ['PE', 'SE']:
        #    raise OptionError('fq序列应为PE或SE')
        if self.option('qc') and not self.option('speciman_info').is_set:
            raise OptionError('质控需提供样本集信息表')
        if not self.option('insertsize'):
            raise OptionError('需要输入insertsize表')
        if not self.option('qc_quality') > 0 and not self.option('qc_quality') < 42:
            raise OptionError('qc最小质量值超出范围，应在0~42之间')
        if not self.option('qc_length') > 0:
            raise OptionError('qc最小长度值超出范围，应大于0')
        if self.option('rm_host'):
            if self.option('ref_database') == '' and not self.option('ref_undefined').is_set:
                raise OptionError('已选择去宿主，需输入参考数据库或参考序列')
            if self.option('ref_database') != '' and self.option('ref_undefined').is_set:
                raise OptionError('去宿主不可同时提供参考数据库及参考序列')
        # if not self.option('assemble_tool') in ['soapdenovo', 'idba']:
        #     raise OptionError('请检查拼接工具是否输入正确')
        if not self.option('assemble_type') in ['soapdenovo', 'idba', 'megahit', 'multiple']:
            raise OptionError('拼接策略参数错误，应为soapdenovo/idba/megahit/multiple')
        # if self.option('assemble_tool') == 'soapdenovo' and self.option('assemble_type') == 'multiple':
        #    raise OptionError('不支持SOAPdenovo混拼流程')
        if self.option('min_contig') < 200 or self.option('min_contig') > 1000:
            raise OptionError('最小Contig长度参数超出范围200~1000')
        if self.option('min_gene') < 0:
            raise OptionError('最小基因长度参数不能为负')
        if not 0.75 <= self.option("cdhit_identity") <= 1:
            raise OptionError("cdhit identity必须在0.75，1之间")
        if not 0 <= self.option("cdhit_coverage") <= 1:
            raise OptionError("cdhit coverage必须在0,1之间")
        if not 0 < self.option("soap_identity") < 1:
            raise OptionError("soap identity必须在0，1之间")
        if not self.option('insertsize'):
            raise OptionError("必须输入插入片段")
        if not self.option('group'):
            raise OptionError('必须输入分组文件')
        return True

    def get_json(self):
        f = open(self.json_path, 'r')
        json_dict = json.loads(f.read())
        return json_dict

    def get_sample(self):
        samp_list = []
        with open(self.option('group').prop['path'], 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                t = line.split('\t')
                samp_list.append(t)
        return samp_list

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def set_run(self, opts, module, event, step, start = True):
        module.set_options(opts)
        module.on('start', self.set_step, {'start': step})
        module.on('end', self.set_step, {'end': step})
        module.on('end', self.set_output, event)
        if start:
            module.run()

    def run_sequence(self):
        opts = {
            'fastq_dir': self.option('in_fastq'),
        }
        self.set_run(opts, self.sequence, 'sequence', self.step.sequence)

    def run_qc(self):
        opts = {
            'fastq_dir': self.sequence.output_dir + '/data',
            'stat_dir': self.sequence.output_dir + '/base_info',
            'insert_size': self.option('speciman_info'),
        }
        self.set_run(opts, self.qc, 'qc', self.step.sequence)

    def run_rm_host(self):
        opts = {
            'fq_type': 'PSE',
            'ref_database': self.option('ref_database'),
            'ref_undefined': self.option('ref_undefined'),
        }
        if self.option('qc'):
            opts['fastq_dir'] = self.qc.option('after_remove_dir')
        else:
            opts['fastq_dir'] = self.option('in_fastq')
        self.set_run(opts, self.rm_host, 'rm_host', self.step.rm_host)

    def run_assem(self):
        opts = {
            'data_id': self.option('main_id'),
            'min_contig': self.option('min_contig'),
        }
        if self.option('rm_host'):
            opts['QC_dir'] = self.rm_host.option('result_fq_dir')
        elif self.option('qc'):
            opts['QC_dir'] = self.qc.option('sickle_dir')
        else:
            opts['QC_dir'] = self.option('in_fastq')
        if self.option('assemble_type') == 'soapdenovo':
            self.set_run(opts, self.assem_soapdenovo, 'assem', self.step.assem)
        else:
            if self.option('assemble_type') == 'idba':
                opts['assemble_tool'] = 'idba'
                opts['method'] = 'simple'
            if self.option('assemble_type') == 'megahit':
                opts['assemble_tool'] = 'megahit'
                opts['method'] = 'simple'
            if self.option('assemble_type') == 'multiple':
                opts['method'] = 'multiple'
            self.set_run(opts, self.assem_idba, 'assem', self.step.assem)

    def run_gene_predict(self):
        opts = {
            'min_gene': str(self.option('min_gene')),
        }
        if self.option('assemble_type') == 'soapdenovo':
            opts['input_fasta'] = self.assem_soapdenovo.option('contig')
        else:
            opts['input_fasta'] = self.assem_idba.option('contig')
        self.set_run(opts, self.gene_predict, 'gene_predict', self.step.gene_predict)

    def run_gene_set(self):
        opts = {
            'gene_tmp_fa': self.gene_predict.option('out'),
            'insertsize': self.option('insertsize'),
            'cdhit_identity': self.option('cdhit_identity'),
            'cdhit_coverage': self.option('cdhit_coverage'),
            'soap_identity': self.option('soap_identity'),
        }
        if self.option('rm_host'):
            opts['QC_dir'] = self.rm_host.option('result_fq_dir')
        elif self.option('qc'):
            opts['QC_dir'] = self.qc.option('sickle_dir')
        else:
            opts['QC_dir'] = self.option('in_fastq')
        self.set_run(opts, self.gene_set, 'gene_set', self.step.gene_set)
        self.anno_table['geneset'] = self.gene_set.option('rpkm_abundance')
        self.choose_anno.append('geneset')

    def run_nr(self):
        opts = {
            'query': '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',  #self.gene_set.option('uni_fastaa'),
            # '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',
            'query_type': "prot",
            'database': 'nr',
        }
        self.set_run(opts, self.nr, 'nr', self.step.nr_)

    def run_kegg(self):
        opts = {
            'query': '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',  #self.gene_set.option('uni_fastaa'),
            # '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',
            'query_type': "prot",
            'database': 'kegg',
        }
        self.set_run(opts, self.kegg, 'kegg', self.step.kegg)

    def run_cog(self):
        opts = {
            'query': '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',  #self.gene_set.option('uni_fastaa'),
            # '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/uniGeneset/gene.uniGeneset.faa',
            'query_type': "prot",
            'database': 'eggnog',
        }
        self.set_run(opts, self.cog, 'cog', self.step.cog)

    def run_anno(self):
        opts = {
            'reads_profile_table': '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/gene_profile/RPKM.xls',  #self.gene_set.option('reads_abundance'),
            # self.gene_set.option('rpkm_abundance'),  # '/mnt/ilustre/users/sanger-dev/workspace/20170921/MetaGenomic_metagenome/UniGene/output/gene_profile/RPKM.xls'
        }
        if self.option('nr'):
            opts['nr_xml_dir'] = self.nr.option('outxml_dir')
        if self.option('kegg'):
            opts['kegg_xml_dir'] = self.kegg.option('outxml_dir')
        if self.option('cog'):
            opts['cog_xml_dir'] = self.cog.option('outxml_dir')
        self.set_run(opts, self.anno, 'anno', self.step.anno)

    def run_cazy(self):
        opts = {
            'query': self.gene_set.option('uni_fastaa'),
            'reads_profile_table': self.gene_set.option('reads_abundance'),
        }
        self.set_run(opts, self.cazy, 'cazy', self.step.cazy)

    def run_vfdb(self):
        opts = {
            'query': self.gene_set.option('uni_fastaa'),
            'reads_profile_table': self.gene_set.option('reads_abundance'),
        }
        self.set_run(opts, self.vfdb, 'vfdb', self.step.vfdb)

    def run_ardb(self):
        opts = {
            'query': self.gene_set.option('uni_fastaa'),
            'reads_profile_table': self.gene_set.option('reads_abundance'),
        }
        self.set_run(opts, self.ardb, 'ardb', self.step.ardb)

    def run_card(self):
        opts = {
            'query': self.gene_set.option('uni_fastaa'),
            'reads_profile_table': self.gene_set.option('reads_abundance'),
        }
        self.set_run(opts, self.card, 'card', self.step.card)

    def run_analysis(self, event):
        for db in self.choose_anno:
            self.profile_table1[db] = self.run_new_table(self.anno_table[db], self.anno_table['geneset'],
                                                    self.default_level1[db])
            if self.default_level2[db] == self.default_level1[db] and event == 'all':
                self.profile_table2[db] = self.profile_table1[db]
            elif self.default_level2[db] != self.default_level1[db] and event == 'all':
                self.profile_table2[db] = self.run_new_table(self.anno_table[db], self.anno_table['geneset'],
                                                        self.default_level2[db])
        for module in self.new_table:
            module.run()
        for db in self.profile_table1.keys():
            if self.option('test'):
                self.logger.info(event)
                self.logger.info(self.profile_table1[db])
                self.logger.info(self.option('group').prop['path'])
            self.run_composition(self.profile_table1[db], self.option('group').prop('path'))
        for db in self.profile_table2.keys():
            self.run_compare(self.profile_table2[db], self.option('group'))
        self.on_rely(self.analysis, self.end)
        for module in self.analysis:
            module.run()

    def run_new_table(self, anno, gene, level):
        opts = {
            'anno_table': anno,
            'geneset_table': gene,
            'level_type': level,
        }
        self.set_run(opts, self.table, 'table', self.step.table, False)
        self.new_table.append(self.table)
        new_table_file = self.table.output_dir + '/new_abund_table.xls'
        return new_table_file

    def run_composition(self, abund, group):
        opts = {
            'analysis': 'bar,heatmap,circos',
            'abundtable': abund,
            'group': group,
            'species_number': '50',
        }
        self.set_run(opts, self.composition, 'composition', self.step.composition, False)
        self.analysis.append(self.composition)

    def run_compare(self, abund, group):
        opts = {
            'dis_method': 'bray_curtis',
            'otutable': abund,
            'group': group,
        }
        if self.option('envtable').is_set:
            opts['envtable'] = self.option('envtable')
            opts['analysis'] = 'distance,pca,pcoa,nmds,rda_cca,dbrda,hcluster'
        else:
            opts['analysis'] = 'distance,pca,pcoa,nmds,hcluster'
        # event = db + '_compare'  # 是否需要将所有的分析按数据库拆开
        self.set_run(opts, self.compare, 'compare', self.step.compare, False)
        self.analysis.append(self.compare)

    '''处理输出文件'''

    def set_output(self, event):
        """
        将各个模块的结果输出至output
        """
        obj = event['bind_object']
        if event['data'] == 'sequece':
            self.move_dir(obj.output_dir, 'rawdata')
        if event['data'] == 'qc':
            self.move_dir(obj.output_dir, 'qc')
        if event['data'] == 'rm_host':
            self.move_dir(obj.output_dir, 'rm_host')
        if event['data'] == 'assem':
            self.move_dir(obj.output_dir, 'assemble')
        if event['data'] == 'gene_predict':
            self.move_dir(obj.output_dir, 'predict')
        if event['data'] == 'gene_set':
            self.move_dir(obj.output_dir, 'geneset')
        if event['data'] == 'anno':
            # self.move_dir(obj.output_dir, 'anno')  # 怎样将nr、cog、kegg拆开,需要传入路径
            if self.option('nr'):
                self.nr_dir = os.path.join(obj.output_dir, 'nr_tax_level')
                self.anno_table['nr'] = os.path.join(self.nr_dir, 'gene_nr_anno.xls')
                self.move_dir(self.nr_dir, 'nr')
            if self.option('cog'):
                self.cog_dir = os.path.join(obj.output_dir, 'cog_result_dir')
                self.anno_table['cog'] = os.path.join(self.cog_dir, 'gene_cog_anno.xls')
                self.move_dir(self.cog_dir, 'cog')
            if self.option('kegg'):
                self.kegg_dir = os.path.join(obj.output_dir, 'kegg_result_dir')
                self.anno_table['kegg'] = os.path.join(self.kegg_dir, 'gene_kegg_anno.xls')
                self.move_dir(self.kegg_dir, 'kegg')
        if event['data'] == 'cazy':
            self.anno_table['cazy'] = os.path.join(obj.output_dir, 'gene_cazy_anno.xls')
            self.move_dir(obj.output_dir, 'cazy')
        if event['data'] == 'vfdb':
            self.anno_table['vfdb'] = os.path.join(obj.output_dir, 'gene_vfdb_anno.xls')
            self.move_dir(obj.output_dir, 'vfdb')
        if event['data'] == 'ardb':
            self.anno_table['ardb'] = os.path.join(obj.output_dir, 'gene_ardb_anno.xls')
            self.move_dir(obj.output_dir, 'ardb')
        if event['data'] == 'card':
            self.anno_table['card'] = os.path.join(obj.output_dir, 'gene_card_anno.xls')
            self.move_dir(obj.output_dir, 'card')
        if event['data'] == 'composition':
            self.move_dir(obj.output_dir, 'composition')
        if event['data'] == 'compare':
            self.move_dir(obj.output_dir, 'compare')  # 是否把分析内容拆开？
        if event['data'] == 'correlation':
            self.move_dir(obj.output_dir, 'correlation')

    def set_output_all(self):
        """
        将所有结果一起导出至output
        """
        pass

    def move_dir(self, olddir, newname):  # 原函数名move2outputdir
        """
        移动一个目录下所有文件/文件夹到workflow输出路径下，供set_output调用
        """
        start = time.time()
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            os.makedirs(newdir)
        allfiles = os.listdir(olddir)
        oldfiles = [os.path.join(olddir, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        self.logger.info(newfiles)
        for newfile in newfiles:
            if os.path.isfile(newfile) and os.path.exists(newfile):
                os.remove(newfile)
            elif os.path.isdir(newfile) and os.path.exists(newfile):
                shutil.rmtree(newfile)
        for i in range(len(allfiles)):
            self.move_file(oldfiles[i], newfiles[i])
        end = time.time()
        duration = end - start
        self.logger.info("文件夹{}移动到{},耗时{}s".format(olddir, newdir, duration))

    def move_file(self, old_file, new_file):
        """
        递归移动文件夹的内容，供move_dir调用
        """
        if os.path.isfile(old_file):
            os.link(old_file, new_file)
        elif os.path.isdir(old_file):
            os.mkdir(new_file)
            for file in os.listdir(old_file):
                file_path = os.path.join(old_file, file)
                new_path = os.path.join(new_file, file)
                self.move_file(file_path, new_path)
        else:
            self.logger.info("导出失败：请检查{}".format(old_file))

    def end(self):
        # self.run_api()
        self.set_upload_results()
        super(MetaGenomicWorkflow, self).end()

    def set_upload_results(self):  # 原modify_output
        """
        结果放置到/upload_results
        """
        pass

    '''导表'''

    def run_api(self, test=False):  # 原run_api_and_set_output
        greenlets_list_first = []  # 一阶段导表
        greenlets_list_sec = []  # 二阶段导表
        greenlets_list_third = []  # 三阶段导表

    def export_test(self):
        self.api_qc = XXX
        self.group_id = ObjectId(XX)
        self.api_qc.add_control_group(XXX)

    @time_count
    def export_XXX(self):
        pass

    def run(self):
        """
        运行 meta_genomic workflow
        :return:
        """
        task_info = self.api.api('task_info.ref')
        task_info.add_task_info()
        self.sequence.on('end', self.run_qc)
        if self.option('rm_host'):
            self.qc.on('end', self.run_rm_host)
            self.rm_host.on('end', self.run_assem)
        else:
            self.qc.on('end', self.run_assem)
        self.assem_soapdenovo.on('end', self.run_gene_predict)
        self.assem_idba.on('end', self.run_gene_predict)
        self.gene_predict.on('end', self.run_gene_set)
        if self.option('nr'):
            self.gene_set.on('end', self.run_nr)
            self.anno_tool.append(self.nr)
        if self.option('kegg'):
            self.gene_set.on('end', self.run_kegg)
            self.anno_tool.append(self.kegg)
        if self.option('cog'):
            self.gene_set.on('end', self.run_cog)
            self.anno_tool.append(self.cog)
        if self.option('cazy'):
            self.gene_set.on('end', self.run_cazy)
            self.all_anno.append(self.cazy)
        if self.option('ardb'):
            self.gene_set.on('end', self.run_ardb)
            self.all_anno.append(self.ardb)
        if self.option('card'):
            self.gene_set.on('end', self.run_card)
            self.all_anno.append(self.card)
        if self.option('vfdb'):
            self.gene_set.on('end', self.run_vfdb)
            self.all_anno.append(self.vfdb)
        if len(self.anno_tool) != 0:
            self.on_rely(self.anno_tool, self.run_anno)
            self.all_anno.append(self.anno)
        if len(self.all_anno) == 0:
            self.gene_set.on('end', self.end)
        else:
            self.sample_in_group = self.get_sample()
            if len(self.sample_in_group) < 2:
                self.on_rely(self.all_anno, self.end)
            elif len(self.sample_in_group) == 2:
                self.on_rely(self.all_anno, self.run_analysis, 'composition')
                self.on_rely(self.analysis, self.end)
            elif len(self.sample_in_group) > 2:
                self.on_rely(self.all_anno, self.run_analysis, 'all')
                # self.on_rely(self.analysis, self.end)
        if self.option('test'):
            self.run_cog()
            self.run_kegg()
            self.run_nr()
            #self.run_analysis('all')
            super(MetaGenomicWorkflow, self).run()
            return True
        if self.option('qc'):
            self.run_sequence()
        elif self.option('rm_host'):
            self.run_rm_host()
        else:
            self.run_assem()
        super(MetaGenomicWorkflow, self).run()
