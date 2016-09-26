# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""无参转录组基础分析"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import shutil
import threading
import re


class DenovoBaseWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        version = v1.0
        last_modify = 20160825
        """
        self._sheet = wsheet_object
        super(DenovoBaseWorkflow, self).__init__(wsheet_object)
        print self._parent
        options = [
            {"name": "fastq_dir", "type": "infile", 'format': "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 有生物学重复的时候的分组文件
            {"name": "control_file", "type": "infile", "format": "denovo_rna.express.control_table"},  # 对照组文件，格式同分组文件
            {"name": "search_pfam", "type": "bool", "default": False},  # orf 是否比对Pfam数据库
            {"name": "primer", "type": "bool", "default": True},  # 是否设计SSR引物
            {"name": "kmer_size", "type": "int", "default": 25},
            {"name": "min_kmer_cov", "type": "int", "default": 2},
            {"name": "min_contig_length", "type": "int", "default": 200},  # trinity报告出的最短的contig长度。默认为200
            {"name": "SS_lib_type", "type": "string", "default": 'none'},  # reads的方向，成对的reads: RF or FR; 不成对的reads: F or R，默认情况下，不设置此参数
            {"name": "exp_way", "type": "string", "default": "fpkm"},  # edger离散值
            {"name": "diff_ci", "type": "float", "default": 0.01},  # 显著性水平
            {"name": "diff_rate", "type": "float", "default": 0.01},  # 期望的差异基因比率
            {"name": "anno_analysis", "type": "string", "default": ""},
            {"name": "exp_analysis", "type": "string", "default": "cluster,network,kegg_rich,go_rich"},
            {"name": "gene_analysis", "type": "string", "default": "orf"},
            {"name": "map_qc_analysis", "type": "string", "default": "satur,dup,coverage,correlation"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filecheck = self.add_tool("denovo_rna.filecheck.file_denovo")
        self.qc = self.add_module("denovo_rna.qc.quality_control")
        self.qc_stat_before = self.add_module("denovo_rna.qc.qc_stat")
        self.qc_stat_after = self.add_module("denovo_rna.qc.qc_stat")
        self.assemble = self.add_tool("denovo_rna.assemble.assemble")
        # self.annotation = self.add_module('denovo_rna.annotation.denovo_annotation')
        self.annotation = self.add_module('denovo_rna.qc.qc_stat')
        self.orf = self.add_tool("denovo_rna.gene_structure.orf")
        self.ssr = self.add_tool("denovo_rna.gene_structure.ssr")
        self.bwa = self.add_module("denovo_rna.mapping.bwa_samtools")
        self.snp = self.add_module("denovo_rna.gene_structure.snp")
        self.map_qc = self.add_module("denovo_rna.mapping.map_assessment")
        self.exp_stat = self.add_module("denovo_rna.express.exp_analysis")
        self.exp_diff = self.add_module("denovo_rna.express.diff_analysis")
        self.orf_len = self.add_tool("meta.qc.reads_len_info")
        self.step.add_steps("qcstat", "assemble", "annotation", "express", "gene_structure", "map_stat")
        self.final_tools = list()
        self.update_status_api = self.api.denovo_update_status
        self.spname_spid = dict()
        self.samples = self.option('fastq_dir').samples
        self.diff_gene_id = None
        self.diff_genes = None
        # self.logger.info('{}'.format(self.events))
        # self.logger.info('{}'.format(self.children))
        # self.logger.info('{}'.format(self._upload_dir_obj))
        # self.logger.info('{}'.format(self.qc_stat_before._upload_dir_obj))
        # self.logger.info('{}'.format(self.qc_stat_after._upload_dir_obj))

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('fastq_dir').is_set:
            raise OptionError('必须设置输入fastq文件夹')
        if not self.option('control_file').is_set:
            raise OptionError('必须设置输入对照方案文件')
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        if self.option('diff_ci') > 1 or self.option('diff_ci') < 0:
            raise OptionError('显著性水平不在所给范围内[0,1]')
        if self.option('diff_rate') > 1 or self.option('diff_rate') < 0:
            raise OptionError('差异基因比率不在所给范围内[0,1]')
        if self.option("fq_type") == 'SE' and self.option("SS_lib_type") not in ['F', 'R', 'none']:
            raise OptionError("SE测序时所设reads方向：{}不正确".format(self.option("SS_lib_type")))
        if self.option("fq_type") == 'PE' and self.option("SS_lib_type") not in ['FR', 'RF', 'none']:
            raise OptionError("PE测序时所设reads方向：{}不正确".format(self.option("SS_lib_type")))
        if self.option("exp_way") not in ['fpkm', 'tpm']:
            raise OptionError("所设表达量的代表指标不在范围内，请检查")
        if self.option('kmer_size') > 32 or self.option('kmer_size') < 1:
            raise OptionError("所设kmer_size不在范围内，请检查")
        if self.option('min_kmer_cov') < 1:
            raise OptionError("所设min_kmer_cov不在范围内，请检查")
        for i in self.option('exp_analysis').split(','):
            if i not in ['', 'cluster', 'network', 'kegg_rich', 'go_rich']:
                raise OptionError("差异性研究没有{}，请检查".format(i))
        for i in self.option('gene_analysis').split(','):
            if i not in ['orf', 'ssr', 'snp']:
                raise OptionError("基因结构分析没有{}，请检查".format(i))
        for i in self.option('map_qc_analysis').split(','):
            if i not in ['', 'satur', 'coverage', 'dup', 'correlation']:
                raise OptionError("转录组质量评估没有{}，请检查".format(i))

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def run_filecheck(self):
        opts = {
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type'),
            'control_file': self.option('control_file')
        }
        if self.option('group_table').is_set:
            opts.update({'group_table': self.option('group_table')})
        self.filecheck.set_options(opts)
        self.filecheck.run()

    def run_qc(self):
        self.qc.set_options({
            'fastq_dir': self.option('fastq_dir'),
            'fq_type': self.option('fq_type')
        })
        self.qc.on('end', self.set_output, 'qc')
        self.qc.on('start', self.set_step, {'start': self.step.qcstat})
        self.qc.on('end', self.set_step, {'end': self.step.qcstat})
        self.qc.run()

    def run_qc_stat(self, event):
        if event['data']:
            self.qc_stat_after.set_options({
                'fastq_dir': self.qc.option('sickle_dir'),
                'fq_type': self.option('fq_type'),
                'dup': True
            })
        else:
            self.qc_stat_before.set_options({
                'fastq_dir': self.option('fastq_dir'),
                'fq_type': self.option('fq_type')})
        if event['data']:
            self.qc_stat_after.on('end', self.set_output, 'qc_stat_after')
            self.qc_stat_after.run()
        else:
            self.qc_stat_before.on('end', self.set_output, 'qc_stat_before')
            self.qc_stat_before.run()

    def run_assemble(self):
        opts = {
            'fq_type': self.option('fq_type'),
            'min_contig_length': self.option('min_contig_length'),
            'SS_lib_type': self.option('SS_lib_type'),
            'kmer_size': self.option('kmer_size'),
            'min_kmer_cov': self.option('min_kmer_cov'),
        }
        if self.option('fq_type') == 'SE':
            opts.update({'fq_s': self.qc.option('fq_s')})
        else:
            opts.update({
                        'fq_l': self.qc.option('fq_l'),
                        'fq_r': self.qc.option('fq_r')
                        })
        self.assemble.set_options(opts)
        self.assemble.on('end', self.set_output, 'assemble')
        self.assemble.on('start', self.set_step, {'start': self.step.assemble})
        self.assemble.on('end', self.set_step, {'end': self.step.assemble})
        self.assemble.run()

    def run_orf(self):
        orf_opts = {
            'fasta': self.assemble.option('trinity_fa'),
            'search_pfam': self.option('search_pfam')
        }
        self.orf.set_options(orf_opts)
        self.orf.on('end', self.set_output, 'orf')
        self.orf.on('start', self.set_step, {'start': self.step.gene_structure})
        self.orf.run()

    def run_orf_len(self):
        orf_fasta = self.orf.work_dir + '/ORF_fasta'
        self.orf_len.set_options({'fasta_path': orf_fasta})
        self.orf_len.on('end', self.set_output, 'orf_len')
        self.orf_len.run()

    def run_bwa(self):
        bwa_opts = {
            'ref_fasta': self.assemble.option('gene_fa')
        }
        if self.option('fq_type') == 'SE':
            bwa_opts.update({'fastq_s': self.qc.option('fq_s')})
        else:
            bwa_opts.update({'fastq_r': self.qc.option('fq_r')})
            bwa_opts.update({'fastq_l': self.qc.option('fq_l')})
        self.bwa.set_options(bwa_opts)
        self.bwa.run()

    def run_snp(self):
        snp_opts = {
            'bed': self.orf.option('bed'),
            'bam': self.bwa.option('out_bam'),
            'ref_fasta': self.assemble.option('gene_fa')
        }
        self.snp.set_options(snp_opts)
        self.snp.on('end', self.set_output, 'snp')
        self.snp.run()

    def run_ssr(self):
        ssr_opts = {
            'fasta': self.assemble.option('gene_fa'),
            'bed': self.orf.option('bed'),
            'primer': self.option('primer')
        }
        self.ssr.set_options(ssr_opts)
        self.ssr.on('end', self.set_output, 'ssr')
        self.ssr.run()

    def run_map_qc(self):
        map_qc_opts = {
            'bed': self.orf.option('bed'),
            'bam': self.exp_stat.option('bam_dir'),
            'fpkm': self.exp_stat.option('gene_fpkm'),
            'analysis': self.option('map_qc_analysis')
        }
        self.map_qc.set_options(map_qc_opts)
        self.map_qc.on('end', self.set_output, 'map_qc')
        self.map_qc.on('end', self.set_step, {'end': self.step.map_stat})
        self.map_qc.run()

    def run_annotation(self):
        anno_opts = {
            "query": self.assemble.option('trinity_fa'),
            "query_type": 'nucl',
            "gi_taxon": True,
            "go_annot": True,
            "cog_annot": True,
            "kegg_annot": True,
            "blast_stat": True
        }
        self.annotation.set_options(anno_opts)
        self.annotation.on('end', self.set_output, 'annotation')
        self.annotation.on('start', self.set_step, {'start': self.step.annotation})
        self.annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.annotation.run()

    def run_exp_stat(self):
        exp_stat_opts = {
            'fq_type': self.option('fq_type'),
            'rsem_fa': self.assemble.option('trinity_fa'),
            'control_file': self.option('control_file'),
            'exp_way': self.option('exp_way'),
            'diff_ci': self.option('diff_ci'),
            'diff_rate': self.option('diff_rate')
        }
        if self.option('fq_type') == 'SE':
            exp_stat_opts.update({'fq_s': self.qc.option('sickle_dir')})
        else:
            exp_stat_opts.update({'fq_r': self.qc.option('sickle_r_dir'), 'fq_l': self.qc.option('sickle_l_dir')})
        if self.option('group_table').is_set:
            exp_stat_opts.update({
                'group_table': self.option('group_table'),
                'gname': self.option('group_table').prop['group_scheme'][0]
            })
        self.exp_stat.set_options(exp_stat_opts)
        self.exp_stat.on('end', self.set_output, 'exp_stat')
        self.exp_stat.on('start', self.set_step, {'start': self.step.express})
        self.exp_stat.run()

    def run_exp_diff(self):
        if self.exp_stat.diff_gene:
            exp_diff_opts = {
                'diff_fpkm': self.exp_stat.option('diff_fpkm'),
                'analysis': self.option('exp_analysis')
            }
            if 'network' in self.option('exp_analysis'):
                exp_diff_opts.update({'gene_file': self.exp_stat.option('gene_file')})
            elif 'kegg_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'kegg_path': self.annotation.option('kegg_path'),
                    'diff_list_dir': self.exp_stat.option('diff_list_dir')
                })
            elif 'go_rich' in self.option('exp_analysis'):
                exp_diff_opts.update({
                    'go_list': self.annotation.option('go_list'),
                    'diff_list_dir': self.exp_stat.option('diff_list_dir'),
                    'all_list': self.exp_stat.option('all_list'),
                    'go_level_2': self.annotation.option('go_level_2')
                })
            self.exp_diff.set_options(exp_diff_opts)
            self.exp_diff.on('end', self.set_output, 'exp_diff')
            self.exp_diff.on('end', self.set_step, {'end': self.step.express})
            self.exp_diff.run()
            self.final_tools.append(self.exp_diff)
        else:
            self.logger.info('输入文件数据量过小，没有检测到差异基因，差异基因相关分析将忽略')

    def move2outputdir(self, olddir, newname, mode='link'):
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
            self.logger.info(newfiles)
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

    def set_output(self, event):
        obj = event["bind_object"]
        # 设置qc报告文件
        if event['data'] == 'qc':
            self.move2outputdir(obj.output_dir, 'QC_stat')
        if event['data'] == 'qc_stat_before':
            self.move2outputdir(obj.output_dir, 'QC_stat/before_qc')
            # set api
            api_sample = self.api.denovo_rna_sample
            qc_stat_info = self.output_dir + '/QC_stat/before_qc'
            quality_stat = self.output_dir + '/QC_stat/before_qc/qualityStat'
            if not os.path.exists(qc_stat_info):
                raise Exception('找不到报告文件：{}'.format(qc_stat_info))
            if not os.path.exists(quality_stat):
                raise Exception('找不到报告文件：{}'.format(quality_stat))
            api_sample.add_samples_info(qc_stat=qc_stat_info, qc_adapt=None, fq_type=self.option('fq_type'))
            api_sample.add_gragh_info(quality_stat, about_qc='before')
        if event['data'] == 'qc_stat_after':
            self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
            # set api
            api_sample = self.api.denovo_rna_sample
            qc_stat_info = self.output_dir + '/QC_stat/after_qc'
            quality_stat = self.output_dir + '/QC_stat/after_qc/qualityStat'
            qc_adapt = self.output_dir + '/QC_stat/adapter.xls'
            files = [qc_stat_info, quality_stat, qc_adapt]
            for f in files:
                if not os.path.exists(f):
                    raise Exception('找不到报告文件：{}'.format(f))
            api_sample.add_samples_info(qc_stat=qc_stat_info, qc_adapt=qc_adapt, fq_type=self.option('fq_type'))
            api_sample.add_gragh_info(quality_stat, about_qc='after')
            self.spname_spid = api_sample.get_spname_spid()
            if self.option('group_table').is_set:
                api_group = self.api.group
                api_group.add_ini_group_table(self.option('group_table').prop['path'], self.spname_spid)
        if event['data'] == 'assemble':
            self.move2outputdir(obj.output_dir, 'Assemble')
            # set api
            api_assemble = self.api.denovo_assemble
            trinity_path = obj.work_dir + '/trinity_out_dir/Trinity.fasta'
            gene_path = obj.work_dir + '/gene.fasta'
            stat_path = self.output_dir + '/Assemble/trinity.fasta.stat.xls'
            files = [trinity_path, gene_path, stat_path]
            for f in files:
                if not os.path.exists(f):
                    raise Exception('找不到报告文件：{}'.format(f))
            sequence_id = api_assemble.add_sequence(trinity_path, gene_path)
            api_assemble.add_sequence_detail(sequence_id, stat_path)
            threads = []
            for f in os.listdir(self.output_dir + '/Assemble'):
                if re.search(r'length\.distribut\.txt$', f):
                    step = f.split('_')[0]
                    file_ = self.output_dir + '/Assemble/' + f
                    t = threading.Thread(api_assemble.add_sequence_step, args=(file_, step))
                    threads.append(t)
            for th in threads:
                th.setDaemon = True
                th.start()
            th.join()
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, 'Map_stat')
        if event['data'] == 'orf':
            self.move2outputdir(obj.output_dir, 'Gene_structure/orf')
        if event['data'] == 'orf_len':
            self.move2outputdir(obj.output_dir, 'Gene_structure/orf')
        if event['data'] == 'ssr':
            self.move2outputdir(obj.output_dir, 'Gene_structure/ssr')
        if event['data'] == 'snp':
            self.move2outputdir(obj.output_dir, 'Gene_structure/snp')
        if event['data'] == 'exp_stat':
            self.move2outputdir(obj.output_dir, 'Express')
            self.logger.info('%s' % self.exp_stat.diff_gene)
            # set api
            api_express = self.api.denovo_express
            express_id = api_express.add_express(samples=self.samples, params=None, name=None)
            rsem_files = os.listdir(self.output_dir + '/Express/rsem/')
            for f in rsem_files:
                if re.search(r'^genes\.TMM', f):
                    count_path = self.output_dir + '/Express/rsem/' + f
                    fpkm_path = self.output_dir + '/Express/rsem/genes.counts.matrix'
                    api_express.add_express_detail(express_id, count_path, fpkm_path, 'gene')
                elif re.search(r'^transcripts\.TMM', f):
                    count_path = self.output_dir + '/Express/rsem/' + f
                    fpkm_path = self.output_dir + '/Express/rsem/transcripts.counts.matrix'
                    api_express.add_express_detail(express_id, count_path, fpkm_path, 'transcript')
                elif re.search(r'\.genes\.results$', f):
                    sample = f.split('.genes.results')[0]
                    file_ = self.output_dir + '/Express/rsem/' + f
                    api_express.add_express_specimen_detail(express_id, file_, 'gene', sample)
                elif re.search(r'\.isoforms\.results$', f):
                    sample = f.split('.genes.results')[0]
                    file_ = self.output_dir + '/Express/rsem/' + f
                    api_express.add_express_specimen_detail(express_id, file_, 'transcript', sample)
            diff_files = os.listdir(self.output_dir + '/Express/diff_exp/')
            param_1 = {
                'ci': self.option('diff_ci'),
                # 'group_id': 'All',
                # 'group_detail'
                # 'control_id': 'All',
                # 'control_detail'
                'rate': self.option('diff_rate'),
            }
            compare_column = list()
            for f in diff_files:
                if re.search(r'_edgr_stat.xls$', f):
                    con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                    compare_column.append('|'.join(con_exp))
            express_diff_id = api_express.add_express_diff(param_1, self.samples, compare_column)
            path_ = os.path.join(self.output_dir, '/Express/diff_exp/')
            for f in diff_files:
                if re.search(r'_edgr_stat.xls$', f):
                    con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                    api_express.add_express_diff_detail(express_diff_id, con_exp, path_ + f)
                if f == 'diff_fpkm':
                    param_2 = {
                        # 'express_diff_id': ,
                        'compare_list': compare_column,
                        'is_sum': True,
                    }
                    self.diff_gene_id = api_express.add_express(samples=self.samples, params=param_2, express_id=express_id)
                    api_express.add_express_detail(self.diff_gene_id, path_ + 'diff_count', path_ + f, 'gene')
        if event['data'] == 'exp_diff':
            # set output
            self.move2outputdir(obj.output_dir, 'Express')
            # set api
            clust_path = os.path.join(self.output_dir, '/Express/cluster/hclust/')
            clust_files = os.listdir(clust_path)
            clust_params = {
                # diff_fpkm: ,
                'log': 10,
                'methor': 'hclust',
                'distance': 'euclidean',
                'sub_num': 5,
            }
            clust_id = api_express.add_cluster(clust_params, self.diff_gene_id, clust_path + 'samples_tree.txt', clust_path + 'genes_tree.txt', clust_path + 'hclust_heatmap.xls')
            for f in clust_files:
                if re.search(r'^subcluster_', f):
                    sub = f.split('_')[1]
                    api_express.add_cluster_detail(clust_id, sub, clust_path + f)
            net_path = os.path.join(self.output_dir + '/Express/network/')
            net_files = os.listdir(net_path)
            net_param = {
                # diff_fpkm: ,
                'softpower': 9,
                'similar': 0.75,
            }
            net_id = api_express.add_network(net_param, self.diff_gene_id, net_path + 'softPower.pdf', net_path + 'ModuleTree.pdf')
            api_express.add_network_detail(net_id, net_path + 'all_nodes.txt', net_path + 'all_edges.txt')
            for f in net_files:
                if re.search(r'^CytoscapeInput-edges-', f):
                    color = f.split('-edges-')[-1].split('.')[0]
                    api_express.add_network_module(net_id, net_path + 'f', color)
        if event['data'] == 'annotation':
            self.move2outputdir(obj.output_dir, 'Annotation')

    def run(self):
        self.filecheck.on('end', self.run_qc)
        self.filecheck.on('end', self.run_qc_stat, False)  # 质控前统计
        self.qc.on('end', self.run_qc_stat, True)  # 质控后统计
        self.qc.on('end', self.run_assemble)
        self.assemble.on('end', self.run_orf)
        self.assemble.on('end', self.run_exp_stat)
        self.orf.on('end', self.run_orf_len)
        self.final_tools.append(self.orf_len)
        if self.option('anno_analysis'):
            self.assemble.on('end', self.run_annotation)
            self.final_tools.append(self.annotation)
        self.on_rely([self.orf, self.exp_stat], self.run_map_qc)
        self.final_tools.append(self.map_qc)
        if 'ssr' in self.option('gene_analysis'):
            self.orf.on('end', self.run_ssr)
            self.final_tools.append(self.ssr)
        if 'snp' in self.option('gene_analysis'):
            self.assemble.on('end', self.run_bwa)
            self.on_rely([self.bwa, self.orf], self.run_snp)
            self.final_tools.append(self.snp)
        if self.option('exp_analysis'):
            if ('go_rich' or 'kegg_rich') in self.option('exp_analysis'):
                self.on_rely([self.exp_stat, self.annotation], self.run_exp_diff)
            else:
                self.exp_stat.on('end', self.run_exp_diff)
        if len(self.final_tools) == 0:
            self.on_rely([self.orf_len, self.exp_stat], self.end)
        elif len(self.final_tools) == 1:
            self.final_tools[0].on('end', self.end)
        else:
            self.on_rely(self.final_tools, self.end)
        self.run_filecheck()
        super(DenovoBaseWorkflow, self).run()

    def end(self):
        self.send_files()
        super(DenovoBaseWorkflow, self).end()

    def send_files(self):
        self.logger.info('denovo_base upload files start')
        repaths = [
            ['.', "文件夹", "denovo rna 结果文件目录"],
            ["QC_stat", "文件夹", "样本数据统计文件目录"],
            ['QC_stat/before_qc', "文件夹", "质控前的样本数据统计文件目录"],
            ["QC_stat/before_qc/qualityStat/", "文件夹", "每个样本的fastq的质量统计文件的输出目录"],
            ["QC_stat/before_qc/fastq_stat.xls", "xls", "所有样本的fastq信息统计表"],
            ['QC_stat/after_qc', "文件夹", "质控后的样本数据统计文件目录"],
            ["QC_stat/after_qc/qualityStat/", "文件夹", "质控后的每个样本的fastq的质量统计文件的输出目录"],
            ["QC_stat/after_qc/fastq_stat.xls", "xls", "质控后的所有样本的fastq信息统计表"],
            ["QC_stat/after_qc/dup.xls", "xls", "所有样本的fastq序列重复统计表"],
            ["QC_stat/sickle_dir/", "文件夹", "质量剪切后的fastq文件输出目录"],
            ['Assemble', "文件夹", "Trinity拼接组装统计结果目录"],
            ['Assemble/transcript.iso.txt', "txt", "Trinity.fasta可变剪接体统计文件"],
            ['Assemble/trinity.fasta.stat.xls', "xls", "Trinity.fasta序列相关信息统计文件"],
            ['Gene_structure', "文件夹", "基因结构分析结果目录"],
            ['Gene_structure/snp', "文件夹", "snp分析结果目录"],
            ['Gene_structure/orf', "文件夹", "orf分析结果目录"],
            ["Gene_structure/orf/reads_len_info", "文件夹", "orf序列长度分布信息文件夹"],
            ['Gene_structure/ssr', "文件夹", "orf分析结果目录"],
            ["Gene_structure/ssr/misa_stat.xls", "xls", "ssr类型统计表"],
            ['Map_stat', "文件夹", "Mapping后质量统计结果目录"],
            ["Map_stat/coverage/", "文件夹", "基因覆盖度分析输出目录"],
            ["Map_stat/sorted_bam/", "文件夹", "每个样本排序后的bam文件输出目录"],
            ["Map_stat/dup/", "文件夹", "冗余序列分析输出目录"],
            ["Map_stat/satur/", "文件夹", "测序饱和度分析输出目录"],
            ["Map_stat/bam_stat.xls", "xls", "bam格式比对结果统计表"],
            ['Express/', "文件夹", "表达量分析结果目录"],
            ['Express/diff_exp', "文件夹", "表达量差异检测分析结果目录"],
            ['Express/rsem', "文件夹", "表达量计算分析结果目录"],
            ['Express/correlation', "文件夹", "表达量样本相关性分析结果目录"],
            ["Express/correlation/correlation_matrix.xls", "xls", "相关系数矩阵表"],
            ["Express/correlation/hcluster_tree_correlation_matrix.xls_average.tre", "xls", "相关系数树文件"]
        ]
        regexps = [
            [r'Assemble/.*_length\.distribut\.txt$', 'txt', '长度分布信息统计文件'],
            [r"Gene_structure/snp/.*snp_position_stat\.xls", "xls", "样本snp编码位置信息统计表"],
            [r"Gene_structure/snp/.*snp_type_stat\.xls", "xls", "样本snp类型统计表"],
            [r"Gene_structure/snp/.*snp\.xls", "xls", "样本snp信息表"],
            [r"Gene_structure/ssr/.*misa$", "misa", "ssr结果"],
            [r"Gene_structure/orf/reads_len_info/.*reads_len_info\.txt$", "xls", "orf序列长度分布信息文件"],
            [r"Gene_structure/orf/transdecoder.pep$", "fasta", "蛋白质序列文件"],
            [r"Gene_structure/orf/transdecoder.cds$", "fasta", "cds序列文件"],
            [r"Gene_structure/orf/transdecoder.bed$", "bed", "orf位置信息bed格式文件"],
            [r"Map_stat/dup/.*pos\.DupRate\.xls", "xls", "比对到基因组的序列的冗余统计表"],
            [r"Map_stat/dup/.*seq\.DupRate\.xls", "xls", "所有序列的冗余统计表"],
            [r"Map_stat/satur/.*eRPKM\.xls", "xls", "RPKM表"],
            [r"Map_stat/coverage/.*cluster_percent\.xls", "xls", "饱和度作图数据"],
            [r"Express/diff_exp/.*_edgr_stat\.xls$", "xls", "edger统计结果文件"],
            [r"Express/rsem/.*results$", "xls", "单样本rsem分析结果表"],
            [r"Express/rsem/.*matrix$", "xls", "表达量矩阵"]
        ]
        if self.option("search_pfam") is True:
            repaths += [["Gene_structure/orf/pfam_domain", "", "Pfam比对蛋白域结果信息"]]
        if self.option('fq_type') == 'SE':
            repaths += [
                ['QC_stat/clip_dir', "文件夹", "SE去接头后的fastq文件输出目录"]
            ]
        else:
            repaths += [
                ["QC_stat/seqprep_dir/", "文件夹", "PE去接头后fastq文件输出目录"]
            ]
        if self.exp_stat.diff_gene:
            regexps += [
                [r"Express/cluster/hclust/subcluster_", "xls", "子聚类热图数据"],
                [r"Express/network/CytoscapeInput.*", "txt", "Cytoscape作图数据"]
            ]
            repaths += [
                ["Express/diff_exp/diff_fpkm", "xls", "差异基因表达量表"],
                ["Express/diff_exp/diff_count", "xls", "差异基因计数表"],
                ['Express/network', "文件夹", "差异基因网络共表达分析结果目录"],
                ["Express/network/all_edges.txt", "txt", "edges结果信息"],
                ["Express/network/all_nodes.txt ", "txt", "nodes结果信息"],
                ["Express/network/removeGene.xls ", "xls", "移除的基因信息"],
                ["Express/network/removeSample.xls ", "xls", "移除的样本信息"],
                ["Express/network/softPower.pdf", "pdf", "softpower相关信息"],
                ["Express/network/ModuleTree.pdf", "pdf", "ModuleTree图"],
                ["Express/network/eigengeneClustering.pdf", "pdf", "eigengeneClustering图"],
                ["Express/network/eigenGeneHeatmap.pdf", "pdf", "eigenGeneHeatmap图"],
                ["Express/network/networkHeatmap.pdf", "pdf", "networkHeatmap图"],
                ["Express/network/sampleClustering.pdf", "pdf", "sampleClustering图"],
                ["Express/cluster", "文件夹", "差异基因聚类分析分析结果目录"],
                ["Express/cluster/hclust/", "", "层级聚类分析结果目录"],
                ["Express/cluster/hclust/hc_gene_order", "txt", "按基因聚类的基因排序列表"],
                ["Express/cluster/hclust/hc_sample_order", "txt", "按样本聚类的样本排序列表"],
                ["Express/cluster/hclust/hclust_heatmap.xls", "xls", "层级聚类热图数据"]
            ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        for i in self.get_upload_files():
            self.logger.info('upload file:{}'.format(str(i)))
        self.logger.info('denovo_base upload files end')
