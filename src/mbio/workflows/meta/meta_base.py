# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""多样性基础分析"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import json
import shutil


class MetaBaseWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        """
        self._sheet = wsheet_object
        super(MetaBaseWorkflow, self).__init__(wsheet_object)
        options = [
            {'name': 'in_fastq', 'type': 'infile', 'format': 'sequence.fastq,sequence.fastq_dir'},  # 输入的fastq文件或fastq文件夹
            {'name': 'otu_fasta', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出的合并到一起的fasta，供后续的otu分析用
            {'name': 'identity', 'type': 'float', 'default': 0.97},  # 相似性值，范围0-1.
            {'name': 'otu_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出结果otu表
            {'name': 'otu_rep', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出结果otu代表序列
            # {'name': 'otu_seqids', 'type': 'outfile', 'format': 'meta.otu.otu_seqids'},  # 输出结果otu中包含序列列表
            {'name': 'otu_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 输出结果biom格式otu表
            {'name': 'revcomp', 'type': 'bool', 'default': False},  # 序列是否翻转
            {'name': 'confidence', 'type': 'float', 'default': 0.7},  # 置信度值
            # {"name": "customer_mode", "type": "bool", "default": False},  # customer 自定义数据库
            {'name': 'database', 'type': 'string'},  # 数据库选择
            {'name': 'ref_fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 参考fasta序列
            {'name': 'ref_taxon', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 参考taxon文件
            {'name': 'taxon_file', 'type': 'outfile', 'format': 'taxon.seq_taxon'},  # 输出序列的分类信息文件
            {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'meta.otu.tax_summary_dir'},  # 输出的otu_taxon_dir文件夹
            {"name": "estimate_indices", "type": "string", "default": "ace,chao,shannon,simpson,coverage"},
            {"name": "rarefy_indices", "type": "string", "default": "sobs,shannon"},  # 指数类型
            {"name": "rarefy_freq", "type": "int", "default": 100},
            {"name": "alpha_level", "type": "string", "default": "otu"},  # level水平
            {"name": "beta_analysis", "type": "string", "default": "pca,hcluster"},
            {"name": "beta_level", "type": "string", "default": "otu"},
            {"name": "dis_method", "type": "string", "default": "bray_curtis"},
            # {"name": "phy_newick", "type": "infile", "format": "meta.beta_diversity.newick_tree"},
            {"name": "permutations", "type": "int", "default": 999},
            {"name": "linkage", "type": "string", "default": "average"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "anosim_grouplab", "type": 'string', "default": ''},
            {"name": "plsda_grouplab", "type": 'string', "default": ''},
            {"name": "file_list", "type": "string", "default": "null"},
            {"name": "raw_sequence", "type": "infile", "format": "sequence.raw_sequence_txt"},
            {"name": "workdir_sample", "type": "string", "default": ""},
            {"name": "if_fungene", "type": "bool", 'default': False}    
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.sample_rename = self.add_tool("meta.sample_rename")
        self.new_sample_extract = self.add_module("meta.sample_extract.sample_extract")  # added by shijin
        self.sample_check = self.add_tool("meta.sample_check")
        # self.info_abstract = self.add_tool("meta.lala.info_abstract")
        self.otu = self.add_tool("meta.otu.usearch_otu")
        self.phylo = self.add_tool("phylo.phylo_tree")
        self.tax = self.add_tool("taxon.qiime_assign")
        self.stat = self.add_tool("meta.otu.otu_taxon_stat")
        self.alpha = self.add_module("meta.alpha_diversity.alpha_diversity")
        self.beta = self.add_module("meta.beta_diversity.beta_diversity")
        self.pan_core = self.add_tool("meta.otu.pan_core_otu")
        self.step.add_steps("sample_rename", "otucluster", "taxassign", "alphadiv", "betadiv")
        self.spname_spid = dict()
        self.otu_id = None
        self.env_id = None
        self.level_dict = {'Domain': 1, 'Kingdom': 2, 'Phylum': 3, 'Class': 4, 'Order': 5, 'Family': 6, 'Genus': 7, 'Species': 8, 'otu': 9}
        self.updata_status_api = self.api.meta_update_status
        self.info_path = ""
        self.work_dir_path = ""
        self.function_gene_tool = None
        self.function_gene_path = ""
        self.in_fastq_path = ""

    def check_options(self):
        """
        检查参数设置
        """
        # if not self.option("fasta").is_set:
        #     raise OptionError("必须设置输入fasta文件.")
        if self.option("identity") < 0 or self.option("identity") > 1:
            raise OptionError("identity值必须在0-1范围内.")
        if self.option("revcomp") not in [True, False]:
            raise OptionError("必须设置序列是否翻转")
        if self.option('database') == "custom_mode":
            if not self.option("ref_fasta").is_set or not self.option("ref_taxon").is_set:
                raise OptionError("数据库自定义模式必须设置参考fasta序列和参考taxon文件")
        else:
            if self.option("database") not in ['silva123/16s_bacteria', 'silva123/16s_archaea',
                                               'silva123/16s', 'silva123/18s_eukaryota', 'silva123',
                                               'silva119/16s_bacteria', 'silva119/16s_archaea',
                                               'silva119/16s', 'silva119/18s_eukaryota', 'unite7.0/its_fungi',
                                               'fgr/amoA', 'fgr/nosZ', 'fgr/nirK', 'fgr/nirS',
                                               'fgr/nifH', 'fgr/pmoA', 'fgr/mmoX', 'fgr/mcrA',
                                               'fgr/amoA_archaea', 'fgr/amoA_bacteria',
                                               'maarjam081/AM', 'Human_HOMD',
                                               'silva128/16s_archaea', 'silva128/16s_bacteria',
                                               'silva128/18s_eukaryota', 'silva128/16s',
                                               'greengenes135/16s', 'greengenes135/16s_archaea', 'greengenes135/16s_bacteria']:
                    # add by wzy 2016.11.14 silva128,2016.11.23 mrcA,2016.11.28,greengenes135,20170424,amoA(a,b)
                raise OptionError("数据库{}不被支持".format(self.option("database")))
        # add by qindanhua 20170112 check if function gene is exist
        if self.option("if_fungene") and self.option("database").split("/")[0] != "fgr":
            raise OptionError("不支持{}功能基因".format(self.option("database").split("/")[0]))
        return True

    # add by qindanhua run function gene tool
    def run_function_gene(self):
        self.step.add_steps("function_gene")
        self.function_gene_tool = self.add_tool("meta.function_gene.function_gene")
        tax_name = self.option("database").split("/")[1]
        if tax_name == 'amoA_archaea':
            tax_name = 'amoA_AOA'
        elif tax_name == 'amoA_bacteria':
            tax_name = 'amoA_AOB'
        self.function_gene_tool.set_options({
            "fasta": self.sample_check.option("otu_fasta"),
            "function_gene": tax_name,
        })
        self.function_gene_tool.on("start", self.set_step, {'start': self.step.function_gene})
        self.function_gene_tool.on("end", self.run_otu)
        self.function_gene_tool.on("end", self.set_step, {'end': self.step.function_gene})
        self.function_gene_tool.run()

    def run_pre_sample_extract(self):
        # if self.option("if_fungene"):
        #     self.in_fastq_path = self.function_gene_tool.output_dir + "/fungene_reads.fastq"
        opts = {
                "in_fastq":  self.option("in_fastq")  # modified by shijin
            }
        self.new_sample_extract.set_options(opts)
        self.new_sample_extract.on("start", self.set_step, {'start': self.step.sample_rename})
        # self.new_sample_extract.on("end", self.set_step, {'end': self.step.pre_sample_extract})
        self.new_sample_extract.run()

    def run_sample_rename(self):
        opts = {
            # "workdir_sample": self.option("workdir_sample"),  # 从数据库中提取到的样本工作路径，以便对其进行操作
            "info_txt": self.new_sample_extract.work_dir + "/info.txt",
            "file_list": self.option("file_list")  # 对样本进行重命名
        }
        self.sample_rename.set_options(opts)
        # self.sample_rename.on("start", self.set_step, {'start': self.step.sample_rename})
        self.sample_rename.run()

    def run_samplecheck(self):  # 样本合并
        opts = {
            "file_sample_list": self.sample_rename.option("file_sample_list")
        }
        if self.option("raw_sequence").is_set:
            opts.update({
                "raw_sequence": self.option("raw_sequence")
            })
        self.sample_check.set_options(opts)
        self.sample_check.on("end", self.set_output, "sample_check")
        self.sample_check.run()

    def run_otu(self):
        if self.option("if_fungene"):
            self.in_fasta_path = self.function_gene_tool.output_dir + "/fungene_reads.fasta"
        opts = {
            "fasta": self.in_fasta_path if self.option("if_fungene") else self.sample_check.option("otu_fasta"),
            # modified by shijin on 20170428
            "identity": self.option("identity")
        }
        self.otu.set_options(opts)
        self.otu.on("end", self.set_output, "otu")
        self.otu.on("start", self.set_step, {'end': self.step.sample_rename, 'start': self.step.otucluster})
        # self.otu.on("end", self.set_step, {'end':self.step.otucluster})
        self.otu.run()

    def run_phylotree(self):
        self.phylo.set_options({
            "fasta_file": self.otu.output_dir + "/otu_reps.fasta"
        })
        # self.phylo.on("start", self.set_step, {'end':self.step.otucluster, 'start':self.step.phylotree})
        self.phylo.on("end", self.set_step, {'end': self.step.otucluster})
        self.phylo.run()

    def run_taxon(self):
        opts = {
            "fasta": self.otu.option("otu_rep"),
            "revcomp": self.option("revcomp"),
            "confidence": self.option("confidence"),
            "database": self.option("database")}

        if self.option("database") == "custom_mode":
            opts.update({
                "ref_fasta": self.option("ref_fasta"),
                "ref_taxon": self.option("ref_taxon")
            })
        self.tax.set_options(opts)
        self.tax.on("end", self.set_output, "tax")
        self.tax.on("start", self.set_step, {'start': self.step.taxassign})
        self.tax.on("end", self.set_step, {'end': self.step.taxassign})
        self.tax.run()

    def run_stat(self):
        self.stat.set_options({
            "in_otu_table": self.otu.option("otu_table"),
            "taxon_file": self.tax.option("taxon_file")
        })
        self.stat.on("end", self.set_output, "stat")
        # self.stat.on("end", self.set_step, {'end': self.step.taxassign})
        self.stat.run()

    def run_alpha(self):
        self.alpha.set_options({
            'otu_table': self.stat.option('otu_taxon_dir'),
            "level": self.option('alpha_level'),
            'estimate_indices': self.option('estimate_indices'),
            'rarefy_indices': self.option('rarefy_indices'),
            'rarefy_freq': self.option('rarefy_freq')
        })
        self.alpha.on("end", self.set_output, "alpha")
        self.alpha.on("start", self.set_step, {'start': self.step.alphadiv})
        self.alpha.on("end", self.set_step, {'end': self.step.alphadiv})
        self.alpha.run()

    def run_beta(self):
        opts = {
            'analysis': 'distance,' + self.option('beta_analysis'),
            'dis_method': self.option('dis_method'),
            'otutable': self.stat.option('otu_taxon_dir'),
            "level": self.option('beta_level'),
            'permutations': self.option('permutations')
        }
        if self.count_otus:
            opts['phy_newick'] = self.phylo.option('phylo_tre').prop['path']
        if self.option('envtable').is_set:
            opts.update({
                'envtable': self.option('envtable')
            })
        if self.option('group').is_set:
            opts.update({
                'group': self.option('group')
            })
        if 'anosim' in self.option('beta_analysis').split(','):
            opts.update({
                'anosim_grouplab': self.option('anosim_grouplab')
            })
        if 'plsda' in self.option('beta_analysis').split(','):
            opts.update({
                'plsda_grouplab': self.option('plsda_grouplab')
            })
        self.beta.set_options(opts)
        self.beta.on("end", self.set_output, "beta")
        self.beta.on("start", self.set_step, {'start': self.step.betadiv})
        self.beta.on("end", self.set_step, {'end': self.step.betadiv})
        self.beta.run()

    def run_pan_core(self):
        opts = {
            "in_otu_table": self.stat.option("otu_taxon_dir")
        }
        self.pan_core.set_options(opts)
        self.pan_core.on("end", self.set_output, "pan_core")
        self.pan_core.run()

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def move2outputdir(self, olddir, newname, mode='link'):  # add by shenghe 20160329
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下，如果文件夹名已存在，文件夹会被完整删除。
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if os.path.exists(newdir):
            if os.path.islink(newdir):
                os.remove(newdir)
            else:
                shutil.rmtree(newdir)  # 不可以删除一个链接
        if mode == 'link':
            # os.symlink(os.path.abspath(olddir), newdir)  # 原始路径需要时绝对路径
            shutil.copytree(olddir, newdir, symlinks=True)
        elif mode == 'copy':
            shutil.copytree(olddir, newdir)
        else:
            raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')

    def set_output(self, event):
        obj = event["bind_object"]
        # 设置QC报告文件
        if event['data'] == "sample_check":
            # self.option("otu_fasta", obj.option("otu_fasta"))
            self.move2outputdir(obj.output_dir, self.output_dir + "/QC_stat")  # 代替cp
            # os.system('cp -r ' + obj.output_dir + ' ' + self.output_dir + "/QC_stat")
            api_samples = self.api.sample
            sample_info_path = self.sample_check.output_dir + "/samples_info/samples_info.txt"
            if not os.path.isfile(sample_info_path):
                raise Exception("找不到报告文件:{}".format(sample_info_path))
            api_samples.add_samples_info(sample_info_path)
            self.spname_spid = api_samples.get_spname_spid()
            """
            base_info_path = ""

            with open(self.qc.output_dir + "/samples_info/samples_info.txt") as f:
                f.readline()
                for line in f:
                    s = line.split('\t')[0]
                    base_info_path = self.qc.output_dir + "/base_info/{}.fastq.fastxstat.txt".format(s)
                    if not os.path.isfile(base_info_path):
                        raise Exception("找不到报告文件:{}".format(base_info_path))
                    api_samples.add_base_info(s, base_info_path)
            """
            for step in (20, 50, 100, 200):
                reads_len_info_path = self.sample_check.output_dir + "/reads_len_info/step_{}.reads_len_info.txt".format(str(step))
                if not os.path.isfile(reads_len_info_path):
                    raise Exception("找不到报告文件")
                api_samples.add_reads_len_info(step, reads_len_info_path)
            if self.option('envtable').is_set:
                api_env = self.api.env
                self.env_id = str(api_env.add_env_table(self.option('envtable').prop["path"], self.spname_spid))
            """
            原始序列信息提取新 by sj
            """
            if self.option('raw_sequence').is_set:
                raw_sequence_path = self.sample_check.output_dir + "/raw_sequence.txt"
                if not os.path.exists(raw_sequence_path):
                    raise Exception("找不到报告文件:{}".format(raw_sequence_path))
                api_samples.add_raw_sequence_info(raw_sequence_path)
            valid_sequence_path = self.sample_check.output_dir + "/valid_sequence.txt"
            if not os.path.exists(valid_sequence_path):
                raise Exception("找不到报告文件:{}".format(valid_sequence_path))
            api_samples.add_valid_sequence_info(valid_sequence_path)
        # 设置OTU table文件
        if event['data'] == "otu":
            self.option("otu_table", obj.option("otu_table"))
            self.option("otu_rep", obj.option("otu_rep"))
            self.option("otu_biom", obj.option("otu_biom"))
            self.move2outputdir(obj.output_dir, self.output_dir + "/Otu")  # 代替cp
            # os.system('cp -r ' + obj.output_dir + ' ' + self.output_dir + "/Otu")
            # 设置进化树文件
        if event['data'] == "tax":
            self.option("taxon_file", obj.option("taxon_file"))
            self.move2outputdir(obj.output_dir, self.output_dir + "/Tax_assign")  # 代替cp
            # os.system('cp -r ' + obj.output_dir + ' ' + self.output_dir + "/Tax_assign")
        if event['data'] == "stat":
            # self.option("otu_taxon_biom", obj.option("otu_taxon_biom"))
            # self.option("otu_taxon_table", obj.option("otu_taxon_table"))
            self.option("otu_taxon_dir", obj.option("otu_taxon_dir"))
            self.move2outputdir(obj.output_dir, self.output_dir + "/OtuTaxon_summary")  # 代替cp
            # os.system('cp -r ' + obj.output_dir + ' ' + self.output_dir + "/OtuTaxon_summary")
            api_otu = self.api.meta
            otu_path = self.output_dir + "/OtuTaxon_summary/otu_taxon.xls"
            rep_path = self.output_dir + "/Otu/otu_reps.fasta"
            if not os.path.isfile(otu_path):
                raise Exception("找不到报告文件:{}".format(otu_path))
            params = {
                "group_id": 'all',
                # "size": 0,
                "size": "",   # modified by hongdongxuan 20170303
                "submit_location": 'otu_statistic',
                "filter_json": "[]",  # add by hongdongxuan 20170303
                "task_type": 'reportTask'
            }
            self.otu_id = api_otu.add_otu_table(otu_path, major=True, rep_path=rep_path, spname_spid=self.spname_spid, params=params)
            self.updata_status_api.add_meta_status(table_id=str(self.otu_id), type_name='sg_otu')
            api_otu_level = self.api.sub_sample
            api_otu_level.add_sg_otu_detail_level(otu_path, self.otu_id, 9)
            # self.otu_id = str(self.otu_id)
            # self.logger.info('OTU mongo ID:%s' % self.otu_id)
            if self.count_otus:
                api_tree = self.api.newicktree
                tree_path = self.phylo.option('phylo_tre').prop['path']
                if not os.path.isfile(tree_path):
                    raise Exception("找不到报告文件:{}".format(tree_path))
                if os.path.exists(self.output_dir + '/Otu/otu_phylo.tre'):
                    os.remove(self.output_dir + '/Otu/otu_phylo.tre')
                os.link(tree_path, self.output_dir + '/Otu/otu_phylo.tre')
                api_tree.add_tree_file(tree_path, major=True, level=9, table_id=str(self.otu_id), table_type='otu', tree_type='phylo')
            if self.option('group').is_set:
                api_group = self.api.group
                api_group.add_ini_group_table(self.option('group').prop["path"], self.spname_spid, sort_samples=True)
        if event['data'] == "alpha":
            self.move2outputdir(obj.output_dir, self.output_dir + "/Alpha_diversity")  # 代替cp
            # os.system('cp -r ' + obj.output_dir + ' ' + self.output_dir + "/Alpha_diversity")
            # 设置alpha多样性文件
            api_est = self.api.estimator
            est_path = self.output_dir + "/Alpha_diversity/Estimators/estimators.xls"
            if not os.path.isfile(est_path):
                raise Exception("找不到报告文件:{}".format(est_path))
            indice = sorted(self.option("estimate_indices").split(','))
            level_id = self.level_dict[self.option('alpha_level')]
            params = {
                # "otu_id": str(self.otu_id),  # 在metabase中不能执行，生成self.otu_id的api可能会被截取
                "level_id": level_id,
                "index_type": ','.join(indice),
                'submit_location': 'alpha_diversity_index',
                'task_type': 'reportTask',
                'group_id': 'all'
            }
            est_id = api_est.add_est_table(est_path, major=True, level=level_id, otu_id=str(self.otu_id),
                                           params=params, spname_spid=self.spname_spid)
            self.updata_status_api.add_meta_status(table_id=str(est_id), type_name='sg_alpha_diversity')
            # 主表写入没有加name，所以此处table_name固定
            api_rare = self.api.rarefaction
            rare_path = self.work_dir + "/AlphaDiversity/Rarefaction/output/"
            indice = sorted(self.option("rarefy_indices").split(','))
            params = {
                # "otu_id": str(self.otu_id),  # 在metabase中不能执行，生成self.otu_id的api可能会被截取
                "level_id": level_id,
                "index_type": ','.join(indice),
                'freq': self.option('rarefy_freq'),
                'submit_location': 'alpha_rarefaction_curve',
                'task_type': 'reportTask',
                'group_id': 'all'
            }
            rare_id = api_rare.add_rare_table(rare_path, level=level_id, otu_id=str(self.otu_id),
                                              params=params, spname_spid=self.spname_spid)
            self.updata_status_api.add_meta_status(table_id=str(rare_id), type_name='sg_alpha_rarefaction_curve')
            # 主表写入没有加name，所以此处table_name固定
        if event['data'] == "beta":
            self.move2outputdir(obj.output_dir, self.output_dir + "/Beta_diversity", mode='copy')  # 代替cp
            # 设置beta多样性文件
            api_dist = self.api.distance
            dist_path = self.beta.option('dis_matrix').prop['path']
            if not os.path.isfile(dist_path):
                raise Exception("找不到报告文件:{}".format(dist_path))
            level_id = self.level_dict[self.option('beta_level')]
            params = {
                # 'otu_id': str(self.otu_id),  # 在metabase中不能执行，生成self.otu_id的api可能会被截取
                'level_id': level_id,
                'distance_algorithm': self.option('dis_method'),
                'submit_location': 'beta_sample_distance_hcluster_tree',  # 为前端分析类型标识
                'task_type': 'reportTask',
                'hucluster_method': self.option('linkage'),
                'group_id': 'all'
            }
            dist_id = api_dist.add_dist_table(dist_path, level=level_id, otu_id=self.otu_id, major=True, params=params, spname_spid=self.spname_spid)
            # self.updata_status_api.add_meta_status(table_id=str(dist_id), type_name='sg_beta_specimen_distance')  # 主表写入没有加name，所以此处table_name固定
            if 'hcluster' in self.option('beta_analysis').split(','):
                # 设置hcluster树文件
                api_hcluster = self.api.newicktree
                hcluster_path = self.beta.output_dir + "/Hcluster/hcluster.tre"
                if not os.path.isfile(hcluster_path):
                    raise Exception("找不到报告文件:{}".format(hcluster_path))
                tree_id = api_hcluster.add_tree_file(hcluster_path, major=True, table_id=str(self.otu_id), level=level_id,
                                                     table_type='otu', tree_type='cluster', params=params,
                                                     spname_spid=self.spname_spid, update_dist_id=dist_id)
                self.updata_status_api.add_meta_status(table_id=str(tree_id), type_name='sg_newick_tree')  # 主表写入没有加name，所以此处table_name固定
            beta_multi_analysis_dict = {'pca': 'beta_multi_analysis_pca', 'pcoa': 'beta_multi_analysis_pcoa',
                                        'nmds': 'beta_multi_analysis_nmds', 'dbrda': 'beta_multi_analysis_dbrda',
                                        'rda_cca': 'beta_multi_analysis_rda_cca'}  # 为前端分析类型标识
            for ana in self.option('beta_analysis').split(','):
                if ana in ['pca', 'pcoa', 'nmds', 'dbrda', 'rda_cca']:
                    api_betam = self.api.beta_multi_analysis
                    params = {
                        # 'otu_id': str(self.otu_id),  # 在metabase中不能执行，生成self.otu_id的api可能会被截取
                        'level_id': level_id,
                        'analysis_type': ana,
                        'submit_location': beta_multi_analysis_dict[ana],
                        'task_type': 'reportTask'
                    }
                    if self.option('envtable').is_set:
                        # params['env_id'] = str(self.env_id)  # 在metabase中不能执行，生成self.env_id的api可能会被截取
                        params['env_labs'] = ','.join(self.option('envtable').prop['group_scheme'])
                    if ana in ['pcoa', 'nmds', 'dbrda']:
                        params['distance_algorithm'] = self.option('dis_method')
                    main_id = api_betam.add_beta_multi_analysis_result(dir_path=self.beta.output_dir, analysis=ana,
                                                                       main=True, env_id=self.env_id,
                                                                       otu_id=self.otu_id, params=params,
                                                                       spname_spid=self.spname_spid)
                    self.updata_status_api.add_meta_status(table_id=main_id, type_name='sg_beta_multi_analysis')  # 主表写入没有加name，所以此处table_name固定
                    self.logger.info('set output beta %s over.' % ana)
        if event['data'] == "pan_core":
            self.move2outputdir(obj.output_dir, self.output_dir + "/pan_core")
            api_pan_core = self.api.pan_core
            name = "Pan_Origin"
            params = {
                "level_id": 9,
                "group_id": "all",
                "submit_location": "otu_pan_core"
            }
            pan_id = api_pan_core.create_pan_core_table(1, json.dumps(params), "all", 9, self.otu_id, name, "end", spname_spid=self.spname_spid)
            name = "Core_Origin"
            core_id = api_pan_core.create_pan_core_table(2, json.dumps(params), "all", 9, self.otu_id, name, "end", spname_spid=self.spname_spid)
            pan_path = self.pan_core.option("pan_otu_table").prop["path"]
            core_path = self.pan_core.option("core_otu_table").prop['path']
            api_pan_core.add_pan_core_detail(pan_path, pan_id)
            api_pan_core.add_pan_core_detail(core_path, core_id)
            self.updata_status_api.add_meta_status(table_id=pan_id, type_name='sg_otu_pan_core')
            self.updata_status_api.add_meta_status(table_id=core_id, type_name='sg_otu_pan_core')

    def check_otu_run(self):
        """
        在不同的OTU数目以及不同的样本数量下，有些分析会被跳过不做
        """
        self.update_info = ""
        self.count_otus = True  # otu/代表序列数量大于3 ,change by wzy from 2 to 3
        self.count_samples = 0  # 样本数量是否大于等于2
        counts = 0
        for i in open(self.otu.output_dir + '/otu_reps.fasta'):
            if i[0] == '>':
                counts += 1
                if counts > 3:    # change from 2 to 3 by wzy
                    break
        else:
            self.count_otus = False
        with open(self.sample_check.output_dir + "/samples_info/samples_info.txt") as r:
            self.count_samples = len(r.readlines()) - 1
        if self.count_samples < 3:  # 少于3个样本
            self.update_info += "样本少于三个，不进行beta多样性相关分析；"
            self.option('beta_analysis', '')
        if not self.count_otus:
            if 'pca' in self.option('beta_analysis'):
                self.option('beta_analysis', self.option('beta_analysis').replace('pca', '').strip(',').replace(',,', ','))
                self.update_info += "OTU数量过少，不做PCA分析；"
            if "unifrac" in self.option('dis_method'):
                self.option('dis_method', "bray_curtis")
                self.update_info += "OTU数量过少，不使用unifrac类型距离算法，采用默认bray_curtis算法；"
            indices = self.option("estimate_indices").split(',')
            if 'pd' in indices:
                indices.remove("pd")
                indices = ','.join(indices)
                self.option('estimate_indices', indices)
                self.update_info += "OTU数量过少没有进行进化树分析，所以移除了依赖进化树分析多样性指数 PD；"
        if self.count_otus and self.count_samples > 1:
            self.on_rely([self.tax, self.phylo], self.run_stat)
            self.stat.on('end', self.run_alpha)
            self.stat.on('end', self.run_beta)
            self.stat.on('end', self.run_pan_core)
            self.on_rely([self.alpha, self.beta, self.pan_core], self.end)
            self.run_taxon()
            self.run_phylotree()
        elif self.count_otus and self.count_samples == 1:
            self.update_info += "样本数量过少，不进行Pan Core分析；"
            self.on_rely([self.tax, self.phylo], self.run_stat)
            self.stat.on('end', self.run_alpha)
            self.stat.on('end', self.run_beta)
            self.on_rely([self.alpha, self.beta], self.end)
            self.run_taxon()
            self.run_phylotree()
        else:
            self.update_info += "OTU数量过少，不进行物种进化树分析，将不会生成物种进化树；"
            self.tax.on('end', self.run_stat)
            self.stat.on('end', self.run_alpha)
            self.stat.on('end', self.run_beta)
            self.on_rely([self.alpha, self.beta], self.end)
            self.run_taxon()
        if self.update_info:
            self.logger.info("分析结果异常处理：{}".format(self.update_info))
            self.step._info += self.update_info
            self.step._has_state_change = True

    def run(self):
        task_info = self.api.api('task_info.task_info')
        task_info.add_task_info()
        self.new_sample_extract.on("end", self.run_sample_rename)
        self.sample_rename.on("end", self.run_samplecheck)
        self.sample_check.on("end", self.run_otu)
        self.otu.on('end', self.check_otu_run)
        if self.option("if_fungene"):
            self.run_function_gene()
        else:
            self.run_pre_sample_extract()
        super(MetaBaseWorkflow, self).run()

    def send_files(self):
        repaths = [
            [".", "", "基础分析结果文件夹"],
            ["QC_stat", "", "样本数据统计文件目录"],
            ["QC_stat/samples_info", "", "样本信息文件目录"],  # add by hongdongxuan 20170323
            ["QC_stat/samples_info/samples_info.txt", "txt", "样本信息统计文件"],
            ["QC_stat/base_info", "", "单个样本碱基质量统计目录"],
            ["QC_stat/reads_len_info", "", "序列长度分布统计文件目录"],
            ["QC_stat/valid_sequence.txt", "txt", "优化序列信息统计表"],  # add by hongdongxuan 20170323
            ["Otu", "", "OTU聚类结果文件目录"],
            ["Tax_assign", "", "OTU对应物种分类文件目录"],
            ["Tax_assign/seqs_tax_assignments.txt", "taxon.seq_taxon", "OTU序列物种分类文件"],
            ["OtuTaxon_summary", "", "OTU物种分类综合统计目录"],
            ["OtuTaxon_summary/otu_taxon.biom", "meta.otu.biom", "biom格式的OTU物种分类统计表"],
            ["OtuTaxon_summary/otu_taxon.xls", "meta.otu.otu_table", "OTU物种分类统计表"],
            ["OtuTaxon_summary/tax_summary_a", "meta.otu.tax_summary_dir", "各分类学水平样本序列数统计表"],
            ["OtuTaxon_summary/tax_summary_r", "meta.otu.tax_summary_dir", "各分类学水平样本序列数相对丰度百分比统计表"],  # add by zhouxuan 20161129
            ["Alpha_diversity", "", "Alpha diversity文件目录"],
            ["Alpha_diversity/Estimators", "", "多样性指数分析文件目录"],  # add by guhaidong 20171018
            ["Alpha_diversity/Estimators/estimators.xls", "xls", "Alpha多样性指数表"],
            ["Alpha_diversity/Rarefaction", "", "稀释曲线分析文件目录"],  # add by guhaidong 20171018
            ["Beta_diversity", "", "Beta diversity文件目录"],
            ["Beta_diversity/Anosim", "", "ANOSIM&Adonis分析结果目录"],
            ["Beta_diversity/Anosim/anosim_results.txt", "txt", "anosim分析结果"],
            ["Beta_diversity/Anosim/adonis_results.txt", "txt", "adonis分析结果"],
            ["Beta_diversity/Anosim/format_results.xls", "xls", "anosim&adonis综合统计表"],
            ["Beta_diversity/Dbrda", "", "db_RDA分析结果目录"],
            ["Beta_diversity/Dbrda/db_rda_sites.xls", "xls", "db_rda样本坐标表"],
            ["Beta_diversity/Dbrda/db_rda_species.xls", "xls", "db_rda物种坐标表"],
            ["Beta_diversity/Dbrda/db_rda_centroids.xls", "xls", "db_rda哑变量环境因子坐标表"],
            ["Beta_diversity/Dbrda/db_rda_biplot.xls", "xls", "db_rda数量型环境因子坐标表"],
            ["Beta_diversity/Box", "", "距离统计和统计检验分析结果目录"],
            ["Beta_diversity/Box/Stats.xls", "xls", "分组统计检验结果"],
            ["Beta_diversity/Box/Distances.xls", "xls", "组内组间距离值统计结果"],
            ["Beta_diversity/Distance", "", "距离矩阵计算结果目录"],
            ["Beta_diversity/Hcluster", "", "层次聚类结果目录"],
            ["Beta_diversity/Hcluster/hcluster.tre", "graph.newick_tree", "层次聚类树结果表"],
            ["Beta_diversity/Nmds", "", "NMDS分析结果目录"],
            ["Beta_diversity/Nmds/nmds_sites.xls", "xls", "样本各维度坐标"],
            ["Beta_diversity/Nmds/nmds_stress.xls", "xls", "样本特征拟合度值"],
            ["Beta_diversity/Pca", "", "PCA分析结果目录"],
            ["Beta_diversity/Pca/pca_importance.xls", "xls", "主成分解释度表"],
            ["Beta_diversity/Pca/pca_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Beta_diversity/Pca/pca_sites.xls", "xls", "样本各成分轴坐标"],
            ["Beta_diversity/Pca/pca_envfit_factor_scores.xls", "xls", "哑变量环境因子表"],
            ["Beta_diversity/Pca/pca_envfit_factor.xls", "xls", "哑变量环境因子坐标表"],
            ["Beta_diversity/Pca/pca_envfit_vector_scores.xls", "xls", "数量型环境因子表"],
            ["Beta_diversity/Pca/pca_envfit_vector.xls", "xls", "数量型环境因子坐标表"],
            ["Beta_diversity/Pcoa", "", "PCoA分析结果目录"],
            ["Beta_diversity/Pcoa/pcoa_eigenvalues.xls", "xls", "矩阵特征值"],
            ["Beta_diversity/Pcoa/pcoa_eigenvaluespre.xls", "xls", "特征解释度百分比"],
            ["Beta_diversity/Pcoa/pcoa_sites.xls", "xls", "样本坐标表"],
            ['Beta_diversity/Rda/dca.xls', 'xls', 'DCA分析结果'],
            ["Beta_diversity/Plsda", "", "PLS_DA分析结果目录"],
            ["Beta_diversity/Plsda/plsda_sites.xls", "xls", "样本坐标表"],
            ["Beta_diversity/Plsda/plsda_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Beta_diversity/Plsda/plsda_importance.xls", "xls", "主成分组别特征值表"],
            ["Beta_diversity/Plsda/plsda_importancepre.xls", "xls", "主成分解释度表"],
            ["Beta_diversity/Rda", "", "RDA_CCA分析结果目录"],
            ["pan_core", "", "Pan/core分析结果目录"],     # add 3 lines by hongdongxuan 20170323
            ["pan_core/core.richness.xls", "xls", "core 表格"],
            ["pan_core/pan.richness.xls", "xls", "pan 表格"]
        ]
        regexps = [
            [r"QC_stat/base_info/.*\.fastq\.fastxstat\.txt", "", "单个样本碱基质量统计文件"],
            [r"QC_stat/reads_len_info/step_\d+\.reads_len_info\.txt", "", "序列长度分布统计文件"],
            [r"Alpha_diversity/Estimators/otu\..*\.summary", "", "单个样本多样性指数表"],  # add by guhaidong 20171018
            [r'Beta_diversity/Distance/%s.*\.xls$' % self.option('dis_method'), 'meta.beta_diversity.distance_matrix', '样本距离矩阵文件'],
            [r'Beta_diversity/Rda/.+_importance\.xls$', 'xls', '主成分变化解释度表'],
            [r'Beta_diversity/Rda/.+_sites\.xls$', 'xls', '样本坐标表'],
            [r'Beta_diversity/Rda/.+_species\.xls$', 'xls', '物种坐标表'],
            [r'Beta_diversity/Rda/.+_biplot\.xls$', 'xls', '数量型环境因子坐标表'],
            [r'Beta_diversity/Rda/.+_centroids\.xls$', 'xls', '哑变量环境因子坐标表'],
            ["Otu/otu_reps.fasta", "sequence.fasta", "OTU代表序列"],
            ["Otu/otu_seqids.txt", "txt", "每个OTU中包含的序列编号列表"],
            ["Otu/otu_table.biom", 'meta.otu.biom', "OTU表对应的Biom文件"],
            ["Otu/otu_table.xls", "meta.otu.otu_table", "各样本OTU中序列数统计表"],
            ["Otu/otu_phylo.tre", "graph.newick_tree", "OTU代表序列进化树"],
            ["QC_stat/base_info/.*\.fastq\.fastxstat\.txt", "txt", "单个样本碱基质量统计文件"],
            ["QC_stat/reads_len_info/step_\d+\.reads_len_info\.txt", "txt", "序列长度分布统计文件"],
            ["OtuTaxon_summary/tax_summary_a/.+\.biom$", "meta.otu.biom", "OTU表的biom格式的文件(absolute)"],
            ["OtuTaxon_summary/tax_summary_a/.+\.xls$", "meta.otu.biom", "单级物种分类统计表(absolute)"],
            ["OtuTaxon_summary/tax_summary_a/.+\.full\.xls$", "meta.otu.biom", "多级物种分类统计表(absolute)"],
            ["OtuTaxon_summary/tax_summary_r/.+\.biom$", "meta.otu.biom", "OTU表的biom格式的文件"],  # add by zhouxuan (3 line) 20161129
            ["OtuTaxon_summary/tax_summary_r/.+\.xls$", "meta.otu.biom", "单级物种分类统计表"],
            ["OtuTaxon_summary/tax_summary_r/.+\.full\.xls$", "meta.otu.biom", "多级物种分类统计表"]
        ]
        for i in self.option("rarefy_indices").split(","):
            if i == "sobs":  # modified by hongdongxuan 20170324
                # repaths.append(["./rarefaction", "文件夹", "{}指数结果输出目录".format(i)])
                repaths.append(["./Alpha_diversity/Rarefaction/sobs", "文件夹", "{}指数结果输出目录".format(i)])
                # regexps.append([r".*rarefaction\.xls", "xls", "{}指数的simpleID的稀释性曲线表".format(i)])
                regexps.append([r".*rarefaction\.xls", "xls", "每个样本的{}指数稀释性曲线表".format(i)])
            else:
                # repaths.append(["./{}".format(i), "文件夹", "{}指数结果输出目录".format(i)])
                repaths.append(["./Alpha_diversity/Rarefaction/{}".format(i), "文件夹", "{}指数结果输出目录".format(i)])
                regexps.append(
                    # [r".*{}\.xls".format(i), "xls", "{}指数的simpleID的稀释性曲线表".format(i)])
                    [r".*{}\.xls".format(i), "xls", "每个样本的{}指数稀释性曲线表".format(i)])
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        # for i in self.get_upload_files():
        #     self.logger.info('upload file:{}'.format(str(i)))

    def end(self):
        self.send_files()
        super(MetaBaseWorkflow, self).end()
