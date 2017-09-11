# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
# last_modify:2017.08.22

from biocluster.module import Module
import os
import shutil
from biocluster.core.exceptions import OptionError


class UniGeneModule(Module):
    def __init__(self, work_id):
        super(UniGeneModule, self).__init__(work_id)
        options = [
            {"name": "gene_tmp_fa", "type": "infile", "format": "sequence.fasta"},  # 输出改名并合并的序列
            {"name": "number", "type": "int", "default": 0},  # 切分为几份，默认0表示按文件大小自动计算，指定某个整数时则按指定数量分割
            {"name": "uni_fasta", "type": "outfile", "format": "sequence.fasta"},  # 非冗余基因集核酸序列
            {"name": "uni_fastaa", "type": "outfile", "format": "sequence.fasta"},  # 非冗余基因集蛋白序列
            {"name": "cdhit_identity", "type": "float", "default": 0.95},  # 给出cdhit的参数identity
            {"name": "cdhit_coverage", "type": "float", "default": 0.9},  # 给出cdhit的参数coverage
            {"name": "insertsize", "type": "infile", "format": "sample.insertsize_table"},  # 插入片段文件
            {"name": "QC_dir", "type": "infile", "format": "sequence.fastq_dir"},  # qc后reads文件夹
            {"name": "reads_abundance", "type": "outfile", "format": "sequence.profile_table"},  # reads_abundance
            {"name": "rpkm_abundance", "type": "outfile", "format": "sequence.profile_table"},  # rpkm_abundance
            {"name": "seed", "type": "int", "default": 35},
            # align the initial n bps as a seed means whole lengths of read
            {"name": "mode", "type": "int", "default": 4},
            # match mode for each read or the seed part of read, which shouldn't contain more than 2 mismatches: 0 for exact mathc only; 1 for 1 mismatch; 2 for 2 mismatch; 4 for find the best hits
            {"name": "processors", "type": "int", "default": 6},
            {"name": "mismatch", "type": "int", "default": 20},  # maximum number of mismatches allowed on a read
            {"name": "repeat", "type": "int", "default": 1},  # how to report repeat hits, 0=none, 1=random one, 2=all
            {"name": "soap_identity", "type": "float", "default": 0.95}  # soap aligner identity
        ]
        self.add_option(options)
        self.cdhit = self.add_module("cluster.cdhit_unigene")
        self.soap_aligner = self.add_module("align.map_geneset")
        self.step.add_steps("cdhit", "soap")

    def check_options(self):
        if not 0.75 <= self.option("cdhit_identity") <= 1:
            raise OptionError("cdhit identity必须在0.75，1之间")
        if not 0 <= self.option("cdhit_coverage") <= 1:
            raise OptionError("cdhit coverage必须在0,1之间")
        if self.option("number") < 0:
            raise OptionError("number必须大于等于0")
        if not self.option("repeat") in [0, 1, 2]:
            raise OptionError("repeat必须为0,1,或2")
        if not self.option("mode") in [0, 1, 2, 4]:
            raise OptionError("repeat必须为0,1,2,或4")
        if not 0 < self.option("seed") <= 256:
            raise OptionError('seed参数必须设置在1-256之间:{}'.format(self.option('seed')))
        if not 0 < self.option("soap_identity") < 1:
            raise OptionError("soap identity必须在0，1之间")
        if not self.option("QC_dir").is_set:
            raise OptionError("必须提供质控后的fq文件夹")
        if not self.option("insertsize").is_set:
            raise OptionError("必须提供插入片段文件")

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def cd_hit(self):
        self.cdhit.set_options({
            "gene_tmp_fa": self.option("gene_tmp_fa"),
            "number": self.option("number"),
            "identity": self.option("cdhit_identity"),
            "coverage": self.option("cdhit_coverage"),
        })
        self.cdhit.on("start", self.set_step, {"start": self.step.cdhit})
        self.cdhit.on("end", self.set_step, {"end": self.step.cdhit})
        self.cdhit.on("end", self.set_step, {"start": self.step.soap})
        self.cdhit.on("end", self.soap)
        self.cdhit.run()

    def soap(self):
        self.soap_aligner.set_options({
            "fafile": self.cdhit.option("uni_fasta"),
            "insertsize": self.option("insertsize"),
            "QC_dir": self.option("QC_dir"),
            "seed": self.option("seed"),
            "mode": self.option("mode"),
            "processors": self.option("processors"),
            "mismatch": self.option("mismatch"),
            "repeat": self.option("repeat"),
            "identity": self.option("soap_identity")
        })
        self.soap_aligner.on("start", self.set_step, {"start": self.step.soap})
        self.soap_aligner.on("end", self.set_step, {"end": self.step.soap})
        self.soap_aligner.on("end", self.set_output)
        self.soap_aligner.run()

    def set_output(self):
        self.linkdir(self.cdhit.output_dir + "/length_distribute", "length_distribute")
        self.linkdir(self.cdhit.output_dir + "/uniGeneset", "uniGeneset")
        self.linkdir(self.soap_aligner.output_dir + "/gene_profile", "gene_profile")
        self.option('uni_fasta', self.cdhit.option("uni_fasta"))
        self.option('uni_fastaa', self.cdhit.option("uni_fastaa"))
        self.option('reads_abundance', self.soap_aligner.option("reads_abundance"))
        self.option('rpkm_abundance', self.soap_aligner.option("rpkm_abundance"))
        self.end()

    def run(self):
        super(UniGeneModule, self).run()
        self.cd_hit()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["uniGeneset", "", "非冗余基因集输出目录"],
            ["uniGeneset/geneCatalog_stat.xls", "xls", "非冗余基因集统计结果"],
            ["uniGeneset/gene.uniGeneset.fa", "fa", "非冗余基因集核酸序列"],
            ["uniGeneset/gene.uniGeneset.faa", "faa", "非冗余基因集蛋白序列"],
            ["length_distribute", "", "非冗余基因集长度分布统计目录"],
            ["gene_profile", "", "非冗余基因丰度目录"]
        ])
        result_dir.add_regexp_rules([
            [r'length_distribute/gene_step_.*\.txt$', 'txt', '长度分布统计结果']
            [r'gene_profile/gene_profile.*\.xls$', 'xls', '基因丰度表']
        ])
        super(UniGeneModule, self).end()

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
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
