#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
# import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class DenovoQcModule(Module):
    """
    denovoRNA数据指控模块
    version 1.0
    author: qindanhua
    last_modify: 2016.06.27
    """
    def __init__(self, work_id):
        super(DenovoQcModule, self).__init__(work_id)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "quality_a", "type": "int", "default": 30},  # 去接头碱基质量
            {"name": "length_a", "type": "int", "default": 30},  # 去接头碱基长度
            {"name": "quality_q", "type": "int", "default": 20},  # 质量剪切碱基质量
            {"name": "length_q", "type": "int", "default": 30},  # 质量剪切碱基长度
            {"name": "clip_s", "type": "outfile", "format": "sequence.fastq"},  # SE去接头输出结果
            {"name": "sickle_s", "type": "outfile", "format": "sequence.fastq"},  # SE输出结果
            {"name": "seqprep_r", "type": "outfile", "format": "sequence.fastq"},  # PE的右端输出结果
            {"name": "seqprep_l", "type": "outfile", "format": "sequence.fastq"},  # PE的左端输出结果
            {"name": "sickle_r", "type": "outfile", "format": "sequence.fastq"},  # PE的右端输出结果
            {"name": "sickle_l", "type": "outfile", "format": "sequence.fastq"},  # PE的左端输出结果
            {"name": "sickle_un", "type": "outfile", "format": "sequence.fastq"}  # PE的未配对输出结果
        ]
        self.add_option(options)
        self.stat = self.add_tool('denovo_rna.qc.fastq_stat')
        self.clipper = self.add_tool('denovo_rna.qc.fastx_clipper')
        self.seqprep = self.add_tool('denovo_rna.qc.seq_prep')
        self.sickle = self.add_tool('denovo_rna.qc.sickle')
        self.draw_info = self.add_tool('denovo_rna.qc.draw_fastq_info')
        self.draw_info_after = self.add_tool('denovo_rna.qc.draw_fastq_info')
        self.stat_after = self.add_tool('denovo_rna.qc.fastq_stat')
        self.step.add_steps('stat', 'clipper', 'seqprep', 'sickle', 'draw_info', 'stat_after', 'draw_info_after')

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fastq_dir").is_set:
            raise OptionError("需要传入fastq文件或者文件夹")
        if not self.option('fastq_dir').prop['has_list_file']:
            raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')

    def stat_finish_update(self):
        self.step.stat.finish()
        self.step.update()

    def stat_after_finish_update(self):
        self.step.stat_after.finish()
        self.step.update()

    def draw_finish_update(self):
        self.step.draw_info.finish()
        self.step.update()

    def draw_after_finish_update(self):
        self.step.draw_info_after.finish()
        self.step.update()

    def stat_run(self):
        self.stat.set_options({
            'fastq': self.option('fastq_dir').prop["path"],
            'fq_type': self.option('fq_type')
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.stat.start()
        self.stat.on("end", self.stat_finish_update)
        self.stat.run()

    def clipper_run(self):
        self.clipper.set_options({
            "fastq_dir": self.option("fastq_dir"),
        })
        self.step.clipper.start()
        self.clipper.on("end", self.sickle_se_run)
        self.clipper.run()

    def sickle_se_run(self):
        self.step.clipper.finish()
        self.step.update()
        self.sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_dir": self.clipper.output_dir
        })
        self.step.sickle.start()
        self.sickle.on("end", self.stat_after)
        self.sickle.on("end", self.draw_info_after_run)
        self.sickle.run()

    def sickle_pe_run(self):
        self.step.seqprep.finish()
        self.step.update()
        self.sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_dir": self.seqprep.output_dir
        })
        self.step.sickle.start()
        self.logger.info("on.stat")
        self.sickle.on("end", self.stat_after_run)
        self.logger.info("on.draw")
        self.sickle.on("end", self.draw_info_after_run)
        self.sickle.run()

    def seqprep_run(self):
        self.seqprep.set_options({
            "fastq_dir": self.option("fastq_dir").prop["path"]
        })
        self.step.seqprep.start()
        self.logger.info("seqprep_start")
        self.seqprep.on("end", self.sickle_pe_run)
        self.logger.info("on.sickle")
        self.seqprep.run()

    def stat_after_run(self):
        self.step.sickle.finish()
        self.step.update()
        self.step.stat_after.start()
        self.stat_after.set_options({
            'fastq': self.sickle.output_dir,
            'fq_type': self.option("fq_type")
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.stat_after.start()
        self.stat.on("end", self.stat_after_finish_update)
        self.stat_after.run()

    def draw_info_run(self):
        self.draw_info.set_options({
            'fastq': self.option('fastq_dir').prop["path"]
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.draw_info.start()
        self.draw_info.on("end", self.draw_finish_update)
        self.draw_info.run()

    def draw_info_after_run(self):
        self.draw_info_after.set_options({
            'fastq': self.sickle.output_dir
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.draw_info_after.start()
        self.draw_info_after.on("end", self.draw_after_finish_update)
        self.draw_info_after.run()

    def set_output(self):
        self.logger.info("set output")
        module_stat = os.path.join(self.output_dir, "fastq_stat")
        module_draw = os.path.join(self.output_dir, "qual_stat")
        if os.path.exists(module_draw):
            pass
        else:
            os.makedirs(module_draw)
        if os.path.exists(module_stat):
            pass
        else:
            os.makedirs(module_stat)
        for f in os.listdir(self.draw_info.output_dir):
            # self.logger.info(os.path.join(self.draw_info.output_dir, f))
            draw_output = os.path.join(self.draw_info.output_dir, f)
            module_draw_file = os.path.join(module_draw, f)
            os.link(draw_output, module_draw_file)
        self.end()

    def run(self):
        super(DenovoQcModule, self).run()
        if self.option("fq_type") in ["PE"]:
            self.stat_run()
            self.draw_info_run()
            self.logger.info("info")
            self.seqprep_run()
            self.on_rely([self.stat, self.draw_info, self.seqprep, self.sickle, self.stat_after, self.draw_info_after], self.set_output)
        elif self.option("fq_type") in ["SE"]:
            self.stat_run()
            self.draw_info_run()
            self.clipper_run()
            self.on_rely([self.stat, self.draw_info, self.clipper, self.sickle], self.set_output)

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(DenovoQcModule, self).end()
