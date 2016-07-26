#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
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
            {"name": "clip_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # SE去接头输出结果文件夹
            {"name": "sickle_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切输出结果文件夹(包括左右段)
            {"name": "sickle_r_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切右端输出结果文件夹
            {"name": "sickle_l_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切左端输出结果文件夹
            {"name": "seqprep_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # PE的去接头输出结果文件
            {"name": "fq_s", "type": "outfile", "format": "sequence.fastq"},  # SE所有样本集合
            {"name": "fq_r", "type": "outfile", "format": "sequence.fastq"},  # PE所有右端序列样本集合
            {"name": "fq_l", "type": "outfile", "format": "sequence.fastq"},  # PE所有左端序列样本集合
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
        self.sickle.on("end", self.stat_after_run)
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
        stat_dir = os.path.join(self.output_dir, "fastqStat_dir")
        draw_dir = os.path.join(self.output_dir, "qualStat_dir")
        draw_after_dir = os.path.join(self.output_dir, "qualStat_after_sickle")
        sickle_dir = os.path.join(self.output_dir, "sickle_dir")
        sickle_r_dir = os.path.join(self.work_dir, "sickle_r_forRSEM")
        sickle_l_dir = os.path.join(self.work_dir, "sickle_l_forRSEM")
        seqprep_dir = os.path.join(self.output_dir, "seqprep_dir")
        clip_dir = os.path.join(self.output_dir, "clip_dir")
        dir_list = [stat_dir, draw_dir, sickle_dir, seqprep_dir, clip_dir, draw_after_dir, sickle_r_dir, sickle_l_dir]
        self.logger.info(dir_list)
        for d in dir_list:
            if os.path.exists(d):
                shutil.rmtree(d)
            os.mkdir(d)
        for f in os.listdir(self.stat.output_dir):
            from_output = os.path.join(self.stat.output_dir, f)
            target_path = os.path.join(stat_dir, f)
            os.link(from_output, target_path)
        for f in os.listdir(self.stat_after.output_dir):
            from_output = os.path.join(self.stat_after.output_dir, f)
            target_path = os.path.join(stat_dir, f)
            os.link(from_output, target_path)
        for f in os.listdir(self.draw_info.output_dir):
            from_output = os.path.join(self.draw_info.output_dir, f)
            target_path = os.path.join(draw_dir, f)
            os.link(from_output, target_path)
        for f in os.listdir(self.draw_info_after.output_dir):
            from_output = os.path.join(self.draw_info_after.output_dir, f)
            target_path = os.path.join(draw_after_dir, f)
            os.link(from_output, target_path)
        for f in os.listdir(self.sickle.output_dir):
            from_output = os.path.join(self.sickle.output_dir, f)
            target_path = os.path.join(sickle_dir, f)
            os.link(from_output, target_path)
            if "sickle_r.fastq" in f:
                os.link(from_output, os.path.join(sickle_r_dir, f))
            elif "sickle_l.fastq" in f:
                os.link(from_output, os.path.join(sickle_l_dir, f))
        ### add by qiuping,2017.07.25
        if self.option('fq_type') == 'PE':
            self.option('sickle_r_dir', sickle_r_dir)
            self.option('sickle_l_dir', sickle_l_dir)
            r_files = os.listdir(self.option('sickle_r_dir').prop['path'])
            l_files = os.listdir(self.option('sickle_l_dir').prop['path'])
            r_file = ' '.join(r_files)
            l_file = ' '.join(l_files)
            os.system('cd {} && cat {} > {}/left.fq && cd {} && cat {} > {}/right.fq'.format(sickle_l_dir, l_file, self.work_dir, sickle_r_dir, r_file, self.work_dir))
            self.logger.info('cd {} && cat {} > {}/left.fq && cd {} && cat {} > {}/right.fq'.format(sickle_l_dir, l_file, self.work_dir, sickle_r_dir, r_file, self.work_dir))
            self.option('fq_l', self.work_dir + '/left.fq')
            self.option('fq_r', self.work_dir + '/right.fq')
        if self.option('fq_type') == 'SE':
            files = os.listdir(sickle_dir)
            s_file = ' '.join(files)
            os.system('cd {} && cat {} > {}/single.fq'.format(sickle_dir, s_file, self.work_dir))
            self.option('fq_r', self.work_dir + '/single.fq')
        ### modify end，将多个fq文件cat到一起并设置outfile
        self.option("sickle_dir").set_path(sickle_dir)
        if self.option("fq_type") == "PE":
            for f in os.listdir(self.seqprep.output_dir):
                from_output = os.path.join(self.seqprep.output_dir, f)
                target_path = os.path.join(seqprep_dir, f)
                os.link(from_output, target_path)
            self.option("seqprep_dir").set_path(seqprep_dir)
            shutil.rmtree(clip_dir)
        else:
            for f in os.listdir(self.clipper.output_dir):
                from_output = os.path.join(self.clipper.output_dir, f)
                target_path = os.path.join(clip_dir, f)
                os.link(from_output, target_path)
            self.option("clip_dir").set_path(clip_dir)
            shutil.rmtree(seqprep_dir)
            shutil.rmtree(sickle_l_dir)
            shutil.rmtree(sickle_r_dir)
        self.logger.info("done")
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
            self.on_rely([self.stat, self.draw_info, self.clipper, self.sickle, self.stat_after, self.draw_info_after], self.set_output)

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(DenovoQcModule, self).end()
