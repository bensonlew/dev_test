#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile


class QualityControlModule(Module):
    """
    denovoRNA数据指控模块
    version 1.0
    author: qindanhua
    last_modify: 2016.07.25
    """
    def __init__(self, work_id):
        super(QualityControlModule, self).__init__(work_id)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "clip_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # SE去接头输出结果文件夹
            {"name": "sickle_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切输出结果文件夹(包括左右段)
            {"name": "sickle_r_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切右端输出结果文件夹
            {"name": "sickle_l_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # 质量剪切左端输出结果文件夹
            {"name": "seqprep_dir", "type": "outfile", "format": "sequence.fastq_dir"},  # PE的去接头输出结果文件
            {"name": "fq_s", "type": "outfile", "format": "sequence.fastq"},  # SE所有样本cat集合
            {"name": "fq_r", "type": "outfile", "format": "sequence.fastq"},  # PE所有右端序列样本cat集合
            {"name": "fq_l", "type": "outfile", "format": "sequence.fastq"},  # PE所有左端序列样本cat集合
            # {"name": "quality_a", "type": "int", "default": 30},  # 去接头碱基质量
            # {"name": "length_a", "type": "int", "default": 30},  # 去接头碱基长度
            # {"name": "quality_q", "type": "int", "default": 20},  # 质量剪切碱基质量
            # {"name": "length_q", "type": "int", "default": 30},  # 质量剪切碱基长度
        ]
        self.add_option(options)
        self.clipper = []
        self.seqprep = []
        self.sickle = []
        self.samples = {}
        self.end_times = 0

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fastq_dir").is_set:
            raise OptionError("需要传入fastq文件或者文件夹")
        if not self.option('fastq_dir').prop['has_list_file']:
            raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')

    def finish_update(self, event):
        # obj = event['bind_object']
        # self.logger.info(event)
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def get_list(self):
        if self.option("fastq_dir").is_set:
            self.logger.info("hei")
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        self.logger.info(list_path)
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        self.logger.info(samples)
        return samples

    def clipper_run(self):
        n = 1
        for f in self.samples:
            fq_s = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f])
            clipper = self.add_tool('denovo_rna.qc.fastx_clipper')
            self.step.add_steps('clipper_{}'.format(n))
            clipper.set_options({
                "fastq_s": fq_s,
            })
            step = getattr(self.step, 'clipper_{}'.format(n))
            step.start()
            clipper.on("end", self.finish_update, "clipper_{}".format(n))
            clipper.on("end", self.sickle_se_run)
            # clipper.run()
            n += 1
            self.clipper.append(clipper)
        if len(self.clipper) == 1:
            self.clipper[0].run()
        else:
            for tool in self.clipper:
                tool.run()

    def seqprep_run(self):
        self.samples = self.get_list()
        n = 1
        for f in self.samples:
            fq_l = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["l"])
            fq_r = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["r"])
            self.logger.info(fq_l)
            self.logger.info(fq_r)
            seqprep = self.add_tool('denovo_rna.qc.seq_prep')
            self.step.add_steps('seqprep_{}'.format(n))
            seqprep.set_options({
                "fastq_l": fq_l,
                "fastq_r": fq_r
            })
            step = getattr(self.step, 'seqprep_{}'.format(n))
            step.start()
            seqprep.on("end", self.finish_update, "seqprep_{}".format(n))
            seqprep.on("end", self.sickle_pe_run)
            # seqprep.run()
            n += 1
            self.seqprep.append(seqprep)
        self.logger.info(self.seqprep)
        if len(self.seqprep) == 1:
            self.seqprep[0].run()
        else:
            for tool in self.seqprep:
                tool.run()

    def sickle_se_run(self, event):
        obj = event["bind_object"]
        clip_s = os.path.join(obj.output_dir, "clip_s.fastq")
        self.logger.info(clip_s)
        sickle = self.add_tool('denovo_rna.qc.sickle')
        sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_s": clip_s
        })
        self.step.sickle.start()
        sickle.on("end", self.finish_update)
        sickle.run()
        self.sickle.append(sickle)

    def sickle_pe_run(self, event):
        obj = event["bind_object"]
        seqprep_l = os.path.join(obj.output_dir, "sickle_l.fastq")
        seqprep_r = os.path.join(obj.output_dir, "sickle_r.fastq")
        sickle = self.add_tool('denovo_rna.qc.sickle')
        sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_l": seqprep_l,
            "fastq_r": seqprep_r

        })
        self.step.sickle.start()
        sickle.on("end", self.finish_update)
        sickle.on("end", self.set_output)
        sickle.run()
        self.sickle.append(sickle)

    def set_output(self):
        self.logger.info("set output")
        if self.end_times < len(self.samples):
            self.end_times += 1
        if self.end_times == len(self.samples):
            sickle_dir = os.path.join(self.output_dir, "sickle_dir")
            sickle_r_dir = os.path.join(self.output_dir, "sickle_r_forRSEM")
            sickle_l_dir = os.path.join(self.output_dir, "sickle_l_forRSEM")
            seqprep_dir = os.path.join(self.output_dir, "seqprep_dir")
            clip_dir = os.path.join(self.output_dir, "clip_dir")
            dir_list = [sickle_dir, seqprep_dir, clip_dir, sickle_r_dir, sickle_l_dir]
            self.logger.info(dir_list)
            for d in dir_list:
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.mkdir(d)
            self.logger.info("done")
            self.end()

    def run(self):
        super(QualityControlModule, self).run()
        if self.option("fq_type") in ["PE"]:
            self.seqprep_run()
        else:
            self.clipper_run()
        # if len(self.sickle) < 2:
        #     self.sickle[0].on("end", self.set_output)
        # else:
        # self.on_rely(self.sickle, self.set_output)

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(QualityControlModule, self).end()