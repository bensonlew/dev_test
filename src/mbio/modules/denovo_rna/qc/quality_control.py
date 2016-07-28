#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
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
        self.samples = {}
        self.seqprep = []
        self.clipper = []
        self.end_times = 0

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fastq_dir").is_set:
            raise OptionError("需要传入fastq文件或者文件夹")
        if self.option("fastq_dir").is_set:
            # self.samples = self.get_list()
            list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
            if not os.path.exists(list_path):
                OptionError("缺少list文件")
            row_num = len(open(list_path, "r").readline().split())
            self.logger.info(row_num)
            if self.option('fq_type') == "PE" and row_num != 3:
                raise OptionError("PE序列list文件应该包括文件名、样本名和左右端说明三列")
            elif self.option('fq_type') == "SE" and row_num != 2:
                raise OptionError("SE序列list文件应该包括文件名、样本名两列")
        # if not self.option('fastq_dir').prop['has_list_file']:
        #     raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        # self.logger.info(list_path)
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        # self.logger.info(samples)
        return samples

    def clipper_run(self):
        n = 1
        self.samples = self.get_list()
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
            clipper.on("end", self.rename, f)
            clipper.on("end", self.sickle_se_run, f)
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
            seqprep = self.add_tool('denovo_rna.qc.seq_prep')
            self.step.add_steps('seqprep_{}'.format(n))
            seqprep.set_options({
                "fastq_l": fq_l,
                "fastq_r": fq_r
            })
            step = getattr(self.step, 'seqprep_{}'.format(n))
            step.start()
            seqprep.on("end", self.finish_update, "seqprep_{}".format(n))
            seqprep.on("end", self.sickle_pe_run, f)
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
        clip_s = os.path.join(obj.output_dir, event["data"] + "_clip_s.fastq")
        self.logger.info(clip_s)
        sickle = self.add_tool('denovo_rna.qc.sickle')
        self.step.add_steps('sickle_{}'.format(self.end_times))
        sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_s": clip_s
        })
        step = getattr(self.step, 'sickle_{}'.format(self.end_times))
        step.start()
        sickle.on("end", self.finish_update, 'sickle_{}'.format(self.end_times))
        sickle.on("end", self.set_output, event["data"])
        sickle.run()
        # self.sickle.append(sickle)

    def sickle_pe_run(self, event):
        obj = event["bind_object"]
        seqprep_l = ""
        seqprep_r = ""
        for f in os.listdir(obj.output_dir):
            if "seqprep_l" in f:
                seqprep_l = os.path.join(obj.output_dir, f)
            if "seqprep_r" in f:
                seqprep_r = os.path.join(obj.output_dir, f)
        sickle = self.add_tool('denovo_rna.qc.sickle')
        self.step.add_steps('sickle_{}'.format(self.end_times))
        sickle.set_options({
            "fq_type": self.option("fq_type"),
            "fastq_l": seqprep_l,
            "fastq_r": seqprep_r

        })
        step = getattr(self.step, 'sickle_{}'.format(self.end_times))
        step.start()
        sickle.on("end", self.finish_update, 'sickle_{}'.format(self.end_times))
        sickle.on("end", self.set_output, event["data"])
        sickle.run()
        # self.sickle.append(sickle)

    def rename(self, event):
        obj = event["bind_object"]
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            new_name = os.path.join(obj.output_dir, event["data"] + "_" + f)
            os.rename(old_name, new_name)

    def set_output(self, event):
        self.logger.info("set output{}".format(event["data"]))
        obj = event["bind_object"]
        if self.end_times < len(self.samples):
            self.end_times += 1
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            new_name = os.path.join(obj.output_dir, event["data"] + "_" + f)
            os.rename(old_name, new_name)
        if self.end_times == len(self.samples):
            sickle_dir = os.path.join(self.output_dir, "sickle_dir")
            sickle_r_dir = os.path.join(self.work_dir, "sickle_r_forRSEM")
            sickle_l_dir = os.path.join(self.work_dir, "sickle_l_forRSEM")
            seqprep_dir = os.path.join(self.output_dir, "seqprep_dir")
            clip_dir = os.path.join(self.output_dir, "clip_dir")
            dir_list = [sickle_dir, seqprep_dir, clip_dir, sickle_r_dir, sickle_l_dir]
            # self.logger.info(dir_list)
            for d in dir_list:
                if os.path.exists(d):
                    shutil.rmtree(d)
                os.mkdir(d)
            sickle_out = glob.glob(r"{}/Sickle*/output/*".format(self.work_dir))
            seqprep_out = glob.glob(r"{}/SeqPrep*/output/*".format(self.work_dir))
            clip_out = glob.glob(r"{}/FastxClipper*/output/*".format(self.work_dir))
            self.logger.info(os.path.join(sickle_dir, "list.txt"))
            with open(os.path.join(sickle_dir, "list.txt"), "w") as w:
                for f in sickle_out:
                    f_name = f.split("/")[-1]
                    sample_name = f_name.split("_")[0]
                    w.write("{}\t{}".format(f_name, sample_name))
                    if "sickle_r.fastq" in f:
                        w.write("\t{}\n".format("r"))
                        os.link(f, os.path.join(sickle_r_dir, f_name))
                    elif "sickle_l.fastq" in f:
                        w.write("\t{}\n".format("l"))
                        os.link(f, os.path.join(sickle_l_dir, f_name))
                    else:
                        w.write("\n")
                    target_path = os.path.join(sickle_dir, f_name)
                    os.link(f, target_path)
            self.option("sickle_dir").set_path(sickle_dir)
            if self.option("fq_type") == "PE":
                shutil.rmtree(clip_dir)
                for f in seqprep_out:
                    f_name = f.split("/")[-1]
                    target_path = os.path.join(seqprep_dir, f_name)
                    os.link(f, target_path)
                self.option("seqprep_dir").set_path(seqprep_dir)
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
            elif self.option('fq_type') == 'SE':
                shutil.rmtree(seqprep_dir)
                shutil.rmtree(sickle_r_dir)
                shutil.rmtree(sickle_l_dir)
                for f in clip_out:
                    f_name = f.split("/")[-1]
                    target_path = os.path.join(clip_dir, f_name)
                    os.link(f, target_path)
                self.option("clip_dir").set_path(clip_dir)
                files = os.listdir(sickle_dir)
                s_file = ' '.join(files)
                os.system('cd {} && cat {} > {}/single.fq'.format(sickle_dir, s_file, self.work_dir))
                self.option('fq_r', self.work_dir + '/single.fq')
                self.logger.info("done")
            self.end()

    def run(self):
        # super(QualityControlModule, self).run()
        if self.option("fq_type") in ["PE"]:
            self.seqprep_run()
        else:
            self.clipper_run()
        super(QualityControlModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(QualityControlModule, self).end()
