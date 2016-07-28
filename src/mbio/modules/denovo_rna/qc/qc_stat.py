#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile


class QcStatModule(Module):
    """
    denovoRNA数据指控模块
    version 1.0
    author: qindanhua
    last_modify: 2016.07.26
    """
    def __init__(self, work_id):
        super(QcStatModule, self).__init__(work_id)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "fq_type", "type": "string"},  # PE OR SE
            {"name": "dup", "type": "bool", "default": False},  # PE OR SE
        ]
        self.add_option(options)
        self.samples = {}
        self.tools = []
        self.stat = self.add_tool('denovo_rna.qc.fastq_stat')
        # self.draw_info = self.add_tool('denovo_rna.qc.draw_fastq_info')
        self.dup = self.add_tool('denovo_rna.qc.fastq_dup')
        self.step.add_steps("dup", "stat")

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
        if not self.option('fastq_dir').prop['has_list_file']:
            raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')

    def finish_update(self, event):
        # obj = event['bind_object']
        # self.logger.info(event)
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def stat_finish_update(self):
        self.step.stat.finish()
        self.step.update()

    def dup_finish_update(self):
        self.step.dup.finish()
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
        self.tools.append(self.stat)

    def dup_run(self):
        self.dup.set_options({
            'fastq_dir': self.option('fastq_dir').prop["path"],
            'fq_type': self.option('fq_type')
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.dup.start()
        self.dup.on("end", self.dup_finish_update)
        self.dup.run()
        self.tools.append(self.dup)

    def draw_run(self):
        self.samples = self.get_list()
        # files = []
        n = 1
        if self.option("fq_type") == "PE":
            for f in self.samples:
                fq_l = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["l"])
                fq_r = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["r"])
                draw_l = self.add_tool('denovo_rna.qc.draw_fastq_info')
                draw_r = self.add_tool('denovo_rna.qc.draw_fastq_info')
                self.step.add_steps('drawL_{}'.format(n))
                self.step.add_steps('drawR_{}'.format(n))
                draw_l.set_options({
                    "fastq": fq_l
                })
                draw_r.set_options({
                    "fastq": fq_r
                })
                step = getattr(self.step, 'drawL_{}'.format(n))
                step.start()
                step = getattr(self.step, 'drawR_{}'.format(n))
                step.start()
                draw_l.on("end", self.finish_update, "drawL_{}".format(n))
                draw_r.on("end", self.finish_update, "drawR_{}".format(n))
                draw_l.on("end", self.rename, "L_{}".format(f))
                draw_r.on("end", self.rename, "R_{}".format(f))
                draw_l.run()
                draw_r.run()
                self.tools.append(draw_l)
                self.tools.append(draw_r)
                n += 1
        else:
            for f in self.samples:
                fq_s = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f])
                draw = self.add_tool('denovo_rna.qc.draw_fastq_info')
                self.step.add_steps('draw_{}'.format(n))
                draw.set_options({
                    "fastq": fq_s,
                })
                step = getattr(self.step, 'draw_{}'.format(n))
                step.start()
                draw.on("end", self.finish_update, "draw_{}".format(n))
                draw.on("end", self.rename, f)
                draw.run()
                self.tools.append(draw)
                n += 1

    def run(self):
        # super(QcStatModule, self).run()
        self.stat_run()
        if self.option("dup") is True:
            self.dup_run()
        self.draw_run()
        self.on_rely(self.tools, self.set_output)
        self.logger.info(self.tools)
        super(QcStatModule, self).run()

    def set_output(self):
        self.logger.info("set output")
        draw_dir = os.path.join(self.output_dir, "qualityStat")
        if os.path.exists(draw_dir):
            shutil.rmtree(draw_dir)
        os.mkdir(draw_dir)
        draw_out = glob.glob(r"{}/DrawFastqInfo*/output/*".format(self.work_dir))
        dup_out = glob.glob(r"{}/FastqDup/output/*".format(self.work_dir))
        stat_out = glob.glob(r"{}/FastqStat/output/*".format(self.work_dir))
        for f in draw_out:
            f_name = f.split("/")[-1]
            target_path = os.path.join(draw_dir, f_name)
            os.link(f, target_path)
        for f in dup_out:
            f_name = f.split("/")[-1]
            os.link(f, self.output_dir + "/{}".format(f_name))
        for f in stat_out:
            f_name = f.split("/")[-1]
            os.link(f, self.output_dir + "/{}".format(f_name))
        self.logger.info("done")
        self.end()

    def rename(self, event):
        obj = event["bind_object"]
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            new_name = os.path.join(obj.output_dir, event["data"] + "_" + f)
            os.rename(old_name, new_name)

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

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(QcStatModule, self).end()

