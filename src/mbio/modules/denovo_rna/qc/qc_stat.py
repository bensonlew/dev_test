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
        # self.dup = self.add_tool('denovo_rna.qc.fastq_dup')
        self.step.add_steps("dup", "stat")

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fastq_dir").is_set:
            raise OptionError("需要传入fastq文件或者文件夹")
        if self.option("fastq_dir").is_set:
            list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
            if not os.path.exists(list_path):
                OptionError("缺少list文件")
            self.samples = self.get_list()
            row_num = len(open(list_path, "r").readline().split())
            self.logger.info(row_num)
            if self.option('fq_type') == "PE" and row_num != 3:
                raise OptionError("PE序列list文件应该包括文件名、样本名和左右端说明三列")
            elif self.option('fq_type') == "SE" and row_num != 2:
                raise OptionError("SE序列list文件应该包括文件名、样本名两列")

    def finish_update(self, event):
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
        n = 0
        for f in self.samples:
            options = {}
            if self.option("fq_type") == "PE":
                fq_l = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["l"])
                fq_r = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f]["r"])
                options = {
                    'fastq_l': fq_l,
                    'fastq_r': fq_r,
                    'fq_type': self.option('fq_type')
                }
            elif self.option("fq_type") == "SE":
                fq_s = os.path.join(self.option("fastq_dir").prop["path"], self.samples[f])
                options = {
                    'fastq_s': fq_s,
                    'fq_type': self.option('fq_type')
                }
            dup = self.add_tool('denovo_rna.qc.fastq_dup')
            self.step.add_steps('dup_{}'.format(n))
            dup.set_options(options)
            step = getattr(self.step, 'dup_{}'.format(n))
            step.start()
            dup.on("end", self.finish_update, "dup_{}".format(n))
            dup.on("end", self.rename, f)
            dup.run()
            self.tools.append(dup)
            n += 1

    # def dup_run(self):
    #     self.dup.set_options({
    #         'fastq_dir': self.option('fastq_dir').prop["path"],
    #         'fq_type': self.option('fq_type')
    #         })
    #     # self.on_rely(estimators, self.rarefaction_run)
    #     self.step.dup.start()
    #     self.dup.on("end", self.dup_finish_update)
    #     self.dup.run()
    #     self.tools.append(self.dup)

    def draw_run(self):
        # self.samples = self.get_list()
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
                draw_l.on("end", self.rename, "{}_l".format(f))
                draw_r.on("end", self.rename, "{}_r".format(f))
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
        self.logger.info('{}'.format(self.events))
        # self.logger.info(self.tools)
        super(QcStatModule, self).run()
        for eve in self.events.values():
            self.logger.info('{}'.format(eve.is_start))

    def set_output(self):
        self.logger.info("set output")
        for f in glob.glob(r"{}/*".format(self.output_dir)):
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.remove(f)
        draw_dir = os.path.join(self.output_dir, "qualityStat")
        os.mkdir(draw_dir)
        dup_out = []
        for tool in self.tools:
            out_files = os.listdir(tool.output_dir)
            for f in out_files:
                f_path = os.path.join(tool.output_dir, f)
                if "qual_stat" in f:
                    target = os.path.join(draw_dir, f)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(f_path, target)
                elif "dup" in f:
                    dup_out.append(f_path)
                else:
                    target = self.output_dir + "/{}".format("fastq_stat.xls")
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(f_path, target)
        # self.logger.info(dup_out)
        if self.option("dup") is True:
            with open(self.work_dir + "/dup.xls", "w") as w:
                if self.option("fq_type") == "PE":
                    w.write("sample\tread1Dup\tread2Dup\tPairedDup\n")
                else:
                    w.write("sample\treadDup\n")
                for f in dup_out:
                    sample_name = os.path.basename(f).split("_")[0]
                    f = open(f, "r")
                    f.readline()
                    w.write("{}\t{}".format(sample_name, f.next()))
            if os.path.exists(self.output_dir + "/dup.xls"):
                os.remove(self.output_dir + "/dup.xls")
            os.link(self.work_dir + "/dup.xls", self.output_dir + "/dup.xls")
        self.logger.info("done")
        self.end()

    def rename(self, event):
        obj = event["bind_object"]
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            new_name = os.path.join(obj.output_dir, event["data"] + "_" + f)
            os.rename(old_name, new_name)

    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        # self.logger.info(list_path)
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        self.logger.info(samples)
        return samples

    def end(self):
        self.logger.info('%s' % self.upload_dir)
        if self.upload_dir:
            for i in self.upload_dir:
                self.logger.info('%s' % i._parent)
                self.logger.info('%s' % i.file_list)
        result_dir = self.add_upload_dir(self.output_dir)
        self.logger.info('%s' % self.upload_dir)
        result_dir.add_relpath_rules([
                [r".", "", "结果输出目录"],
                [r"./qualityStat/", "文件夹", "质量统计文件夹"],
                [r"./fastq_stat.xls", "xls", "fastq信息统计表"]
            ])
        if self.option("dup") is True:
            result_dir.add_relpath_rules([
                [r"./dup.xls", "xls", "fastq序列重复信息"]
            ])
        # print self.get_upload_files()
        # self.logger.info(self._parent)
        # self.logger.info(self._parent.events['childend'].is_start)
        # self.logger.info(self.events['childend'].is_start)
        super(QcStatModule, self).end()
