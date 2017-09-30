# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify:2017.08.22

"""bar / circos / heatmap"""
import os
import shutil
from biocluster.module import Module
from biocluster.core.exceptions import OptionError
from mainapp.models.mongo.public.meta.meta import Meta

class CompositionAnalysisModule(Module):
    def __init__(self, work_id):
        super(CompositionAnalysisModule, self).__init__(work_id)
        options = [
            {"name": "analysis", "type": "string","default": "bar,heatmap,circos"},
            {"name": "abundtable", "type": "infile", "format": "meta.otu.otu_table"},  # 物种/功能丰度表格
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string", "default": ""},  # 输入的group_detail,来自于网页选择,待测
            {"name": "add_Algorithm", "type": "string", "default": ""},  # 分组样本求和算法，默认不求和
            {"name": "method", "type": "string", "default": ""},  # 物种层次聚类方式，默认不聚类
            {"name": "sample_method", "type": "string", "default": ""},  # 样本层次聚类方式，默认不聚类
            {"name": "species_number", "type": "string", "default": "50"},  # 物种数目，默认top50
        ]
        self.add_option(options)
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        self.sort_samples2 = self.add_tool("meta.otu.sort_samples")
        self.heatmap = self.add_module("meta.composition.heatmap")
        if self.option("group_detail") != "":
            group_table = os.path.join(self.work_dir, "group_table.xls")
            self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table)

    def check_options(self):
        analysis = self.option('analysis').split(',')
        for i in analysis:
            if i in ['bar', 'circos', 'heatmap']:
                print i
                break
            else:
                print i
                raise OptionError('没有选择任何分析或者分析类型选择错误：%s' % self.option('analysis'))
        if not self.option("abundtable").is_set:
            raise OptionError("请传入物种/功能/基因丰度表格！")
        if not self.option("group").is_set and self.option("group_detail") == "":
            raise OptionError("请输入分组信息!")

    def run_bar_sort_samples(self):
        if self.option("group").is_set:
            self.group_table_path = self.option("group")
        self.sort_samples.set_options({
            "in_otu_table": self.option("abundtable"),
            "group_table": self.group_table_path,
            "method": self.option("add_Algorithm")
        })
        self.sort_samples.on('end', self.set_output)
        self.sort_samples.run()

    def run_circos_sort_samples(self):
        if self.option("group").is_set:
            self.group_table_path = self.option("group")
        self.sort_samples2.set_options({
            "in_otu_table": self.option("abundtable"),
            "group_table": self.group_table_path,
        })
        self.sort_samples2.on('end', self.set_output)
        self.sort_samples2.run()

    def run_heatmap(self):
        if self.option("group").is_set:
            self.group_table_path = self.option("group")
        self.heatmap.set_options({
            "abundtable": self.option("abundtable"),
            "group": self.group_table_path,
            "species_number": self.option("species_number"),
            "method": self.option("method"),
            "sample_method": self.option("sample_method"),
            "add_Algorithm": self.option("add_Algorithm")
        })
        self.heatmap.on('end', self.set_output)
        self.heatmap.run()

    def set_output(self):
        if 'bar' in self.option('analysis'):
            self.linkdir(self.sort_samples.output_dir, 'bar')
        if 'circos' in self.option('analysis'):
            self.linkdir(self.sort_samples2.output_dir, 'circos')
        if 'heatmap' in self.option('analysis'):
            self.linkdir(self.heatmap.output_dir, 'heatmap')
        super(CompositionAnalysisModule, self).end()

    def linkdir(self, dirpath, dirname):
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

    def run(self):
        super(CompositionAnalysisModule, self).run()
        if 'bar' in self.option('analysis'):
            self.run_bar_sort_samples()
        if 'circos' in self.option('analysis'):
            self.run_circos_sort_samples()
        if 'heatmap' in self.option('analysis'):
            self.run_heatmap()

