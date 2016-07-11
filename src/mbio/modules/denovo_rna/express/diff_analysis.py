# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
#last_modify:2016.07.04

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError


class DiffAnalysisModule(Module):
    def __init__(self, work_id):
        super(DiffAnalysisModule, self).__init__(work_id)
        self.step.add_steps('venn', 'cluster')
        options = [
            {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #基因表达量表
            {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  #差异基因表达量表
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表格
            {"name": "distance_method", "type": "string", "default": "euclidean"},  # 计算距离的算法
            {"name": "log",  "type": "int", "default": 10},  # 画热图时对原始表进行取对数处理，底数为10或2
            {"name": "method", "type": "string", "default": "h_clust"},  # 聚类方法选择
            {"name": "sub_num", "type": "int", "default": 10}  # 子聚类的数目
        ]
        self.add_option(options)
        self.venn = self.add_tool("graph.venn_table")
        self.cluster = self.add_tool("denovo_rna.express.cluster")
        self._end_info = 0

    def check_options(self):
        if not self.option("diff_fpkm").is_set:
            raise OptionError("必须设置输入文件:差异基因fpkm表")
        if self.option("distance_method") not in ("manhattan", "euclidean"):
            raise OptionError("所选距离算法不在提供的范围内")
        if self.option('log') not in (10, 2):
            raise OptionError("所选log底数不在提供的范围内")
        if self.option("method") not in ("h_clust", "kmeans", "both"):
            raise OptionError("所选方法不在范围内")
        if not isinstance(self.option("sub_num"), int):
            raise OptionError("子聚类数目必须为整数")
        if not self.option("group_table").is_set:
            raise OptionError("venn分析的分组文件必须设置")
        return True

    def venn_run(self):
        self.venn.set_options({
                                "otu_table": self.option("diff_fpkm"),
                                "group_table": self.option("group_table")
                                })
        self.venn.on('end', self.set_output, 'venn')
        self.step.venn.start()
        self.venn.run()
        self.step.venn.finish()
        self.step.update()

    def cluster_run(self):
        self.cluster.set_options({
                                   "diff_fpkm": self.option("diff_fpkm"),
                                   "distance_method": self.option("distance_method"),
                                   "log": self.option("log"),
                                   "method": self.option("method"),
                                   "sub_num": self.option("sub_num")
                                   })
        self.cluster.on('end', self.set_output, 'cluster')
        self.step.cluster.start()
        self.cluster.run()
        self.step.cluster.finish()
        self.step.update()

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                if os.path.isfile(newfile):
                    os.remove(newfile)
                else:
                    os.system('rm -r %s' % newfile)
                    self.logger.info('rm -r %s' % newfile)
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            elif os.path.isdir(oldfiles[i]):
                self.logger.info('cp -r %s %s' % (oldfiles[i], newdir))
                os.system('cp -r %s %s' % (oldfiles[i], newdir))

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'venn':
            self.linkdir(obj.output_dir, 'venn')
            self._end_info += 1
        elif event['data'] == 'cluster':
            self.linkdir(obj.output_dir, 'cluster')
            self._end_info += 1
        else:
            pass
        if self._end_info == 2:
            self.end()

    def run(self):
        super(DiffAnalysisModule, self).run()
        self.venn_run()
        self.cluster_run()
        # self.on_rely([self.venn, self.cluster], self.end)

    def end(self):
        repaths = [
                    [".", "", "差异表达模块结果输出目录"],
                    ["./venn", "", "venn分析结果输出目录"],
                    ["./cluster", "", "cluster分析结果输出目录"],
                    [r"venn/venn_table.xls", "xls", "venn分析结果"]
                    ]
        if self.option('method') in ('both', 'h_clust'):
            repaths += [
                         [r"hc_gene_order", "txt", "按基因聚类的基因排序列表"],
                         [r"hc_sample_order", "txt", "按样本聚类的样本排序列表"],
                         [r"hclust_heatmap.xls", "xls", "层级聚类热图数据"]
                         ]
        elif self.option('method') in ('both', 'kmeans'):
            repaths += [
                         [r"kmeans_heatmap.xls", "xls", "层级聚类热图数据"]
                         ]
        regexps = [
            [r"^subcluster_", "xls", "子聚类热图数据"]
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(DiffAnalysisModule, self).end()
