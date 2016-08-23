# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
# last_modify:2016.07.04

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError


class DiffAnalysisModule(Module):
    def __init__(self, work_id):
        super(DiffAnalysisModule, self).__init__(work_id)
        self.step.add_steps('cluster', 'network', 'go_rich', 'kegg_rich')
        options = [
            {"name": "analysis", "type": "string", "default": "cluster,network,kegg_rich,go_rich"},  # 选择要做的分析
            {"name": "diff_fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 差异基因表达量表
            {"name": "gene_file", "type": "infile", "format": "denovo_rna.express.gene_list"},
            {"name": "distance_method", "type": "string", "default": "euclidean"},  # 计算距离的算法
            {"name": "log", "type": "int", "default": 10},  # 画热图时对原始表进行取对数处理，底数为10或2
            {"name": "method", "type": "string", "default": "hclust"},  # 聚类方法选择
            {"name": "sub_num", "type": "int", "default": 10},  # 子聚类的数目
            {"name": "softpower", "type": "int", "default": 9},
            {"name": "dissimilarity", "type": "float", "default": 0.25},
            {"name": "module", "type": "float", "default": 0.6},
            {"name": "network", "type": "float", "default": 0.6},
            {"name": "kegg_path", "type": "infile", "format": "denovo_rna.express.gene_list"},  # KEGG的pathway文件
            {"name": "diff_list", "type": "infile", "format": "denovo_rna.express.gene_list_dir"},  # 两两样本/分组的差异基因文件
            {"name": "correct", "type": "string", "default": "BH"},  # 多重检验校正方法
            {"name": "all_list", "type": "infile", "format": "annotation.kegg.kegg_list"},
            {"name": "go_list", "type": "infile", "format": "annotation.go.go_list"},  # test
            {"name": "go_level_2", "type": "infile", "format": "annotation.go.level2"}
        ]
        self.add_option(options)
        self.venn = self.add_tool("graph.venn_table")
        self.cluster = self.add_tool("denovo_rna.express.cluster")
        self.network = self.add_tool("denovo_rna.express.network")
        self._end_info = 0

    def check_options(self):
        if not self.option("diff_fpkm").is_set:
            raise OptionError("必须设置输入文件:差异基因fpkm表")
        if not self.option("gene_file").is_set:
            raise OptionError("必须设置输入文件:基因名字列表")
        if self.option("distance_method") not in ("manhattan", "euclidean"):
            raise OptionError("所选距离算法不在提供的范围内")
        if self.option('log') not in (10, 2):
            raise OptionError("所选log底数不在提供的范围内")
        if self.option("method") not in ("hclust", "kmeans", "both"):
            raise OptionError("所选方法不在范围内")
        if not isinstance(self.option("sub_num"), int):
            raise OptionError("子聚类数目必须为整数")
        if self.option("softpower") > 20 or self.option("softpower") < 1:
            raise OptionError("softpower值超出范围")
        if self.option('dissimilarity') > 1 or self.option("dissimilarity") < 0:
            raise OptionError("模块dissimilarity相异值超出范围")
        if self.option('module') > 1 or self.option("module") < 0:
            raise OptionError("模块module相异值超出范围")
        if self.option('network') > 1 or self.option("network") < 0:
            raise OptionError("模块network相异值超出范围")
        return True

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

    def network_run(self):
        self.network.set_options({
            "diff_fpkm": self.option("diff_fpkm"),
            "gene_file": self.option("gene_file"),
            "softpower": self.option("softpower"),
            "dissimilarity": self.option("dissimilarity"),
            "module": self.option("module"),
            "network": self.option("network")
        })
        self.network.on('end', self.set_output, 'network')
        self.step.network.start()
        self.network.run()
        self.step.network.finish()
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
                    # self.logger.info('rm -r %s' % newfile)
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            elif os.path.isdir(oldfiles[i]):
                # self.logger.info('cp -r %s %s' % (oldfiles[i], newdir))
                os.system('cp -r %s %s' % (oldfiles[i], newdir))

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'venn':
            self.linkdir(obj.output_dir, 'venn')
            self._end_info += 1
        elif event['data'] == 'cluster':
            self.linkdir(obj.output_dir, 'cluster')
            self._end_info += 1
        elif event['data'] == 'network':
            self.linkdir(obj.output_dir, 'network')
            self._end_info += 1
        else:
            pass
        if self.option('group_table').is_set:
            if self._end_info == 3:
                self.end()
        elif self._end_info == 2:
            self.end()

    def run(self):
        super(DiffAnalysisModule, self).run()
        if self.option('group_table').is_set:
            self.venn_run()
        self.cluster_run()
        self.network_run()
        # self.on_rely([self.venn, self.cluster], self.end)

    def end(self):
        repaths = [
                    [".", "", "差异表达模块结果输出目录"],
                    ["./venn", "", "venn分析结果输出目录"],
                    ["./cluster", "", "cluster分析结果输出目录"],
                    ["./network", "", "network分析结果输出目录"],
                    ["venn/venn_table.xls", "xls", "venn分析结果"],
                    ["all_edges.txt", "txt", "edges结果信息"],
                    ["all_nodes.txt ", "txt", "nodes结果信息"],
                    ["removeGene.xls ", "xls", "移除的基因信息"],
                    ["removeSample.xls ", "xls", "移除的样本信息"],
                    ["softPower.pdf", "pdf", "softpower相关信息"],
                    ["ModuleTree.pdf", "pdf", "ModuleTree图"],
                    ["eigengeneClustering.pdf", "pdf", "eigengeneClustering图"],
                    ["eigenGeneHeatmap.pdf", "pdf", "eigenGeneHeatmap图"],
                    ["networkHeatmap.pdf", "pdf", "networkHeatmap图"],
                    ["sampleClustering.pdf", "pdf", "sampleClustering图"]
                    ]
        if self.option('method') in ('both', 'hclust'):
            repaths += [
                         ["./cluster/hclust", "", "hclust分析结果输出目录"],

                         ["./cluster/hclust/hc_gene_order", "txt", "按基因聚类的基因排序列表"],
                         ["./cluster/hclust/hc_sample_order", "txt", "按样本聚类的样本排序列表"],
                         ["./cluster/hclust/hclust_heatmap.xls", "xls", "层级聚类热图数据"]
                         ]
        elif self.option('method') in ('both', 'kmeans'):
            repaths += [
                         [r"./cluster/kmeans/kmeans_heatmap.xls", "xls", "层级聚类热图数据"]
                         ]
        regexps = [
                    [r"subcluster_", "xls", "子聚类热图数据"],
                    [r"network/CytoscapeInput.*", "txt", "Cytoscape作图数据"]
                    ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(DiffAnalysisModule, self).end()
