# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# last_modify:2016.09.22

from biocluster.module import Module
import os
import types
from biocluster.core.exceptions import OptionError


class PpinetworkAnalysisModule(Module):
    def __init__(self, work_id):
        super(PpinetworkAnalysisModule, self).__init__(work_id)
        self.step.add_steps('map', 'ppinetwork_predict', 'ppinetwork_topology')
        options = [
            {"name": "diff_exp", "type": "string"},  # 差异基因详情表，表中含有logFC
            {"name": "species", "type": "int", "default": 9606},  #设置物种
            {"name": "combine_score", "type": "int", "default": 600},  # 设定蛋白质与蛋白质之间的相互作用可能性的阈值
            {"name": "logFC", "type": "float", "default": 0.2},  # 设定logFC系数的阈值
            {"name": "FDR", "type": "float", "default": 0.05},
            {"name": "species_list", "type": "string"}
        ]
        self.add_option(options)
        self.map = self.add_tool("ref_rna.protein_regulation.map")
        self.ppinetwork_predict = self.add_tool("ref_rna.protein_regulation.ppinetwork_predict")
        self.ppinetwork_topology = self.add_tool("ref_rna.protein_regulation.ppinetwork_topology")
        self._end_info = 0

    def check_options(self):
        if not self.option('diff_exp'):
            raise OptionError("必须输入含有gene_id的差异基因表xls")
        if not self.option('species_list'):
            raise OptionError('必须提供物种 taxon id 表')
        if not os.path.exists(self.option('species_list')):
            raise OptionError('species_list文件路径有错误')
        with open(self.option('species_list'), "r") as f:
            data = f.readlines()
            species_list = []
            for line in data:
                temp = line.rstrip().split("\t")
                species_list += [eval(temp[0])]
            # print species_list
        if self.option('species') not in species_list:
            raise OptionError("物种不存在,请输入正确的物种taxon_id")
        if self.option('combine_score') > 1000 or self.option('combine_score') < 0:
            raise OptionError("combine_score值超出了范围")
        if self.option('logFC') > 100 or self.option('logFC') < -100:
            raise OptionError("logFC值超出范围")

        return True

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def map_run(self):
        self.map.set_options({
            "diff_exp": self.option("diff_exp"),
            "species": self.option("species"),
            "FDR": self.option("FDR"),
            "species_list": self.option("species_list")
        })
        self.map.on('end', self.set_output, 'map')
        self.map.on('start', self.set_step, {'start': self.step.map})
        self.map.on('end', self.set_step, {'end': self.step.map})
        self.map.on('end', self.ppinetwork_predict_run)
        self.map.run()

    def ppinetwork_predict_run(self):
        diff_exp_mapped_table = os.path.join(self.work_dir, "Map/output/diff_exp_mapped.txt")
        self.ppinetwork_predict.set_options({
            "diff_exp_mapped": diff_exp_mapped_table,
            "species": self.option("species"),
            "combine_score": self.option("combine_score"),
            "logFC": self.option("logFC"),
            "species_list": self.option("species_list")
        })
        self.ppinetwork_predict.on('end', self.set_output, 'ppinetwork_predict')
        self.ppinetwork_predict.on('start', self.set_step, {'start': self.step.ppinetwork_predict})
        self.ppinetwork_predict.on('end', self.set_step, {'end': self.step.ppinetwork_predict})
        self.ppinetwork_predict.on('end', self.ppinetwork_topology_run)
        self.ppinetwork_predict.run()

    def ppinetwork_topology_run(self):
        ppi_table = os.path.join(self.work_dir, "PpinetworkPredict/output/interaction.txt")
        self.ppinetwork_topology.set_options({
            "ppitable": ppi_table,
            "combine_score": self.option("combine_score")
        })
        self.ppinetwork_topology.on('end', self.set_output, 'ppinetwork_topology')
        self.ppinetwork_topology.on('end', self.end)
        self.ppinetwork_topology.run()

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
        if event['data'] == 'map':
            self.linkdir(obj.output_dir, 'map')
        elif event['data'] == 'ppinetwork_predict':
            self.linkdir(obj.output_dir, 'ppinetwork_predict')
        elif event['data'] == 'ppinetwork_topology':
            self.linkdir(obj.output_dir, 'ppinetwork_topology')
        else:
            pass

    def run(self):
        super(PpinetworkAnalysisModule, self).run()
        mapped_table = os.path.join(self.work_dir, "Map/output/diff_exp_mapped.txt")
        if os.path.isfile(mapped_table):
            self.ppinetwork_predict_run()
        else:
            self.map_run()


    def end(self):
        repaths = [
            [".", "", "蛋白质互作网络结果输出目录"],
            ["interaction.txt", "txt", "edges结果文件信息"],
            ["all_nodes.txt", "txt", "node结果信息"],
            ["network_stats.txt", "txt", "网络统计结果信息"],
            ["diff_exp_mapped.txt", "txt", "含有STRINGid的差异基因文件"],
            ["protein_interaction_network_centrality.txt", "txt", "PPI网络中心系数表"],
            ["protein_interaction_network_clustering.txt", "txt", "PPI网络节点聚类系数表"],
            ["protein_interaction_network_transitivity.txt", "txt", "PPI网络传递性"],
            ["protein_interaction_network_degree_distribution.txt", "txt", "PPI网络度分布表"],
            ["protein_interaction_network_by_cut.txt", "txt", "combined_score值约束后的PPI网络"],
            ["protein_interaction_network_node_degree.txt", "txt", "PPI网络节点度属性表"]
        ]

        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        # sdir.add_regexp_rules(regexps)
        super(PpinetworkAnalysisModule, self).end()
