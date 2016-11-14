# -*- coding: utf-8 -*-
# __author__ = 'JieYao'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
import subprocess
from biocluster.core.exceptions import OptionError

class PpinetworkTopologyAgent(Agent):
    """
    需要calc_ppi.py
    version 1.0
    author: JieYao
    last_modified: 2016.8.15
    """
    
    def __init__(self, parent):
        super(PpinetworkTopologyAgent, self).__init__(parent)
        options = [
            {"name": "ppitable", "type": "string"},
            {"name": "combine_score", "type": "int", "default": 600}
            ]
        self.add_option(options)
        self.step.add_steps('PpinetworkAnalysis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.PpinetworkAnalysis.start()
        self.step.update()
        
    def step_end(self):
        self.step.PpinetworkAnalysis.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('ppitable'):
            raise OptionError('必须提供PPI网络表')
        if self.option('combine_score') > 1000 or self.option('combine_score') < 0:
            raise OptionError("combine_score值超出范围")
        if not os.path.exists(self.option('ppitable')):
            raise OptionError('PPI网络表路径错误')
        ppi_list = open( self.option('ppitable'), "r").readline().strip().split("\t")
        if "combined_score" not in ppi_list:
            raise OptionError("PPI网络表缺少结合分数")
        if ("from" not in ppi_list) or ("to" not in ppi_list):
            raise OptionError("PPI网络缺少相互作用蛋白信息")
        #:  
        #    eval(self.option('cut'))
        #except:
        #    raise OptionError("Cut参数值异常，无法转换")
        return True
    
    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = '5G'
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
                [".", "", "PPI网络分析结果输出目录"],
                ["./protein_interaction_network_centrality.txt", "txt", "PPI网络中心系数表"],
                ["./protein_interaction_network_clustering.txt", "txt", "PPI网络节点聚类系数表"],
                ["./protein_interaction_network_transitivity.txt", "txt", "PPI网络传递性"],
                ["./protein_interaction_network_by_cut.txt", "txt", "combined_score值约束后的PPI网络"],
                ["./protein_interaction_network_degree_distribution.txt", "txt", "PPI网络度分布表"],
                ["./protein_interaction_network_node_degree.txt", "txt", "PPI网络节点度属性表"]
                ])
        print self.get_upload_files()
        super(PpinetworkTopologyAgent, self).end()
        

class PpinetworkTopologyTool(Tool):
    def __init__(self, config):
        super(PpinetworkTopologyTool, self).__init__(config)
        self._version = "1.0.1"
        self.cmd_path = self.config.SOFTWARE_DIR + "/bioinfo/rna/scripts/calc_ppi.py"
        self.ppi_table = self.option('ppitable')        
        self.out_files = ['protein_interaction_network_centrality.txt', 'protein_interaction_network_clustering.txt', 'protein_interaction_network_transitivity.txt', 'protein_interaction_network_by_cut.txt', 'protein_interaction_network_degree_distribution.txt',  'protein_interaction_network_node_degree.txt']

    def run(self):
        """
        运行
        """
        super(PpinetworkTopologyTool, self).run()
        self.run_ppi_network_py()
        
    def run_ppi_network_py(self):
        """
        运行calc_ppi.py
        """
        real_ppi_table = self.ppi_table        
        cmd = self.config.SOFTWARE_DIR + '/program/Python/bin/python '
        cmd += self.cmd_path
        cmd += " -i %s -o %s " %(real_ppi_table, self.work_dir + '/PPI_result')
        if self.option('combine_score'):
            cmd += " -c %s" %(self.option('combine_score'))
        print cmd
        self.logger.info("开始运行calc_ppi.py")

        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('PPI_Network计算完成')
        except subprocess.CalledProcessError:
            self.logger.info('PPI_Network计算失败')
            self.set_error("运行calc_ppi.py失败")
        allfiles = self.get_filesname()
        for i in range(len(self.out_files)):
            self.linkfile(allfiles[i], self.out_files[i])
        self.end()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile 资源文件路径
        :param newname 新的文件名
        :return
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def get_filesname(self):
        filelist = os.listdir(self.work_dir + '/PPI_result')
        files_status = [None, None, None, None, None, None]
        for paths,d,filelist in os.walk(self.work_dir + '/PPI_result'):
            for filename in filelist:
                name = os.path.join(paths, filename)
                for i in range(len(self.out_files)):
                    if self.out_files[i] in name:
                        files_status[i] = name
        for i in range(len(self.out_files)):
            if not files_status[i]:
                self.set_error('未知原因，结果文件生成出错或丢失')
        return files_status
