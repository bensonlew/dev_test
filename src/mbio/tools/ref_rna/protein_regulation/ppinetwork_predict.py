## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
#last_modify:20160913

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re

class PpinetworkPredictAgent(Agent):
    """
    调用PPInetwork_predict.r脚本，进行蛋白质相互组预测
    version v1.0
    author: hongdongxuan
    last_modify: 2016.09.13
    """
    def __init__(self, parent):
        super(PpinetworkPredictAgent, self).__init__(parent)
        options = [
            {"name": "diff_exp_mapped", "type": "infile", "format": "ref_rna.protein_regulation.txt"},  #差异基因表达详情表
            {"name": "species", "type": "int", "default": 9606},
            {"name": "combine_score",  "type": "int", "default": 600},
            {"name": "logFC", "type": "float", "default": 0.2},
            {"name": "species_list", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("Ppinetwork")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.Ppinetwork.start()
        self.step.update()

    def stepfinish(self):
        self.step.Ppinetwork.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # species_list = [9606,3711,4932]
        if not self.option('diff_exp_mapped').is_set:
            raise OptionError("必须输入含有STRINGid的差异基因表")
        if self.option('combine_score') > 1000 or self.option('combine_score') < 0:
            raise OptionError("combine_score值超出范围")
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
        if self.option('logFC') > 100 or self.option('logFC') < -100:
            raise OptionError("logFC值超出范围")
        return True

    def set_resource(self):
        """
        设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
        :return:
        """
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            #["interaction.txt", "txt", "edges结果信息"],
            #["all_nodes.txt ", "txt", "nodes结果信息"],
        ])
        result_dir.add_regexp_rules([
            ["interaction.txt", "txt", "edges结果信息"],
            ["all_nodes.txt ", "txt", "nodes属性结果信息"],
            ["network_stats.txt", "txt", "网络统计结果信息"],
        ])
        super(PpinetworkPredictAgent, self).end()


class PpinetworkPredictTool(Tool):
    """
    蛋白质互作组预测tool
    """
    def __init__(self, config):
        super(PpinetworkPredictTool, self).__init__(config)
        self._version = '1.0.1'
        self.r_path = 'program/R-3.3.1/bin/Rscript'
        self.script_path = '/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/'

    def run_PPI(self):
        one_cmd = self.r_path + " %sPPInetwork_predict.r %s %s %s %s %s" % (self.script_path, self.option('diff_exp_mapped').prop['path'], self.option('species'), 'PPI_result', self.option('combine_score'), self.option('logFC'))
        self.logger.info(one_cmd)
        self.logger.info("开始运行one_cmd")
        cmd = self.add_command("one_cmd", one_cmd).run()
        self.wait(cmd)
        if cmd.return_code == 0:
            self.logger.info("运行one_cmd成功")
        else:
            self.logger.info("运行one_cmd出错")

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir + '/PPI_result/')
        for f in results:
            if re.search(r'.*Rdata$', f):
                pass
            else:
                os.link(self.work_dir + '/PPI_result/' + f, self.output_dir + '/' + f)
        self.logger.info('设置文件夹路径成功')


    def run(self):
        super(PpinetworkPredictTool, self).run()
        self.run_PPI()
        self.set_output()
        self.end()
