## !/mnt/ilustre/users/sanger-dev/app/program/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "hongdongxuan"
#last_modify:20160912

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re


class MapAgent(Agent):
    """
    调用map.r脚本，进行将基因ID mapping 到STRINGid
    version v1.0
    author: hongdongxuan
    last_modify: 2016.09.12
    """
    def __init__(self, parent):
        super(MapAgent, self).__init__(parent)
        options = [
            {"name": "diff_exp", "type": "infile", "format": "ref_rna.protein_regulation.xls"},  #差异基因表达详情表
            {"name": "species", "type": "int", "default": 9606},
            {"name": "species_list", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps("map")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.map.start()
        self.step.update()

    def stepfinish(self):
        self.step.map.finish()
        self.step.update()


    def check_options(self):
        """
        重写参数检测函数
        :return:
        """
        # species_list = [9606,3711,4932]
        if not self.option("diff_exp").is_set:
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
                    ])
        result_dir.add_regexp_rules([
            ["diff_exp_mapped.txt", "txt", "含有STRINGid结果信息"],

        ])
        super(MapAgent, self).end()


class MapTool(Tool):
    """
    将基因ID mapping 到STRINGid tool
    """
    def __init__(self, config):
        super(MapTool, self).__init__(config)
        self._version = '1.0.1'
        self.r_path = 'program/R-3.3.1/bin/Rscript'
        self.script_path = '/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/'

    def run_map(self):
        one_cmd = self.r_path + " %smap.r %s %s %s" % (self.script_path, self.option('diff_exp').prop['path'], self.option('species'), 'PPI_result')
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
        super(MapTool, self).run()
        self.run_map()
        self.set_output()
        self.end()
