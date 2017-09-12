# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'

import shutil
import os
from biocluster.module import Module
from biocluster.core.exceptions import OptionError


class CdhitParaModule(Module):
    def __init__(self, work_id):
        super(CdhitParaModule, self).__init__(work_id)
        options = [
            {"name": "first", "type": "int"},  # 第一个文件编号
            {"name": "last", "type": "int"},  # 最后一个文件编号
            {"name": "in_dir", "type": "infile", "format": "sequence.cdhit_cluster_dir"},  # 输入文件夹
            {"name": "identity", "type": "float", "default": 0.95},  # 给出cdhit的参数identity
            {"name": "coverage", "type": "float", "default": 0.9},  # 给出cdhit的参数coverage
            #            {"name": "ou_dir", "type": "infile", "format": "uniGene.build_dir"}  # 输出文件夹
        ]
        self.add_option(options)
        self.compare_single = self.add_tool("cluster.cdhit_compare_single")
        #        self.compare_between = self.add_tool("cluster.cdhit_compare_between")
        self.between_tool = []

    #        self.single_tool=[]

    #    def set_step(self, event):
    #        if 'start' in event['data'].keys():
    #            event['data']['start'].start()
    #        if 'end' in event['data'].keys():
    #            event['data']['end'].finish()
    #        self.step.update()

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def check_options(self):
        if not self.option("first"):
            raise OptionError('必须设定first值')
        if not self.option("last"):
            raise OptionError('必须设定last值')
        if self.option('first') > self.option('last'):
            raise OptionError('设定的开始值大于结束')
        if self.option('first') < 1:
            raise OptionError('first值必须大于等于1')
        if not 0.75 <= self.option("identity") <= 1:
            raise OptionError("identity必须在0.75，1之间")
        if not 0 <= self.option("coverage") <= 1:
            raise OptionError("coverage必须在0,1之间")

    def compare_run(self):
        n = 1
        for i in range(self.option("first"), self.option("last")):
            compare = self.add_tool("cluster.cdhit_compare_between")
            self.step.add_steps('compare_{}'.format(n))
            if self.option("first") >= 2:
                #                self.logger.info(self.option("in_dir").prop['path']+"/gene.geneset.tmp.fa.div-"+str(self.option("first")-1)+"-/o")
                #                self.logger.info(self.option("in_dir").prop['path']+"/gene.geneset.tmp.fa.div-"+str(i)+"-/vs."+str(self.option("first")-2))
                #                self.logger.info(self.option("first")-1)
                #                self.logger.info(i)
                compare.set_options({
                    "database": self.option("in_dir").prop['path'] + "/gene.geneset.tmp.fa.div-" + str(
                        self.option("first") - 1) + "-/o",
                    "query": self.option("in_dir").prop['path'] + "/gene.geneset.tmp.fa.div-" + str(i) + "-/vs." + str(
                        self.option("first") - 2),
                    "dbnum": self.option("first") - 1,
                    "qunum": i,
                    "compare": self.option("in_dir").prop['path'],
                    "identity": self.option("identity"),
                    "coverage": self.option("coverage")
                })
            else:
                opts = {
                    "database": self.option("in_dir").prop['path'] + "/gene.geneset.tmp.fa.div-0-/o",
                    "query": self.option("in_dir").prop['path'] + "/gene.geneset.tmp.fa.div-" + str(i),
                    "dbnum": 0,
                    "qunum": i,
                    "compare": self.option("in_dir").prop['path'],
                    "identity": self.option("identity"),
                    "coverage": self.option("coverage")
                }
                self.logger.info(opts)
                compare.set_options(opts)
            step = getattr(self.step, 'compare_{}'.format(n))
            step.start()
            compare.on("end", self.finish_update, "compare_{}".format(n))
            n += 1
            self.between_tool.append(compare)
        self.logger.info(self.between_tool)
        self.between_tool[0].on('end', self.single_run)
        #        self.on_rely(self.between_tool,self.single_run(i))
        #        if len(self.between_tool) == 1:
        #            self.between_tool[0].on('end', self.single_run)
        #        else:
        for tool in self.between_tool:
            tool.run()
            # self.between_tool[0].on("end",self.single_run(i))
            # self.on_rely(self.between_tool,self.single_run(i))
            # self.step.between_tool[0].start()
            # self.between_tool[0].on(self.single_run(i))
            # self.between_tool[0].run()

    def single_run(self):
        #        single = self.add_tool("cluster.cdhit_compare_single")
        self.step.add_steps('single')
        self.compare_single.set_options({
            "query": self.option("in_dir").prop['path'] + "/gene.geneset.tmp.fa.div-" + str(
                self.option("first")) + "-/vs." + str(self.option("first") - 1),
            "qunum": self.option("first"),
            "compare": self.option("in_dir").prop['path'],
            "identity": self.option("identity"),
            "coverage": self.option("coverage")
        })
        step = getattr(self.step, 'single')
        step.start()
        self.compare_single.on("end", self.finish_update, 'single')
        self.compare_single.run()

    def set_output(self):
        self.end()

    def run(self):
        self.compare_run()
        all_tool = self.between_tool + [self.compare_single]
        self.on_rely(all_tool, self.set_output)
        super(CdhitParaModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        super(CdhitParaModule, self).end()
