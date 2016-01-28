# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""otu样本序列数抽平"""
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import export_otu_table


class OtuSubsampleWorkflow(Workflow):
    """
    报告中调用otu抽平时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(OtuSubsampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "size", "type": "int", "default": "min"},
            {"name": "update_info", "type": "string"},
            {"name": "output_otu_id", "type": "string"}  # 结果的otu id
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.task = self.add_tool("meta.otu.sub_sample")

    def run(self):
        self.task.set_options({
                "in_otu_table": export_otu_table(self.option("input_otu_id"), "subsample_size_"+self.option("size"), self.task.work_dir, self.task),
                "size": self.option("size")
            })
        self.task.on('end', self.set_db)
        self.task.run()
        super(OtuSubsampleWorkflow, self).run()

    def set_db(self):
        """
        保存结果otu表到mongo数据库中
        """
        api_otu = self.api.meta
        otu_path = self.task.output_dir+"/otu_taxon.xls"
        if not os.path.isfile(otu_path):
            raise Exception("找不到报告文件:{}".format(otu_path))
        api_otu.add_otu_table(otu_path, self.option("output_otu_id"))
        self.end()
