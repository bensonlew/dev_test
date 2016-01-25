# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""otu样本序列数抽平"""

# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.workflow import Workflow
import os


class OtuSubsampleWorkflow(Workflow):
    """
    报告中调用otu抽平时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(OtuSubsampleWorkflow, self).__init__(wsheet_object)

    def run(self):
        task = self.add_tool("meta.otu.sub_sample")
        if self.UPDATE_STATUS_API:
            task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        task.set_options(self._sheet.options())
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(OtuSubsampleWorkflow, self).run()

    def set_db(self):
        """
        保存结果otu表到mongo数据库中
        """
        api_otu = self.api.meta
        otu_path = self.output_dir+"/otu_taxon.xls"
        if not os.path.isfile(otu_path):
            raise Exception("找不到报告文件:{}".format(otu_path))
        api_otu.add_otu_table(otu_path, 9, )
        self.end()

