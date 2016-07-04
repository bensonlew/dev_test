# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import os
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level
from mbio.files.meta.otu.otu_table import OtuTableFile


class Estimators(MetaController):
    """

    """
    ESTIMATORS = ['ace', 'bergerparker', 'boneh', 'bootstrap', 'bstick', 'chao', 'coverage', 'default', 'efron',
                  'geometric', 'goodscoverage', 'heip', 'invsimpson', 'jack', 'logseries', 'npshannon', 'nseqs',
                  'qstat', 'shannon', 'shannoneven', 'shen', 'simpson', 'simpsoneven', 'smithwilson', 'sobs', 'solow']

    def __init__(self):
        super(Estimators, self).__init__()

    def POST(self):
        myReturn = super(Estimators, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        for index in data.index_type.split(','):
            if index not in self.ESTIMATORS:
                info = {"success": False, "info": "指数类型不正确{}".format(index)}
                return json.dumps(info)
        options = {
            "otu_table": os.path.join(self.work_dir, "otu_table.xls"),
            "otu_id": data.otu_id,
            "indices": data.index_type,
            "level": data.level_id
        }
        self.setOptions(options)
        self.create_files()
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(Estimators, self).run()
        self.addMongo()
        self.end()

    def addMongo(self):
        """
        调入相关的mongo表，包括基本表和detail表
        """
        apiEstimators = self.api.api("meta.estimators")
        est_name = "estimators_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        est_id = apiEstimators.add_est_collection(est_name)
        est_path = os.path.join(self.work_dir, "output", "estimators.xls")
        apiEstimators.add_est_detail(est_path, est_id)
        self.appendSgStatus(apiEstimators, est_id, "sg_alpha_diversity", "")
        self.logger.info("Mongo数据库导入完成")

    def create_files(self):
        """
        生成文件
        """
        otu_table = export_otu_table_by_level(self.options["otu_id"], self.options["otu_table"], self.options["level"])
        otuFile = OtuTableFile()
        otuFile.set_path(otu_table)
        otuFile.get_info()
        otuFile.check()
