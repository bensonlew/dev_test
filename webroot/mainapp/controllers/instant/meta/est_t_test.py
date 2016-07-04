# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import os
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_group_table_by_detail
from mbio.instant.to_files.export_est import export_est_table
from mbio.files.meta.otu.group_table import GroupTableFile


class EstTTest(MetaController):
    """

    """
    def __init__(self):
        super(EstTTest, self).__init__()

    def POST(self):
        myReturn = super(EstTTest, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        # print(data.group_detail)
        table_dict = json.loads(data.group_detail)
        if not isinstance(table_dict, dict):
            info = {"success": False, "info": "传入的group_detail不是字典"}
            return json.dumps(info)
        if len(table_dict) < 2:
            info = {"success": False, "info": "请选择至少两组及以上的分组"}
            return json.dumps(info)
        options = {
            "est_table": os.path.join(self.work_dir, "estimators_for_t.xls"),
            "groupPath": os.path.join(self.work_dir, "groupTable.xls"),
            "otu_id": data.otu_id,
            "est_id": data.est_id,
            "group_detail": data.group_detail,
            "group_id": data.group_id
        }
        self.setOptions(options)
        self.create_files()
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(EstTTest, self).run()
        self.addMongo()
        self.end()

    def create_files(self):
        """
        生成文件
        """
        est_table = export_est_table(self.options["est_id"], self.work_dir)
        groupPath = export_group_table_by_detail(self.options["group_id"], self.data.group_detail, self.options["groupPath"])
        groupFile = GroupTableFile()
        groupFile.set_path(groupPath)
        groupFile.get_info()
        groupFile.check()

    def addMongo(self):
        """
        调入相关的mongo表，包括基本表和detail表
        """
        api_est_t_test = self.api.api("meta.est_t_test")
        name = "est_t_test_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        est_t_id = api_est_t_test.add_est_t_test_collection(name)
        output = os.path.join(self.work_dir, "output")
        for f in os.listdir(output):
            self.logger.info(os.path.join(output, f))
            api_est_t_test.add_est_t_test_detail(os.path.join(output, f), est_t_id)
        # api_est_t_test.add_est_t_test_detail(self.options['est_table'], est_t_id)
        self.appendSgStatus(api_est_t_test, est_t_id, "sg_alpha_est_t_test", "")
        self.logger.info("Mongo数据库导入完成")
