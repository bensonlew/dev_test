# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import os
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level, export_group_table
from mbio.files.meta.otu.otu_table import OtuTableFile
from mbio.files.meta.otu.group_table import GroupTableFile


class PanCoreTest(MetaController):
    def __init__(self):
        super(PanCoreTest, self).__init__()

    def POST(self):
        myReturn = super(PanCoreTest, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        options = {
            "in_otu_table": os.path.join(self.work_dir, "otuTable.xls"),
            "group_table": os.path.join(self.work_dir, "groupTable.xls"),
            "category_name": data.group_id,
            "level": data.level_id,
            "pan_id": "577478e80e6da9c51236b883",
            "core_id": "577478e80e6da9c51236b884"
        }
        self.setOptions(options)
        self.create_files()
        num_lines = sum(1 for line in open(self.option["in_otu_table"]))
        if num_lines < 11:
            info = {"success": False, "info": "Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！"}
            return info
        self.importWorkflow("mbio.workflows.meta.report.pan_core_test")
        self.run()
        return self.returnInfo

    def run(self):
        super(PanCoreTest, self).run()
        # self.addMongo()
        self.end()

    def addMongo(self):
        """
        调入相关的mongo表，包括基本表和detail表
        """
        apiPanCore = self.api.api("meta.pan_core")
        name1 = "pan_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        name2 = "core_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        panId = apiPanCore.CreatePanCoreTable(1, name1)
        coreId = apiPanCore.CreatePanCoreTable(2, name2)
        panPath = os.path.join(self.work_dir, "output", "pan.richness.xls")
        corePath = os.path.join(self.work_dir, "output", "core.richness.xls")
        apiPanCore.AddPanCoreDetail(panPath, panId)
        apiPanCore.AddPanCoreDetail(corePath, coreId)
        self.appendSgStatus(apiPanCore, panId, "sg_otu_pan_core", "")
        self.appendSgStatus(apiPanCore, coreId, "sg_otu_pan_core", "")
        self.logger.info("Mongo数据库导入完成")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["core.richness.xls", "xls", "core 表格"],
            ["pan.richness.xls", "xls", "pan 表格"]
        ])
        self.uploadFiles("pan_core")
        super(PanCoreTest, self).end()

    def create_files(self):
        """
        生成文件
        """
        otuPath = export_otu_table_by_level(self.data.otu_id, self.option["in_otu_table"], self.option["level"])
        otuFile = OtuTableFile()
        otuFile.set_path(otuPath)
        otuFile.get_info()
        otuFile.check()
        groupPath = export_group_table(self.data.group_id, self.data.category_name, self.option["group_table"])
        groupFile = GroupTableFile()
        groupFile.set_path(groupPath)
        groupFile.get_info()
        groupFile.check()
