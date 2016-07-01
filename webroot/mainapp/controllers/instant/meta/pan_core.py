# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import os
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level, export_group_table
from mbio.files.meta.otu.otu_table import OtuTableFile
from mbio.files.meta.otu.group_table import GroupTableFile


class PanCore(MetaController):
    def __init__(self):
        super(PanCore, self).__init__()

    def POST(self):
        myReturn = super(PanCore, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        options = {
            "otuPath": os.path.join(self.work_dir, "otuTable.xls"),
            "groupPath": os.path.join(self.work_dir, "groupTable.xls"),
            "level": data.level_id,
            "groupId": data.group_id,
            "otuId": data.otu_id
        }
        self.setOptions(options)
        self.create_files()
        num_lines = sum(1 for line in open(self.option["otuPath"]))
        if num_lines < 11:
            info = {"success": False, "info": "Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！"}
            return info
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(PanCore, self).run()
        self.addMongo()
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
        super(PanCore, self).end()

    def create_files(self):
        """
        生成文件
        """
        otuPath = export_otu_table_by_level(self.option["otuId"], self.option["otuPath"], self.option["level"])
        otuFile = OtuTableFile()
        otuFile.set_path(otuPath)
        otuFile.get_info()
        otuFile.check()
        groupPath = export_group_table(self.option["groupId"], self.data.category_name, self.option["groupPath"])
        groupFile = GroupTableFile()
        groupFile.set_path(groupPath)
        groupFile.get_info()
        groupFile.check()
        if groupFile.prop["is_empty"]:
            self.option["groupPath"] = ""
