# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import os
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level, export_group_table
from mbio.files.meta.otu.otu_table import OtuTableFile
from mbio.files.meta.otu.group_table import GroupTableFile


class TwoGroup(MetaController):
    def __init__(self):
        super(TwoGroup, self).__init__()

    def POST(self):
        myReturn = super(TwoGroup, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        options = {
            "otu_file": data.otu_id,
            "level": int(data.level_id),
            "test": data.test,
            "group_file": data.group_id,
            "group_detail": data.group_detail,
            "correction": data.correction,
            "ci": float(data.ci),
            "type": data.type,
            "group_name": G().get_group_name(data.group_id),
            "two_group_id": str(two_group_id),
            "coverage": data.coverage
        }
        self.setOptions(options)
        self.create_files()
        success = self.check_options(data)
        if success:
            info = {"success": False, "info": '+'.join(success)}
            return json.dumps(info)
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(TwoGroup, self).run()
        self.addMongo()
        self.end()

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'level_id', 'group_detail', 'group_id', 'ci', 'correction', 'type', 'test', 'coverage', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if int(data.level_id) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append("level_id不在范围内")
        if float(data.ci) > 1 or float(data.ci) < 0:
            success.append("显著性水平不在范围内")
        if data.correction not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr", "none"]:
            success.append("多重检验方法不在范围内")
        if data.type not in ["two.side", "greater", "less"]:
            success.append("检验类型不在范围内")
        if float(data.ci) > 1 or float(data.ci) < 0:
            success.append("显著性水平不在范围内")
        if data.test not in ["chi", "fisher", "kru_H", "mann", "anova", "student", "welch"]:
            success.append("所选的分析检验方法不在范围内")
        if float(data.coverage) not in [0.90, 0.95, 0.98, 0.99, 0.999]:
            success.append('置信区间的置信度coverage不在范围值内')
        table_dict = json.loads(data.group_detail)
        if not isinstance(table_dict, dict):
            success.append("传入的table_dict不是一个字典")
        return success

    def addMongo(self):
        """
        调入相关的mongo表，包括基本表和detail表
        """
        apiTwoGroup = self.api.api("meta.pan_core")
        name1 = "pan_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        name2 = "core_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        panId = apiTwoGroup.CreateTwoGroupTable(1, name1)
        coreId = apiTwoGroup.CreateTwoGroupTable(2, name2)
        panPath = os.path.join(self.work_dir, "output", "pan.richness.xls")
        corePath = os.path.join(self.work_dir, "output", "core.richness.xls")
        apiTwoGroup.AddTwoGroupDetail(panPath, panId)
        apiTwoGroup.AddTwoGroupDetail(corePath, coreId)
        self.appendSgStatus(apiTwoGroup, panId, "sg_otu_pan_core", "")
        self.appendSgStatus(apiTwoGroup, coreId, "sg_otu_pan_core", "")
        self.logger.info("Mongo数据库导入完成")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["core.richness.xls", "xls", "core 表格"],
            ["pan.richness.xls", "xls", "pan 表格"]
        ])
        self.uploadFiles("pan_core")
        super(TwoGroup, self).end()

    def create_files(self):
        """
        生成文件
        """
        otuPath = export_otu_table_by_level(self.options["otuId"], self.options["otuPath"], self.options["level"])
        otuFile = OtuTableFile()
        otuFile.set_path(otuPath)
        otuFile.get_info()
        otuFile.check()
        groupPath = export_group_table(self.options["groupId"], self.data.category_name, self.options["groupPath"])
        groupFile = GroupTableFile()
        groupFile.set_path(groupPath)
        groupFile.get_info()
        groupFile.check()
        if groupFile.prop["is_empty"]:
            self.options["groupPath"] = ""
