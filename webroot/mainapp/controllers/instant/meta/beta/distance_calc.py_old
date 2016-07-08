# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import os
import datetime
import json
import types
from bson.objectid import ObjectId
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level
from mbio.files.meta.otu.otu_table import OtuTableFile
from bson.errors import InvalidId



class DistanceCalc(MetaController):
    class Sheet(object):
        def __init__(self, bindObject):
            self.id = bindObject._taskId
            self.project_sn = bindObject._projectSn

    def __init__(self):
        super(DistanceCalc, self).__init__()

    def POST(self):
        myReturn = super(DistanceCalc, self).POST()
        self.sheet = self.Sheet(self)
        if myReturn:
            return myReturn
        data = web.input()
        self.params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'distance_algorithm': data.distance_algorithm,
            'submit_location': data.submit_location
            }
        default_argu = ['otu_id', 'level_id', 'distance_algorithm', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'distance_algorithm': data.distance_algorithm,
            'submit_location': data.submit_location
            }
        option = {
            'otu_fp': os.path.join(self.work_dir, 'temp_otu.xls'),
            'otu_id': data.otu_id,
            'method': data.distance_algorithm,
            'level_id': data.level_id
            }
        self.setOptions(option)
        self.create_files()
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(DistanceCalc, self).run()
        if not self.instantModule.success:
            info = {'success': False, 'info': self.instantModule.damageinfo}
            self.returnInfo = json.dumps(info)
            return False
        self.addMongo()
        self.end()
        return True

    def addMongo(self):
        """
        调入相关的mongo表，包括基本表和detail表
        """

        apiDistanceCalc = self.api.api("distance", 'mbio')
        dist_id = apiDistanceCalc.add_dist_table(self.instantModule.output_file, major=True,
                                                 level=self.option['level_id'], otu_id=self.option['otu_id'],
                                                 params=json.dumps(self.params_json,
                                                                   sort_keys=True, separators=(',', ':')))

        self.appendSgStatus(apiDistanceCalc, dist_id, "sg_beta_specimen_distance", "")
        self.logger.info("Mongo数据库导入完成")

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["core.richness.xls", "xls", "core 表格"],
            ["pan.richness.xls", "xls", "pan 表格"]
            ])
        self.uploadFiles("pan_core")
        super(DistanceCalc, self).end()

    def create_files(self):
        """
        生成文件
        """
        otuPath = export_otu_table_by_level(self.option["otu_id"], self.option["otu_fp"], self.option["level_id"])
        otuFile = OtuTableFile()
        otuFile.set_path(otuPath)
        otuFile.get_info()
        otuFile.check()
        self.option['otu_fp'] = otuFile

    def check_objectid(self, in_id):
        """
        检查一个id是否可以被ObjectId
        """
        if isinstance(in_id, types.StringTypes):
            try:
                in_id = ObjectId(in_id)
            except InvalidId:
                return False
        elif isinstance(in_id, ObjectId):
            pass
        else:
            return False
        return in_id
