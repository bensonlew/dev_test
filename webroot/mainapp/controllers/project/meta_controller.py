# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
from mainapp.libs.input_check import MetaCheck
from mainapp.controllers.core.basic import Basic
from mainapp.models.mongo.public.meta.meta import Meta
from mainapp.libs.signature import check_sig
from mainapp.models.mongo.distance_matrix import Distance


class MetaController(Basic):
    @check_sig
    @MetaCheck
    def POST(self):
        """
        获取taskId, projectSn, 以及memberId
        然后调用父类的方法生成工作路径， logger, 以及workId
        """
        data = web.input()
        self.data = data
        self._client = data.client
        if hasattr(data, 'otu_id'):
            otuId = data.otu_id
            self._mainTableId = otuId
            table_info = Meta().get_otu_table_info(otuId)
        else:
            distance_id = data.specimen_distance_id
            self._mainTableId = distance_id
            table_info = Distance().get_distance_matrix_info(distance_id)
        taskId = table_info["task_id"]
        self._taskId = taskId
        self._projectSn = table_info["project_sn"]
        task_info = Meta().get_task_info(table_info["task_id"])
        self._memberId = task_info["member_id"]
        super(MetaController, self).POST()
