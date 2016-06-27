# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
from mainapp.libs.input_check import MetaCheck
from mainapp.controllers.core.basic import Basic
from mainapp.models.mongo.public.meta.meta import Meta
from mainapp.libs.signature import check_sig


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
        otuId = data.otu_id
        self._mainTableId = otuId
        otu_info = Meta().get_otu_table_info(data.otu_id)
        taskId = otu_info["task_id"]
        self._taskId = taskId
        self._projectSn = otu_info["project_sn"]
        task_info = Meta().get_task_info(otu_info["task_id"])
        self._memberId = task_info["member_id"]
        super(MetaController, self).POST()
