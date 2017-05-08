# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

import web
import json
import datetime
from bson import ObjectId
from mainapp.libs.signature import check_sig
from mainapp.libs.param_pack import *
from mainapp.models.mongo.ref_rna import RefRna
from mainapp.controllers.project.ref_rna_controller import RefRnaController


class BlastAnnotationAction(RefRnaController):
    """
    nr/swissprot进行blast筛选重注释接口
    """
    def __init__(self):
        super(BlastAnnotationAction, self).__init__(instant=False)

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get("HTTP_CLIENT")
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": json.dumps(return_result)}
            return json.dumps(info)
        stat_info = RefRna().get_main_info(data.stat_id, "sg_annotation_stat")
        if not stat_info:
            info = {"success": False, "info": "stat_id不存在,请确认参数是否正确"}
            return json.dumps(info)
        params_json = {
            "stat_id": data.stat_id,
            "nr_evalue": data.nr_evalue,
            "nr_score": data.nr_score,
            "nr_similarity": data.nr_similarity,
            "nr_identity": data.nr_identity,
            "swissprot_evalue": data.swissprot_evalue,
            "swissprot_score": data.swissprot_score,
            "swissprot_similarity": data.swissprot_similarity,
            "swissprot_identity": data.swissprot_identity,
            "submit_location": data.submit_location
        }
        main_table_name = "AnnotationStat_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        mongo_data = [
            ("project_sn", stat_info["project_sn"]),
            ("task_id", stat_info["task_id"]),
            ("seq_type", "new"),
            ("status", "start"),
            ("name", main_table_name),
            ("database", "nr,swissprot,pfam"),
            ("desc", "注释统计主表"),
            ("created_ts", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = RefRna().insert_main_table("sg_annotation_stat", mongo_data)
        update_info = {str(main_table_id): "sg_annotation_stat"}
        options = {
            "blastout_table": data.stat_id,
            "nr_evalue": data.nr_evalue,
            "nr_score": data.nr_score,
            "nr_similarity": data.nr_similarity,
            "nr_identity": data.nr_identity,
            "swissprot_evalue": data.swissprot_evalue,
            "swissprot_score": data.swissprot_score,
            "swissprot_similarity": data.swissprot_similarity,
            "swissprot_identity": data.swissprot_identity,
            "stat_id": str(main_table_id),
            "old_stat_id": data.stat_id,
            "update_info": json.dumps(update_info)
        }
        to_file = ["ref_rna.export_blast_table(blastout_table)"]
        self.set_sheet_data(name="ref_rna.report.blast_annotation", options=options,
                            main_table_name="AnnotationStat/" + main_table_name, task_id=stat_info["task_id"], project_sn=stat_info["project_sn"], to_file=to_file)
        task_info = super(BlastAnnotationAction, self).POST()
        task_info["content"] = {"ids": {"id": str(main_table_id), "name": main_table_name}}
        return json.dumps(task_info)

    def check_options(self, data):
        """
        检查网页端传来的参数是否正确
        """
        params_name = ["stat_id", "nr_evalue", "nr_score", "nr_similarity", "nr_identity", "swissprot_evalue", "swissprot_score", "swissprot_similarity", "swissprot_identity", "submit_location"]
        success = []
        for name in params_name:
            if not (hasattr(data, name)):
                success.append("缺少参数{}".format(name))
        if float(data.nr_evalue) > 10e-3:
            success.append("NR E-value值需小于10e-3")
        if float(data.swissprot_evalue) > 10e-3:
            success.append("Swiss-Prot E-value值需小于10e-3")
        if float(data.nr_similarity) < 0 or float(data.nr_similarity) > 1:
            success.append("NR Similarity值需在0-1范围内")
        if float(data.swissprot_similarity) < 0 or float(data.swissprot_similarity) > 1:
            success.append("Swiss-Prot Similarity值需在0-1范围内")
        if float(data.nr_identity) < 0 or float(data.nr_identity) > 1:
            success.append("NR Identity值需在0-1范围内")
        if float(data.swissprot_identity) < 0 or float(data.swissprot_identity) > 1:
            success.append("Swiss-Prot Identity值需在0-1范围内")
        return success
