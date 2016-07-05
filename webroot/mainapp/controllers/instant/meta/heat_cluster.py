# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import datetime
import re
from mainapp.controllers.project.meta_controller import MetaController
from mbio.instant.to_files.export_file import export_otu_table_by_level
from mbio.files.meta.otu.otu_table import OtuTableFile
from mainapp.models.mongo.public.meta.meta import Meta


class HeatCluster(MetaController):
    def POST(self):
        myReturn = super(HeatCluster, self).POST()
        if myReturn:
            return myReturn
        sampleNames = Meta().sampleIdToNname(self.data.specimen_ids)
        options = {
            "otuPath": os.path.join(self.work_dir, "otu.trans"),
            "samples": sampleNames,
            "linkage": self.data.linkage,
            "otuId": self.data.otu_id,
            "level_id": self.data.level_id
        }
        self.setOptions(options)
        self.createFiles()
        self.importInstant("meta")
        self.run()
        return self.returnInfo

    def run(self):
        super(HeatCluster, self).run()
        self.addMongo()
        self.end()

    def addMongo(self):
        apiTree = self.api.api("meta.heat_cluster")
        name = "heat_cluster_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        treeId = apiTree.CreateTreeTable(name)
        self.appendSgStatus(apiTree, treeId, "sg_newick_tree", "")
        self.logger.info("Mongo数据库导入完成")

    def end(self):
        super(HeatCluster, self).end()

    def createFiles(self):
        """
        生成OTU文件
        """
        orignOtuPath = export_otu_table_by_level(self.options["otuId"], os.path.join(self.work_dir, "orignOtu.xls"), self.options["level_id"])
        orignOtuFile = OtuTableFile()
        orignOtuFile.set_path(orignOtuPath)
        orignOtuFile.get_info()
        orignOtuFile.check()
        noZeroPath = os.path.join(self.work_dir, "otu.nozero")
        samples = re.split(",", self.options["samples"])
        orignOtuFile.sub_otu_sample(samples, noZeroPath)
        noZeroFile = OtuTableFile()
        noZeroFile.set_path(noZeroPath)
        transOtu = os.path.join(self.work_dir, "otu.trans")
        noZeroFile.transposition(transOtu)
