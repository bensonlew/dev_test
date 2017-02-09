# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""fq文件序列拆分"""

import os
import datetime
import json
import shutil
import re
from biocluster.workflow import Workflow


class FastqSplitWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(FastqSplitWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_fastq", "type": "infile", 'format': "sequence.fastq"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run_fastq_split(self):
        fastq_path = self.option("in_fastq").prop["path"]
        with open(fastq_path,"r") as f:
            for line in f:
                m = re.match("@(.+)_(\d+)", line)
                if not m:
                    raise Exception('fastq文件格式不符合要求')
                sample_name = m.group(1)
                sample = self.return_sample(sample_name)
                sample.add_new_fastq(line, next(f), next(f), next(f))
        self.logger.info("全部fastq序列处理完毕")
        # self.set_db()
        self.end()

    def return_sample(self,sample_name):
        if sample_name in self.samples:
            return self.samples[sample_name]
        sample = Sample(sample_name)
        self.samples[sample_name] = sample
        return sample


    def set_db(self):
        sour = os.path.join(self.venn.work_dir, "output/venn_table.xls")
        dest = os.path.join(self.work_dir, "output")
        shutil.copy2(sour, dest)
        self.logger.info("正在往数据库里插入sg_otu_venn_detail表")
        api_venn = self.api.venn
        myParams = json.loads(self.sheet.params)
        name = "venn_table_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        venn_id = api_venn.create_venn_table(self.sheet.params, myParams["group_id"], self.option("level"), self.option("otu_id"), name)
        venn_path = os.path.join(self.venn.work_dir, "venn_table.xls")
        venn_graph_path = os.path.join(self.venn.work_dir, "venn_graph.xls")
        api_venn.add_venn_detail(venn_path, venn_id, self.option("otu_id"), self.option("level"))
        api_venn.add_venn_graph(venn_graph_path, venn_id)
        self.add_return_mongo_id("sg_otu_venn", venn_id)
        self.end()

    def run(self):
        self.run_fastq_split()
        super(FastqSplitWorkflow, self).run()

    def end(self):
        """
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["venn_table.xls", "xls", "Venn表格"]
        ])
        """
        super(FastqSplitWorkflow, self).end()

class Sample(object):
    def __init__(self, name):
        self.name = name
        self._new_fastq_file = open(self.output_dir+ "/" + self.name + '.fastq', 'w')

    def add_new_fastq(self, line1,line2,line3,line4):
        self._new_fastq_file.write(line1)
        self._new_fastq_file.write(line2)
        self._new_fastq_file.write(line3)
        self._new_fastq_file.write(line4)


    def close_all(self):
        self._new_fastq_file.close()