# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from biocluster.api.database.base import Base, report_check
import os
import datetime
from biocluster.config import Config


class GenomeInfo(Base):
    def __init__(self, bind_object=None):
        super(GenomeInfo, self).__init__(bind_object)
        self._db_name = Config().MONGODB + "_ref_rna"

    @report_check
    def add_genome_info(self, species_name, output_dir= "", major = True):
        main_insert_data = {
            'project_sn': self.bind_object.sheet.project_sn,
            'task_id': self.bind_object.sheet.id,
            'species_name': species_name,
            'ref_anno_version': "",  # 暂时为空
            "hyperlink": "",  # 暂时为空
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        main_collection = self.db['sg_species_information']
        main_id = main_collection.insert_one(main_insert_data).inserted_id
        if major:
            self.add_genome_info_detail(output_dir=output_dir, inserted_id=main_id)

    @report_check
    def add_genome_info_detail(self, output_dir, inserted_id):
        insert_data_list = []
        if len(os.listdir(output_dir)) == 2:
            with open("{}/gene.stat.xls".format(output_dir), "r") as fr:
                fr.readline()
                for line in fr:
                    tmp = line.strip().split("\t")
                    chr = tmp[0]
                    size = tmp[1]
                    gc = tmp[2]
                    gene = tmp[3]
                    protein_coding = tmp[4]
                    other_rna = tmp[5]
                    pseudo = tmp[6]
                    insert_data = {
                        "species_id": inserted_id,
                        "ref_seq":chr,
                        "length":size,
                        "qc_percent": gc,
                        "gene": gene,
                        "proteincoding": protein_coding,
                        "other_rna": other_rna,
                        "pseudogene": pseudo
                    }
                    insert_data_list.append(insert_data)
        else:
            with open("{}/gene.stat.xls".format(output_dir), "r") as fr:
                fr.readline()
                for line in fr:
                    tmp = line.strip().split("\t")
                    chr = tmp[0]
                    size = tmp[1]
                    gc = tmp[2]
                    insert_data = {
                        "ref_seq": chr,
                        "length": size,
                        "qc_percent": gc
                    }
                    insert_data_list.append(insert_data)
        collections = self.db['sg_species_information_detail']
        collections.insert_many(insert_data_list)
        self.bind_object.logger.info("导入sg_species_information_detail细节表成功")
