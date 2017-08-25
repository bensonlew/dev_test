# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import json
from biocluster.api.database.base import Base, report_check
import re
import os
import datetime
from bson import SON
from biocluster.config import Config


class VennTable(Base):
    def __init__(self, bind_object):
        super(VennTable, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        self.work_dir = self.bind_object.work_dir
        if Config().MONGODB == 'sanger':
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()

    @report_check
    def run(self):
        """
        运行函数
        """
        self.main_id = self.venn_in()
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def table_in(self):
        """
        导入表格相关信息
        """
        all_file = os.listdir(self.output_dir)
        venn_list = []
        group_list = []
        if len(all_file) == 2:
            with open(self.work_dir + '/VennTable/group_table')as f:
                line = f.readline()
                group_name = line.strip().split("\t")
                group_list.append(group_name[1])
            venn_otu = self.insert_table(self.output_dir + '/venn_table.xls', 'Venn数据表', '分组间共有和分组中特有的物种的数量统计')
            venn_list.append(venn_otu)
        else:
            for f in all_file:
                if f.endswith("venn_table.xls"):
                    f_path = self.output_dir + '/' + f
                    group_name = f.strip().split("venn_table.xls")[0]
                    venn_otu = self.insert_table(f_path, group_name + '的venn结果表', group_name + '分组间共有和分组中特有的物种的数量统计')
                    venn_list.append(venn_otu)
                else:
                    pass
        return venn_list

    def insert_table(self, fp, name, desc):
        columns = ['Group_label', 'Coincidence_num']
        insert_data = []
        table_id = self.db['table'].insert_one(SON(
            project_sn=self.bind_object.sheet.project_sn,
            task_id=self.bind_object.id,
            name=name,
            attrs=columns,
            desc=desc,
            status='end',
            created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )).inserted_id
        with open(fp) as f:
            for line2 in f:
                line_split = line2.strip().split('\t')
                data = SON(table_id=table_id)
                data['Group_label'] = line_split[0]
                data['Coincidence_num'] = line_split[1]
                # data['Coincidence_list'] = line_split[2]
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)
        return table_id

    def venn_in(self):
        """
        导入venn图相关信息
        """
        all_file = os.listdir(self.output_dir)
        venn_list = []
        group_list = []
        if len(all_file) == 2:
            with open(self.work_dir + '/VennTable/group_table')as f:
                line = f.readline()
                group_name = line.strip().split("\t")
                group_list.append(group_name[1])
            venn_otu = self.insert_venn(self.output_dir + '/venn_graph.xls', 'Venn', 'venn图')
            venn_list.append(venn_otu)
        else:
            for f in all_file:
                if f.endswith("venn_graph.xls"):
                    f_path = self.output_dir + '/' + f
                    group_name = f.strip().split("venn_graph.xls")[0]
                    venn_otu = self.insert_venn(f_path, group_name + '的venn图', group_name + '的venn图')
                    venn_list.append(venn_otu)
                else:
                    pass
        return venn_list

    def insert_venn(self, fp, name, desc):
        with open(fp) as f:
            venn_id = self.db['venn'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name=name,
                desc=desc,
                status='faild',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            samples = []
            insert_data = []
            # f = f.next()
            for line in f:
                if re.search("#", line):
                    pass
                else:
                    line_list = []
                    line_split = line.strip().split('\t')
                    samples.append(line_split[0])
                    member = line_split[1].split(",")
                    for i in member:
                        line_list.append(i)
                    data = SON(category_name=line_split[0], venn_id=venn_id)
                    data['venn_list'] = line_list
                    insert_data.append(data)
            self.db['venn_detail'].insert_many(insert_data)
            self.db['venn'].update_one({'_id': venn_id}, {'$set': {'status': 'end', 'attrs': samples}})
            return venn_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
