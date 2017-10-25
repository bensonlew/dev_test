# -*- coding: utf-8 -*-
# __author__ = zhouxuan
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os  
import re
import collections


class HcHeatmapAgent(Agent):
    """
    小工具聚类heatmap图：实现任意二维数据的热图并含有行和列的聚类树
    auther: xuan.zhou
    last_modified: 201706022 数据表格式修改
    使用脚本实现聚类树
    last_modified: 20170907 增加分组方案文件 zengjing
    """

    def __init__(self, parent):
        super(HcHeatmapAgent, self).__init__(parent)
        options = [
            {"name": "data_table", "type": "infile", "format": "toolapps.table"},  # 数据表
            {"name": "row_method", "type": "string", "default": "no"},  # 行聚类方式,为no时不聚类
            {"name": "col_method", "type": "string", "default": "no"},  # 列聚类方式,为no时不聚类
            {"name": "col_number", "type": "string", "default": "10"},  # 列数
            {"name": "row_number", "type": "string", "default": "10"},  # 行数
            {"name": "data_T", "type": "string", "default": "false"},
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table"},  # modify by zengjing 20170907
            {"name": "group_method", "type": "string", "default": "none"}  # 组内合并参数none,sum,average,middle
        ]
        self.add_option(options)
        self.step.add_steps('hc_heatmap')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        self.new_data = ''

    def step_start(self):
        self.step.hc_heatmap.start()
        self.step.update()

    def step_end(self):
        self.step.hc_heatmap.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("data_table"):
            raise OptionError("参数data_table不能为空")
        if self.option("row_method") not in ['average', 'complete', 'single', 'no']:
            raise OptionError("请选择正确行聚类方式")
        if self.option("col_method") not in ['average', 'complete', 'single', 'no']:
            raise OptionError("请选择正确列聚类方式")
        if self.option("group_table").is_set:
            if self.option("group_method") not in ["none", "average", "sum", "middle"]:
                raise OptionError("分组样本计算方式只能为none,average,sum,none")
            if self.option('data_T') != "false":
                for i in self.option('group_table').prop['sample_name']:
                    if i not in self.option('data_table').prop['row_sample']:
                        raise OptionError('分组文件中的样本{}不存在于表格中，查看是否是数据取值选择错误'.format(i))
            else:
                for i in self.option('group_table').prop['sample_name']:
                    if i not in self.option('data_table').prop['col_sample']:
                        raise OptionError('分组文件中的样本{}不存在于表格中，查看是否是数据取值选择错误'.format(i))

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 5
        self._memory = '5G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "Heatmap结果目录"],
            # ["./.*result_data", "xls", "结果表"],
            ["./row_tre", "tre", "行聚类树"],
            ["./col_tre", "tre", "列聚类树"],
        ])
        super(HcHeatmapAgent, self).end()


class HcHeatmapTool(Tool):
    """
    使用脚本/mnt/ilustre/users/sanger-dev/app/bioinfo/statistical/scripts/plot-hcluster_tree_app.pl
    """

    def __init__(self, config):
        super(HcHeatmapTool, self).__init__(config)
        self.R_path = 'program/R-3.3.1/bin/Rscript'
        self.app_path = 'bioinfo/statistical/scripts/plot-hcluster_tree_app.pl'
        self._version = 2.0
        self.new_table = self.option('data_table').prop['new_table']

    def get_group_data_table(self):
        """筛选出丰度为前多少的行和列的数据并根据分组方案得到合并的二维表格"""
        self.new_data = os.path.join(self.work_dir, "new_data.xls")
        self.get_new_data(old_path=self.new_table, new_path=self.new_data,
                          col_number=self.option("col_number"), row_number=self.option("row_number"), t=self.option('data_T'))
        if self.option('group_table').is_set and self.option("group_method") in ["average", "sum", "middle"]:
            group_samples = self.option('group_table').get_group_detail()
            self.logger.info(group_samples)
            with open(self.new_data, "r") as f:
                lines = f.readlines()
                line = lines[0].rstrip().split("\t")
                for group in group_samples:
                    new_samples = list(group_samples[group].keys())
                    if len(new_samples) >= 2:
                        new_sample_index = {}
                        for s in new_samples:
                            old_samples = group_samples[group][s]
                            new_sample_index[s] = []
                            for o in old_samples:
                                for i in range(len(line)):
                                    if o == line[i]:
                                        new_sample_index[s].append(i)
                        table_path = os.path.join(self.work_dir, group + "_table.xls")
                        with open(table_path, "w") as w:
                            header = line[0] + "\t" + '\t'.join(new_samples) + "\n"
                            w.write(header)
                            for item in lines[1:]:
                                item = item.rstrip().split("\t")
                                w.write(item[0] + "\t")
                                tmp = []
                                for s in new_samples:
                                    summary = 0
                                    mid_list = []
                                    for i in new_sample_index[s]:
                                        summary += float(item[i])
                                        mid_list.append(float(item[i]))
                                    mid_list.sort()
                                    size = len(mid_list)
                                    if size % 2 == 0:
                                        midian = (mid_list[size/2] + mid_list[size/2 -1]) / 2
                                    else:
                                        midian = mid_list[(size-1)/2]
                                    average = summary / size
                                    if self.option("group_method") == "sum":
                                        tmp.append(str(round(summary, 4)))
                                    elif self.option("group_method") == "average":
                                        tmp.append(str(round(average, 4)))
                                    elif self.option("group_method") == "middle":
                                        tmp.append(str(round(midian, 4)))
                                w.write('\t'.join(tmp) + "\n")
                        self.create_tree(table_path, group)
                    else:
                        raise OptionError("聚类时样本必须大于等于2,{}分组里样本小于2,不能进行聚类".format(group))
        elif self.option('group_table').is_set:
            group_samples = self.option('group_table').get_group_detail()
            self.logger.info(group_samples)
            with open(self.new_data, "r") as f:
                lines = f.readlines()
                line = lines[0].rstrip().split("\t")
                for group in group_samples:
                    samples = []
                    for s1 in group_samples[group]:
                        for s2 in group_samples[group][s1]:
                            samples.append(s2)
                    samples = list(set(samples))
                    if len(samples) >= 2:
                        sample_index = {}
                        for s in samples:
                            for i in range(len(line)):
                                if s == line[i]:
                                    sample_index[s]= i
                        table_path = os.path.join(self.work_dir, group + "_table.xls")
                        with open(table_path, "w") as w:
                            header = line[0] + "\t" + '\t'.join(samples) + "\n"
                            w.write(header)
                            for item in lines[1:]:
                                item = item.rstrip().split("\t")
                                w.write(item[0] + "\t")
                                tmp = []
                                for s in samples:
                                    tmp.append(item[sample_index[s]])
                                w.write('\t'.join(tmp) + "\n")
                        self.create_tree(table_path, group)
                    else:
                        raise OptionError("聚类时样本必须大于等于2,{}分组里样本小于2,不能进行聚类".format(group))
        else:
            os.link(self.new_data, os.path.join(self.work_dir, "new_table.xls"))
            self.create_tree(os.path.join(self.work_dir, "new_table.xls"), "no")

    def get_new_data(self, old_path, new_path, col_number, row_number, t):
        """
        使用pandas包完成对于二维表格（行列名称均存在）的筛选，筛选出丰度为前多少的行和列的数据
        :return: 新的表格
        """
        import pandas as pd
        data = pd.read_table(old_path, header=0)  # 把数据读取成data_frame格式
        col = data.sum(axis=1)  # 计算每行的和
        data = data.join(col.to_frame(name="col"))  # 把每行和加到数据框的最后一列
        data = data.sort(["col"], ascending=False)  # 根据行和排序（由高到低）
        data_1 = data.drop(["col"], axis=1)  # 从数据框中去除行和
        if row_number != "":
            data_1 = data_1.iloc[:int(row_number)]  # 筛选出前多少行
        name_list = [i for i in data_1.T.iloc[0]]  # 行列转置的时候可能会用
        name = data_1.T.iloc[0].to_frame(name="name")  # 原数据行名的保留
        row = data_1.T.iloc[1:].sum(axis=1)  # 去除行名数据框转置求行和也就是求原数据的列和
        data_2 = data_1.T.iloc[1:].join(row.to_frame(name="row"))  # 以下四行就是以相同的方式筛选出前多少行(由于转置所以实际是列)
        data_2 = data_2.sort(["row"], ascending=False)
        data_2 = data_2.drop(["row"], axis=1)
        if col_number != "":
            data_2 = data_2.iloc[:int(col_number)]
        if t == "true":
            data_2.columns = name_list
            data_ = data_2
            data_.index.name = "name"
            data_.to_csv(new_path, sep="\t", index=True)
        else:
            data_ = name.join(data_2.T)  # 恢复行名
            data_.to_csv(new_path, sep="\t", index=False)  # 数据框写入文件

    def create_tree(self, table_path, group):
        """
        plot-hcluster_tree_app.pl,输出画图所需的树文件
        """
        if self.option('row_method') == 'no' and self.option('col_method') == 'no':
            self.set_output()
        else:  # -m ~ col
            if self.option('row_method') != 'no' and self.option('col_method') == 'no':
                cmd = '{} -i {} -m_1 {} -trans row -o {}'.format(self.app_path, table_path, self.option('row_method'), self.output_dir)
            elif self.option('row_method') == 'no' and self.option('col_method') != 'no':
                cmd = '{} -i {} -m {} -trans col -o {}'.format(self.app_path, table_path, self.option('col_method'), self.output_dir)
            else:
                cmd = '{} -i {} -m {} -m_1 {} -trans both -o {}'.format(self.app_path, table_path, self.option('col_method'), self.option('row_method'), self.output_dir)
            self.logger.info("开始运行plot-hcluster_tree_app.pl")
            command = self.add_command("hc_heatmap_{}".format(group.lower()), cmd)
            command.run()
            self.wait(command)
            if command.return_code == 0:
                self.logger.info("运行plot-hcluster_tree_app.pl完成，正确生成hc.cmd.r脚本")
            else:
                self.set_error("运行plot-hcluster_tree_app.pl运行出错!")
                raise Exception("运行plot-hcluster_tree_app.pl运行出错，请检查输入的参数是否正确")
            r_cmd = '{} {}/hc.cmd.r'.format(self.R_path, self.work_dir)
            self.logger.info("运行hc.cmd.r")
            r_command = self.add_command("hc_r_{}".format(group.lower()), r_cmd)
            r_command.run()
            self.wait(r_command)
            if r_command.return_code == 0:
                self.logger.info("运行hc.cmd.r脚本成功")
            else:
                self.set_error("运行hc.cmd.r运行出错!")
                raise Exception("运行hc.cmd.r运行出错，请检查输入的参数是否正确")

    def set_output(self):
        file_name = os.listdir(self.output_dir)
        if self.option('row_method') != 'no' and self.option('col_method') == 'no':
            for name in file_name:
                m = re.match(r"hcluster_tree_(.+)_table.xls(.*).tre$", name)
                if m:
                    group = m.group(1)
                    os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "{}_row_tre".format(group)))
                    os.remove(os.path.join(self.output_dir, name))
            self.logger.info("存在行聚类树")
        elif self.option('row_method') == 'no' and self.option('col_method') != 'no':
            for name in file_name:
                m = re.match(r"hcluster_tree_(.+)_table.xls(.*).tre$", name)
                if m:
                    group = m.group(1)
                    os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "{}_col_tre".format(group)))
                    os.remove(os.path.join(self.output_dir, name))
            self.logger.info("存在列聚类树")
        else:
            for name in file_name:
                m = re.match(r"hcluster_tree_(.+)_table.xls(.*)\.tre$", name)
                if m:
                    group = m.group(1)
                    new = os.path.join(self.output_dir, "{}_col_tre".format(group))
                    os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "{}_col_tre".format(group)))
                    os.remove(os.path.join(self.output_dir, name))
                n = re.match(r"hcluster_tree_(.+)_table.xls(.*)\.ttre$", name)
                if n:
                    group = n.group(1)
                    os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "{}_row_tre".format(group)))
                    os.remove(os.path.join(self.output_dir, name))
        file_name_2 = os.listdir(self.output_dir)
        for name in file_name_2:
            if re.search('(\.pdf)$', name):
                os.remove(os.path.join(self.output_dir, name))
        table_files = os.listdir(self.work_dir)
        for f in table_files:
            t = re.search(r"(.+)_table.xls$", f)
            if t:
                g = t.group(1)
                out = os.path.join(self.output_dir, "{}_result_data".format(g))
                if not os.path.exists(out):
                    os.link(os.path.join(self.work_dir, f), out)

    def run(self):
        """
        运行
        """
        super(HcHeatmapTool, self).run()
        self.get_group_data_table()
        self.set_output()
        self.end()
