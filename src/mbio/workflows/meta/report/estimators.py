# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import *
import datetime
from mainapp.libs.param_pack import group_detail_sort
######################################added 2 lines by yiru 20170421
from bson import ObjectId
from mbio.packages.beta_diversity.filter_newick import *
######################################

class EstimatorsWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(EstimatorsWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "indices", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "est_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            ]
        self.add_option(options)
        # print(self._sheet.options())
        self.set_options(self._sheet.options())
        self.estimators = self.add_tool('meta.alpha_diversity.estimators')

    def run(self):
        #################################################################added lines till next "##############################"" by yiru 20170421
        indices = self.option('indices').split(',')
        if 'pd' in indices:
            ####如果需要得到PD指数，则需要找到OTU对应的进化树
            if self.option('level') != 9:
                newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),tempdir=self.work_dir, return_file=False, bind_obj=self)
                all_find = re.findall(r'\'.+?\'', newicktree)  # 找到所有带引号的进化树中复杂的名称
                for n, m in enumerate(all_find):
                    all_find[n] = m.strip('\'')
                all_find = dict((i[1], i[0]) for i in enumerate(all_find))  # 用名称做键，找到的位置数字做值

                def match_newname(matchname):
                    '随着自身被调用，自身的属性count随调用次数增加，返回OTU加次数，用于重命名进化树复杂的名称'
                    if hasattr(match_newname, 'count'):
                        match_newname.count = match_newname.count + 1
                    else:
                        match_newname.count = 1
                    return 'OTU' + str(match_newname.count)  # 后面替换OTU中名称用同样的命名规则
                newline = re.sub(r'\'.+?\'', match_newname, newicktree)  # 替换树种的复杂名称用 OTU 加数字代替 , 选哟注意的是这里的sub查找与findall查到方式是一致的
                temp_tree_file = self.work_dir + '/temp.tree'
                tempfile = open(temp_tree_file, 'w')
                tempfile.write(newline)
                tempfile.close()
                self.logger.info('get_newick:' + temp_tree_file)
                otu_table = self.option('otu_file').path
                temp_otu_file = self.option('otu_file').path + '.temp'
                all_lines = open(otu_table, 'r').readlines()
                if len(all_lines) < 3:
                    raise Exception('分类水平：%s,otu表数据少于2行：%s' % (self.option('level'), len(all_lines)))
                new_all = []
                new_all.append(all_lines[0])
                for line in all_lines[1:]:  # 遍历OTU表，将OTU表的复杂OTU名称改为之前find到的复杂名称对应的字典
                    name = line.split('\t')
                    if name[0] not in all_find:
                        raise Exception('OTU表中存在不是直接通过组合原始表分类名称的OTU名：%s' % name[0])
                    name[0] = 'OTU' + str(all_find[name[0]] + 1)
                    new_all.append('\t'.join(name))
                otu_file_temp = open(temp_otu_file, 'w')
                otu_file_temp.writelines(new_all)
                otu_file_temp.close()
                options = {
                    'otu_table': temp_otu_file,
                    'newicktree': temp_tree_file,
                    'indices': self.option('indices')
                }
            else:
                newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'), tempdir=self.work_dir, return_file=False, bind_obj=self)
                temp_tree_file = self.work_dir + '/temp.tree'
                tempfile = open(temp_tree_file, 'w')
                tempfile.write(newicktree)
                tempfile.close()
                otu_table = self.option('otu_file').path
                temp_otu_file = self.option('otu_file').path + '.temp'
                all_lines = open(otu_table, 'r').readlines()
                new_all = []
                new_all.append(all_lines[0])
                for line in all_lines[1:]:  # OTU表中有复杂的名称OTU名称，包含进化物种类型，进化树种只有OTU名称
                    name = line.split('\t')
                    name[0] = name[0].split(';')[-1].strip()
                    new_all.append('\t'.join(name))
                otu_file_temp = open(temp_otu_file, 'w')
                otu_file_temp.writelines(new_all)
                otu_file_temp.close()
                options = {
                    'otu_table': temp_otu_file,
                    'newicktree': temp_tree_file,
                    'indices': self.option('indices')
                }
        else:
        ##########################################################################################
            options = {
                'otu_table': self.option('otu_file'),
                'indices': self.option('indices')
            }
        # print(self.option('indices'))
        self.estimators.set_options(options)
        self.estimators.on('end', self.set_db)
        self.estimators.run()
        self.output_dir = self.estimators.output_dir
        super(EstimatorsWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_estimators = self.api.estimator
        est_path = self.output_dir+"/estimators.xls"
        if not os.path.isfile(est_path):
            raise Exception("找不到报告文件:{}".format(est_path))
        est_id = api_estimators.add_est_table(est_path, level=self.option('level'),otu_id=self.option('otu_id'), est_id=self.option("est_id"))
        # self.add_return_mongo_id('sg_alpha_diversity', est_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            # [".", "", "结果输出目录"],
            ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        # print self.get_upload_files()
        super(EstimatorsWorkflow, self).end()
