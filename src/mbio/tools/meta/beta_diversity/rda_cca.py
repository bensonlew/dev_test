# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
import subprocess
from biocluster.core.exceptions import OptionError
import re


class RdaCcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2017.01.20
    """

    def __init__(self, parent):
        super(RdaCcaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}
        ]
        self.add_option(options)
        self.step.add_steps('RDA_CCA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.RDA_CCA.start()
        self.step.update()

    def step_end(self):
        self.step.RDA_CCA.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        if self.option('otutable').prop['sample_num'] < 3:
            raise OptionError('otu表的样本数目少于3，不可进行beta多元分析')
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if self.option('envlabs'):
                labs = self.option('envlabs').split(',')
                for lab in labs:
                    if lab not in self.option('envtable').prop['group_scheme']:
                        raise OptionError('提供的envlabs中有不在环境因子表中存在的因子：%s' % lab)
            else:
                pass
            if self.option('envtable').prop['sample_number'] < 3:
                raise OptionError('环境因子表的样本数目少于3，不可进行beta多元分析')
        else:
            raise OptionError('必须提供环境因子表')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        # if len(self.option('envtable').prop['sample']) > len(samplelist):
        #     raise OptionError('OTU表中的样本数量:%s小于环境因子表中的样本数量:%s' % (len(samplelist),
        #                       len(self.option('envtable').prop['sample'])))
        # for sample in self.option('envtable').prop['sample']:
        #     if sample not in samplelist:
        #         raise OptionError('环境因子中存在，OTU表中的未知样本:%s' % sample)
        common_samples = set(samplelist) & set(self.option('envtable').prop['sample'])
        if len(common_samples) < 3:
            raise OptionError("环境因子表和OTU表的共有样本数必须大于等于3个：{}".format(len(common_samples)))
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = '3G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "rda_cca分析结果目录"]
        ])
        result_dir.add_regexp_rules([
            [r'.*_importance\.xls', 'xls', '主成分解释度表'],
            [r'.*_sites\.xls', 'xls', '样本坐标表'],
            [r'.*_species\.xls', 'xls', '物种坐标表'],
            [r'.*dca\.xls', 'xls', 'DCA分析结果'],
            [r'.*_biplot\.xls', 'xls', '数量型环境因子坐标表'],
            [r'.*_centroids\.xls', 'xls', '哑变量环境因子坐标表'],
            [r'.*_envfit\.xls', 'xls', 'p_value值和r值表']
        ])
        # print self.get_upload_files()
        super(RdaCcaAgent, self).end()


class RdaCcaTool(Tool):  # rda/cca需要第一行开头没有'#'的OTU表，filter_otu_sample函数生成的表头没有'#'
    def __init__(self, config):
        super(RdaCcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'bioinfo/statistical/scripts/ordination.pl')

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + '/temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            return new_path
        else:
            return self.option('envtable').path

    def get_otu_table(self):
        """
        根据level返回进行计算的otu表路径
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        # otu表对象没有样本列表属性
        return otu_path
        # return self.filter_otu_sample(otu_path, self.option('envtable').prop['sample'],
        #                               os.path.join(self.work_dir, 'temp_filter.otutable'))

    def filter_otu_sample(self, otu_path, filter_samples, newfile):
        if not isinstance(filter_samples, types.ListType):
            raise Exception('过滤otu表样本的样本名称应为列表')
        try:
            with open(otu_path, 'rb') as f, open(newfile, 'wb') as w:
                one_line = f.readline()
                all_samples = one_line.rstrip().split('\t')[1:]
                if not ((set(all_samples) & set(filter_samples)) == set(filter_samples)):
                    raise Exception('提供的过滤样本存在otu表中不存在的样本all:%s,filter_samples:%s' % (all_samples, filter_samples))
                if len(all_samples) == len(filter_samples):
                    return otu_path
                samples_index = [all_samples.index(i) + 1 for i in filter_samples]
                w.write('OTU\t' + '\t'.join(filter_samples) + '\n')
                for line in f:
                    all_values = line.rstrip().split('\t')
                    new_values = [all_values[0]] + [all_values[i] for i in samples_index]
                    w.write('\t'.join(new_values) + '\n')
                return newfile
        except IOError:
            raise Exception('无法打开OTU相关文件或者文件不存在')


    def run(self):
        """
        运行
        """
        super(RdaCcaTool, self).run()
        self.logger.info("RUN")
        self.run_ordination()

    def formattable(self, tablepath):
        with open(tablepath) as table:
            if table.read(1) == '#':
                newtable = os.path.join(self.work_dir, 'temp_format.table')
                with open(newtable, 'w') as w:
                    w.write(table.read())
                return newtable
        return tablepath

    def remove_zero_line(self, fp, new_fp):
        """
        去除全为0的行
        """
        with open(fp) as f, open(new_fp, 'w') as w:
            w.write(f.readline())
            for line in f:
                data = line.strip().split('\t')[1:]
                sum_data = sum([float(i) for i in data])
                if not (sum_data > 0):
                    continue
                w.write(line)

    def create_otu_and_env_common(self, T1, T2, new_T1, new_T2):
        import pandas as pd
        T1 = pd.read_table(T1, sep='\t', dtype=str)
        T2 = pd.read_table(T2, sep='\t', dtype=str)
        T1_names = list(T1.columns[1:])
        T2_names = list(T2.iloc[0:, 0])
        T1_T2 = set(T1_names) - set(T2_names)
        T2_T1 = set(T2_names) - set(T1_names)
        T1T2 = set(T2_names) & set(T1_names)
        if len(T1T2) < 3:
            return False
        [T1_names.remove(value) for value in T1_T2]
        T1.to_csv(new_T1, sep="\t", columns=[T1.columns[0]] + T1_names, index=False)
        indexs = [T2_names.index(one) for one in T2_T1]
        T2 = T2.drop(indexs)
        T2.to_csv(new_T2, sep="\t", index=False)
        return True

    def run_ordination(self):
        """
        运行ordination.pl
        """
        old_otu_table = self.get_otu_table()
        old_env_table = self.get_new_env()
        otu_species_list = self.get_species_name()
        self.otu_table = self.work_dir + '/new_otu.xls'
        self.env_table = self.work_dir + '/new_env.xls'
        if not self.create_otu_and_env_common(old_otu_table, old_env_table, self.otu_table, self.env_table):
            self.set_error('环境因子表中的样本与OTU表中的样本共有数量少于2个')
        tablepath = self.work_dir + '/remove_zero_line_otu.xls'
        self.remove_zero_line(self.formattable(self.otu_table), tablepath)
        self.env_labs = open(self.env_table, 'r').readline().strip().split('\t')[1:]
        # self.env_table_ = self.rm_(self.env_table)
        envfit = self.env_type()
        self.logger.info(tablepath)
        cmd = self.cmd_path
        cmd += ' -type rdacca -community %s -environment %s -outdir %s -env_labs %s -envfit %s' % (
               tablepath, self.env_table,
               self.work_dir, '+'.join(self.env_labs), envfit)
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('Rda/Cca计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('Rda/Cca计算失败')
            self.set_error('R程序计算Rda/Cca失败')
        allfiles = self.get_filesname()
        for i in [1, 2, 3, 4, 5, 6]:
            if allfiles[i]:
                newname = '_'.join(os.path.basename(allfiles[i]).split('_')[-2:])
                if i == 4:
                    self._magnify_vector(self.work_dir + '/rda/' + allfiles[4], self.work_dir + '/rda/' + allfiles[3],
                                         self.work_dir + '/rda/' + 'magnify_' + newname)
                    self.linkfile(self.work_dir + '/rda/' + 'magnify_' + newname, newname)
                else:
                    self.linkfile(self.work_dir + '/rda/' + allfiles[i], newname)
        newname = os.path.basename(allfiles[0]).split('_')[-1]
        self.linkfile(self.work_dir + '/rda/' + allfiles[0], newname)
        if len(otu_species_list) == 0:  # 20170122 add 13 lines by zhouxuan
            if os.path.exists(self.output_dir + "/rda_species.xls"):
                self.linkfile(self.output_dir + "/rda_species.xls", "rda_plot_species_data.xls")
            else:
                self.linkfile(self.output_dir + "/cca_species.xls", "cca_plot_species_data.xls")
        else:
            new_file_path = self.get_new_species_xls(otu_species_list)
            if os.path.exists(self.output_dir + "/rda_species.xls"):
                self.linkfile(new_file_path, "rda_plot_species_data.xls")
            else:
                self.linkfile(new_file_path, "cca_plot_species_data.xls")
        self.logger.info('运行ordination.pl程序计算rda/cca完成')
        self.end()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def _magnify_vector(self, vector_file, sites_file, new_vector):
        """
        放大环境因子向量的箭头
        """
        def get_range(path):
            with open(path) as sites:
                sites.readline()
                pc1 = []
                pc2 = []
                for line in sites:
                    split_line = line.rstrip().split('\t')
                    if len(split_line) < 3:
                        self.set_error('未知原因，坐标文件少于3列')
                    pc1.append(abs(float(split_line[1])))
                    pc2.append(abs(float(split_line[2])))
                # range_pc = min([max(pc1), max(pc2)])
            return max(pc1), max(pc2)
        range_vector_pc1, range_vector_pc2 = get_range(vector_file)
        range_sites_pc1, range_sites_pc2 = get_range(sites_file)
        if range_vector_pc1 == 0:
            range_vector_pc1 += 0.00001
        if range_vector_pc2 == 0:
            range_vector_pc2 += 0.00001
        magnify_1 = range_sites_pc1 / range_vector_pc1
        magnify_2 = range_sites_pc2 / range_vector_pc2
        magnify = magnify_1 if magnify_1 < magnify_2 else magnify_2
        with open(new_vector, 'w') as new, open(vector_file) as vector:
            new.write(vector.readline())
            for line in vector:
                line_split = line.rstrip().split('\t')
                for i in range(len(line_split) - 1):
                    index = i + 1
                    line_split[index] = str(float(line_split[index]) * magnify)
                new.write('\t'.join(line_split) + '\n')

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在

        :return rda_imp,rda_spe,rda_dca,rda_site,rda_biplot: 返回各个文件
        """
        filelist = os.listdir(self.work_dir + '/rda')
        rda_imp = None
        rda_spe = None
        rda_dca = None
        rda_site = None
        rda_biplot = None
        rda_centroids = None
        rda_envfit = None
        for name in filelist:
            if '_importance.xls' in name:
                rda_imp = name
            elif '_sites.xls' in name:
                rda_site = name
            elif '_species.xls' in name:
                rda_spe = name
            elif 'dca.xls' in name:
                rda_dca = name
            elif '_biplot.xls' in name:
                rda_biplot = name
            elif '_centroids.xls' in name:
                rda_centroids = name
            elif '_envfit.xls' in name:
                rda_envfit = name
        if rda_imp and rda_site and rda_spe and rda_dca and (rda_biplot or rda_centroids) or rda_envfit:
            self.logger.info(str([rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids, rda_envfit]))
            return [rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids, rda_envfit]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')

    def get_species_name(self):  # 20170122 add by zhouxuan
        """
        判断otu表中的物种数量是否大于30 ，如果大于30，筛选出丰度在前30的物种
        :return: 丰度为前30的物种或者 空的列表
        """
        otu_path = self.get_otu_table()
        with open(otu_path, "rb") as r:
            r = r.readlines()
            species_number = len(r) - 1
            if species_number <= 30:
                return []
            else:
                species_dict = dict()
                abundance_list = []
                for line in r:
                    # line = line.strip("\n")
                    array = line.strip("\n").split("\t")
                    if array[0] != "OTU ID":
                        value = 0
                        new_array = array[1:]
                        for i in range(len(new_array)):
                            value = value + int(new_array[i])
                        species_dict[array[0]] = value
                        abundance_list.append(value)
                abundance_list.sort()
                abundance_list.reverse()
                new_abundance_list = abundance_list[0:30]
                species_list = []
                for key in species_dict:
                    if species_dict[key] in new_abundance_list:
                        species_list.append(key)
                return species_list

    def get_new_species_xls(self, otu_species_list):  # 20170122 add by zhouxuan
        """
        根据物种列表信息，获取新的species表格文件
        :param otu_species_list: 物种列表信息
        :return: 新的species文件的路径
        """
        if os.path.exists(self.output_dir + "/rda_species.xls"):
            old_species_table = self.output_dir + "/rda_species.xls"
            new_species_table = self.work_dir + "rda_plot_species_data.xls"
        else:
            old_species_table = self.output_dir + "/cca_species.xls"
            new_species_table = self.work_dir + "cca_plot_species_data.xls"
        with open(old_species_table, "rb") as table, open(new_species_table, "a") as w:
            line = table.readlines()
            for l in line:
                content = l.strip().split("\t")
                if content[0] == "Species":
                    w.write('\t'.join(content) + "\n")
                else:
                    if content[0] in otu_species_list:
                        w.write('\t'.join(content) + "\n")
        return new_species_table

    def env_type(self):
        """
        判断环境因子表中是否存在分类型环境因子
        :param env_table:被判断的环境因子表
        :return:如果存在分类型环境因子返回“F”,不存在时返回“T”
        """
        file_path = self.env_table
        result_list = []
        with open(file_path, "rb") as f:
            content = f.readlines()
            judge_content = content[1].split("\t")[1:]  # 根据环境因子表的第一行数据进行判定
            for r in judge_content:
                num = r.split(".")  # 分隔小数
                ppp = ''
                for m in num:
                    Q = re.match('-(.*)', m)  # 解决负数问题
                    if Q:  # 去掉负号
                        m = Q.group(1)
                    print m
                    p = re.match('[0-9](.*)', m)
                    if p:
                        pass
                    else:
                        ppp = "1"
                        break
                if ppp == "1":
                    result_list.append("False")
                else:
                    result_list.append("True")
        if "False" in result_list:
            return "F"
        else:
            return "T"

    # def rm_(self, old_file):  # add by zhouxuan 20170401
    #     new_file = self.work_dir + '/new_env_.xls'
    #     with open(old_file, "rb") as f, open(new_file, "a") as w:
    #         content = f.readlines()
    #         for r in content:
    #             if r.startswith("#"):
    #                 r = r.split("\t")
    #                 readline = "sample" + "\t" +("\t").join(r[1:]) +"\n"
    #                 w.write(readline)
    #             else:
    #                 w.write(r)
    #     return new_file
