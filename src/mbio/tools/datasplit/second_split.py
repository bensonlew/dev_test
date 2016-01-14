# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""二次拆分程序 用于对父样本(混样)进行二次拆分"""
from __future__ import division
import os
import re
import errno
import subprocess
import multiprocessing
from collections import defaultdict
from biocluster.config import Config
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError
from mbio.packages.datasplit.miseq_split import reverse_complement, code2index, code2primer, str_check


class SecondSplitAgent(Agent):
    """
    二次拆分
    """
    def __init__(self, parent=None):
        super(SecondSplitAgent, self).__init__(parent)
        self._run_mode = "ssh1"
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'},  # 样本拆分信息表
            {'name': 'unzip_path', 'type': "string"}  # bcl2fastq软件拆分出来的fastq解压后的输出目录
        ]
        self.add_option(options)

    def check_option(self):
        """
        参数检测
        """
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        if not self.option('unzip_path'):
            raise OptionError("参数unzip_path不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 3
        self._memory = ''


class SecondSplitTool(Tool):
    """
    """
    def __init__(self, config):
        super(SecondSplitTool, self).__init__(config)
        self._version = 1.0
        self.option('sample_info').get_info()
        self.pear_path = os.path.join(Config().SOFTWARE_DIR, "datasplit/bin/pear")
        self.short_child = defaultdict(int)
        self.child_num = defaultdict(int)
        self.p_no_index = defaultdict(int)
        self.p_chimera_index = defaultdict(int)
        self.primer_miss = defaultdict(int)
        self.f_index = dict()
        self.r_index = dict()
        self.f_primer = dict()
        self.r_primer = dict()
        self.f_varbase = dict()
        self.r_varbase = dict()
        self.f_chomp_length = dict()
        self.r_chomp_length = dict()
        self.dump_database_to_memery()

    def dump_database_to_memery(self):
        """
        实际运行过程中，发现一直打开barcode.list和primer.list导致程勋运行缓慢，
        所以先把里面的内容读取到内存中以后再开始运行程序
        """
        for c_id in self.option('sample_info').prop["child_ids"]:
            index_code = self.option('sample_info').child_sample(c_id, "index")
            (self.f_index[c_id], self.r_index[c_id], self.f_varbase[c_id], self.r_varbase[c_id]) = code2index(index_code)
            primer_code = self.option('sample_info').child_sample(c_id, "primer")
            (self.f_primer[c_id], self.r_primer[c_id]) = code2primer(primer_code)
            self.f_chomp_length[c_id] = len(self.f_index[c_id]) +\
                len(self.f_primer[c_id]) + len(self.f_varbase[c_id])
            self.r_chomp_length[c_id] = len(self.r_index[c_id]) +\
                len(self.r_primer[c_id]) + len(self.r_varbase[c_id])

    def make_ess_dir(self):
        """
        为二次拆分创建必要的目录
        """
        merge_dir = os.path.join(self.work_dir, "merge")
        stat_dir = os.path.join(self.work_dir, "stat")
        child_dir = os.path.join(self.work_dir, "child_seq")
        dir_list = [merge_dir, stat_dir, child_dir]
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录失败")

    def pear(self):
        """
        用软件pear对bcl2fastq的结果进行merage
        """
        merge_dir = os.path.join(self.work_dir, "merge")
        i = 0
        cmd_list = list()
        for p in self.option('sample_info').prop["parent_sample"]:
            if p["has_child"]:
                i += 1
                file_r1 = os.path.join(self.option('unzip_path'), p['mj_sn'] + "_r1.fastq")
                file_r2 = os.path.join(self.option('unzip_path'), p['mj_sn'] + "_r2.fastq")
                pearstr = (self.pear_path + "  -p 1.0 -j 16 -f " + file_r1 + " -r " + file_r2 + " -o " +
                           merge_dir + "/pear_" + p['mj_sn'] + "> " +
                           merge_dir + "/" + p['mj_sn'] + ".pear.log")
                command = subprocess.Popen(pearstr, shell=True)
                cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行pear")
            mycmd.communicate()
        for mycmd in cmd_list:
            if mycmd.returncode == 0:
                self.logger.info("pear运行完成")
            else:
                self.set_error("pear运行出错")

    def pear_stat(self):
        """
        对pear输出的log文件进行统计,输出到pear.stat当中
        """
        merge_dir = os.path.join(self.work_dir, "merge")
        stat_dir = os.path.join(self.work_dir, "stat")
        for p in self.option('sample_info').prop["parent_sample"]:
            if p["has_child"]:
                file_name = os.path.join(merge_dir, p['mj_sn'] + ".pear.log")
                stat_name = os.path.join(stat_dir, "pear.stat")
                with open(file_name, 'r') as r:
                    with open(stat_name, 'a') as a:
                        for line in r:
                            if re.search(r'Assembled\sreads\s\.\.', line):
                                total_reads = re.search(r'/\s(.+)\s\(', line).group(1)
                                total_reads = re.sub(r',', '', total_reads)
                                rate = re.search(r'\((.+)\)', line).group(1)
                                a.write(p['sample_id'] + "\t" + total_reads + "\t" + rate + "\n")
            else:
                file_name = os.path.join(self.option('unzip_path'), p['mj_sn'] + "_r1.fastq")
                num_lines = sum(1 for line in open(file_name))
                total_reads = num_lines / 4
                stat_name = os.path.join(stat_dir, "pear.stat")
                with open(stat_name, 'a') as a:
                    a.write(p['sample_id'] + "\t" + str(int(total_reads)) + "\t" + '0' + "\n")

    def split(self):
        """
        对所有的父样本进行遍历检测，如果该父样本有子样本，则进行2次拆分
        """
        process_list = list()
        for p_id in self.option('sample_info').prop["parent_ids"]:
            if self.option('sample_info').parent_sample(p_id, "has_child"):
                self.logger.info(p_id + "开始拆分")
                p = multiprocessing.Process(target=self.s_split, args=(p_id,))
                process_list.append(p)
        for p in process_list:
            p.start()
        for p in process_list:
            p.join()

    def my_stat(self, p_id):
        p_stat = os.path.join(self.work_dir, "stat", "p_stat.stat")
        with open(p_stat, 'ab') as w:
            w.write(str(p_id) + "\t" + str(self.p_no_index[p_id]) + "\t" +
                    str(self.p_chimera_index[p_id]) + "\t" + str(self.primer_miss[p_id]) + "\n")
        c_stat = os.path.join(self.work_dir, "stat", "c_stat.stat")
        with open(c_stat, 'ab') as w:
            child_ids = self.option('sample_info').find_child_ids(p_id)
            for c_id in child_ids:
                w.write(str(c_id) + "\t" + str(self.child_num[c_id]) + "\t" + str(self.short_child[c_id]) + "\n")

    def s_split(self, p_id):
        """
        接收一个父样本的id号，进行二次拆分
        :param p_id: 父样本的id号
        """
        c = 0
        mj_sn = self.option('sample_info').parent_sample(p_id, "mj_sn")
        sourcefile = os.path.join(self.work_dir, "merge", "pear_" + mj_sn + ".assembled.fastq")
        with open(sourcefile, 'r') as r:
            self.logger.info("process sequence: 1")
            for line in r:
                c += 1
                self.logger_process(c, p_id)
                head = line.rstrip('\n')
                ori_seq = r.next().rstrip('\n')
                direction = r.next()
                ori_quality = r.next().rstrip('\n')
                rev_ori_seq = ori_seq[::-1]
                rev_ori_seq = reverse_complement(rev_ori_seq)
                rev_ori_quality = ori_seq[::-1]
                f_level = 0
                r_level = 0
                (f_level, c_id) = self._try_match(ori_seq, p_id)
                if f_level == 4:  # 当f_level==4时，表示这条序列已经正确匹配，应该进行长度校验后，确定是否输出
                    name = self._length_check(c_id, ori_seq)
                    if name != "":
                        self._write_match_file(name, head, ori_seq, direction, ori_quality,
                                               c_id)
                else:  # 当一条序列不能正确匹配的时候，将这条序列翻转，再进行一次匹配
                    (r_level, c_id) = self._try_match(rev_ori_seq, p_id)
                    if r_level == 4:
                        name = self._length_check(c_id, rev_ori_seq)
                        if name != "":
                            self._write_match_file(name, head, rev_ori_seq, direction, rev_ori_quality,
                                                   c_id)
                    else:
                        if f_level < r_level:
                            f_level = r_level
                        if f_level == 1:
                            self.p_no_index[p_id] += 1
                        elif f_level == 2:
                            self.p_chimera_index[p_id] += 1
                        elif f_level == 3:
                            self.primer_miss[p_id] += 1
        self.my_stat(p_id)

    def logger_process(self, count, p_id):
        if count % 10000 == 0:
            self.logger.info("process sequence " + p_id + " : " + " " + str(count))

    def _write_match_file(self, name, head, seq, direction, quality, c_id):
        with open(name, 'a') as a:
            a.write(head + "\n")
            a.write(seq[self.f_chomp_length[c_id]:-self.r_chomp_length[c_id]] + "\n")
            a.write(direction)
            a.write(quality[self.f_chomp_length[c_id]:-self.r_chomp_length[c_id]] + "\n")

    def _length_check(self, c_id, seq):
        """
        检查一条序列的长度是否满足要求，如果满足则返回一个文件名，不满足则返回空
        :param c_id: 子样本id
        :param seq: 需要进行长度校验的序列
        """
        if len(seq) - self.f_chomp_length[c_id] - self.r_chomp_length[c_id] < int(self.option('sample_info').child_sample(c_id, "filter_min")):
            name = ""
            self.short_child[c_id] += 1
        else:
            name = os.path.join(self.work_dir, "child_seq",
                                self.option('sample_info').child_sample(c_id, 'mj_sn') + "_" +
                                self.option('sample_info').child_sample(c_id, 'primer') + ".fastq")
            self.child_num[c_id] += 1
        return name

    def _try_match(self, seq, p_id):
        """
        对一条序列进行匹配，尝试把它拆分到子样本
        :param seq: 序列
        :return: (level, c_id)
        level == 1  未找到index
        level == 2  index错配
        level == 3  primer错配
        level == 4  正确匹配
        c_id  子样本的id
        """
        level = 0
        for c_id in self.option('sample_info').find_child_ids(p_id):
            new_seq = seq[int(self.f_varbase[c_id]):]  # 去掉左边的可变剪切
            length = len(self.f_index[c_id])
            real_f_index = new_seq[0:length]  # 获取这条序列的index
            new_seq = new_seq[length:]  # 去掉左边的index
            missmatch = str_check(real_f_index, self.f_index[c_id])  # 获取错配数
            if missmatch > int(self.option('sample_info').child_sample(c_id, "index_miss")):
                level = 1
                continue
            else:  # 如果左端index匹配，那么继续验证右端index是否也匹配
                rev_new_seq = new_seq[::-1]
                rev_new_seq = rev_new_seq[int(self.r_varbase[c_id]):]
                rev_new_seq = reverse_complement(rev_new_seq)
                length = len(self.r_index[c_id])
                real_r_index = rev_new_seq[0:length]
                rev_new_seq = rev_new_seq[length:]
                missmatch = str_check(real_r_index, self.r_index[c_id])
                if missmatch > int(self.option('sample_info').child_sample(c_id, "index_miss")):
                    level = 2
                    continue
                else:  # 如果两端的index都匹配了，那么验证左端primer是否匹配
                    length = len(self.f_primer[c_id])
                    real_f_primer = new_seq[0:length]
                    missmatch = str_check(real_f_primer, self.f_primer[c_id])
                    if missmatch > int(self.option('sample_info').child_sample(c_id, "primer_miss")):
                        level = 3
                        continue
                    else:  # 如果左端primer匹配了，检测primer引物是否匹配
                        length = len(self.r_primer[c_id])
                        real_r_primer = rev_new_seq[0:length]
                        missmatch = str_check(real_r_primer, self.r_primer[c_id])
                        if missmatch > int(self.option('sample_info').child_sample(c_id, "primer_miss")):
                            level = 3
                            continue
                        else:
                            level = 4
                            return (level, c_id)
        return(level, "")

    def run(self):
        super(SecondSplitTool, self).run()
        self.make_ess_dir()
        self.pear()
        self.pear_stat()
        self.split()
        self.end()
