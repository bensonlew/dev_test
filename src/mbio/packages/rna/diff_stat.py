# !/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "qiuping"
# last_modify:20161101
from collections import defaultdict
import math
from math import log

class Express(object):
    def __init__(self):
        self.gene = None
        self.counts = {}  # 每个样本对应的count值
        self.fpkms = {}  # 每个样本对应的fpkm值


class DiffStat(object):
    def __init__(self):
        self.express_info = {}
        self.samples = []
        self.group_info = defaultdict(list)

    def get_express_info(self, countfile, fpkmfile):
        """
        countfile：基因计数矩阵表
        fpkmfile：基因表达量矩阵表
        return：express_info字典，键为基因名称，值为Express对象
        """
        with open(countfile, 'rb') as c, open(fpkmfile, 'rb') as f:
            head = c.readline().strip('\n').split('\t')
            self.samples = head[1:]
            for line in c:
                line = line.strip('\n').split('\t')
                gene = line[0]
                foo = Express()
                foo.gene = gene
                for sam in self.samples:
                    foo.counts[sam] = float(line[head.index(sam)])
                self.express_info[gene] = foo

            f.readline()
            for line in f:
                line = line.strip('\n').split('\t')
                gene = line[0]
                tmp = {}
                for sam in self.samples:
                    tmp[sam] = float(line[head.index(sam)])
                self.express_info[gene].fpkms = tmp
        return self.express_info

    def get_group_info(self, group):
        """
        group:分组文件
        return：字典，键为分组名称，值为该分组对应的所有样本名的列表；
        eg: {'A':[1,2,3], 'B':[4,5,6]}
        """
        with open(group, 'rb') as g:
            g.readline()
            for line in g:
                line = line.strip('\n').split()
                self.group_info[line[1]].append(line[0])
        return self.group_info

    def get_mean(self, samples, count_dict, fpkm_dict):
        """
        samples: 样本的列表
        count_dict：全部样本对应的counts值的字典
        fpkm_dict：全部样本对应的fpkm值的字典
        return：samples中的样本的count、fpkm的均值
        """
        sum_count = 0
        sum_fpkm = 0
        for sam in samples:
            sum_count += count_dict[sam]
            sum_fpkm += fpkm_dict[sam]
        mean_count = float(sum_count) / len(samples)
        mean_fpkm = float(sum_fpkm) / len(samples)
        return round(mean_count, 3), round(mean_fpkm, 3)

    def diff_stat(self, express_info, edgr_result, control, other, output, group_info=None, regulate=True, diff_ci=0.05,
                  fc=2, diff_fdr_ci=0.05,pvalue_padjust="padjust"):
        """
        express_info:字典，键为基因名，值为Express对象
        edgr_result:edgr分析得到的结果文件
        control:对照组/样本名
        other:实验组/样本名
        group_info:字典，键为分组名称，值为该分组对应的所有样本名的列表
        regulate:是否做上下调分析
        diff_ci:显著差异水平
        #fc:差异倍数，当logfc大于该值时则认为上调，否则下调
        fc:按照fc过滤，选择出大于此fc值，
        pvalue_padjust:padjust-对应diff_fdr_ci pvalue-对应diff_ci  默认按照diff_fdr_ci筛选
        """
        with open(edgr_result, 'rb') as r, open('%s/%s_vs_%s_edgr_stat.xls' % (output, control, other), 'wb') as w:
            r.readline()
            count_ = []
            fpkm_ = []
            if group_info:
                con_sams = group_info[control]
                oth_sams = group_info[other]
                samples_ = []
                samples_.extend(con_sams)
                samples_.extend(oth_sams)
                print samples_
                for ss in samples_:
                    count_.append("{}_count".format(ss))
                    fpkm_.append("{}_fpkm".format(ss))
                head = "seq_id\t%s\t%s\t%s_count\t%s_count\t%s_fpkm\t%s_fpkm\tlog2fc\tlogCPM\tpvalue\tpadjust\tsignificant\tregulate\tncbi\n" % (
                    "\t".join(count_), "\t".join(fpkm_), control, other, control, other)
            else:
                head = "seq_id\t%s_count\t%s_count\t%s_fpkm\t%s_fpkm\tlog2fc\tlogCPM\tpvalue\tpadjust\tsignificant\tregulate\tncbi\n" % (
                control, other, control, other)
            w.write(head)
            for line in r:
                line = line.strip('\n').split('\t')
                gene = line[0]
                pvalue = float(line[-2])
                if fc == 2:
                    logCPM = float(line[-3])
                else:
                    print fc
                    logCPM = float(line[-3])
                    logCPM = math.pow(2, logCPM)
                    logCPM = math.log(logCPM, fc)
                fdr = float(line[-1])
                counts = express_info[gene].counts
                fpkms = express_info[gene].fpkms
                if group_info:
                    # con_sams = group_info[control]
                    # oth_sams = group_info[other]
                    control_count, control_fpkm = self.get_mean(con_sams, counts, fpkms)
                    other_count, other_fpkm = self.get_mean(oth_sams, counts, fpkms)
                else:
                    control_count = express_info[gene].counts[control]
                    control_fpkm = express_info[gene].fpkms[control]
                    other_count = express_info[gene].counts[other]
                    other_fpkm = express_info[gene].fpkms[other]
                # lfc = (other_fpkm + 0.1) / (control_fpkm + 0.1)
                # logfc = round(math.log(lfc, 2), 3)
                """
                if fc == 2:
                    logfc = float(line[-4])
                else:
                    logfc = float(line[-4])
                    logfc = math.pow(2, logfc)
                    logfc = math.log(logfc, fc)
                """
                import math
                if fc:  #如果设置fc倍数变化，则按照fc筛选
                    filter_fc = float(log(fc)/log(2))
                    if float(line[-4]) <filter_fc:
                        continue  #跳出此次循环
                    else:
                        logfc=float(line[-4])
                else:  #否则不过滤fc
                    logfc= float(line[-4])
                ncbi = 'https://www.ncbi.nlm.nih.gov/gquery/?term=' + gene
                # if regulate:
                # print regulate
                if logfc > 0:
                    reg = 'up'
                elif logfc < 0:
                    reg = 'down'
                else:
                    reg = 'no change'
                    # else:
                    # reg = 'undone'
                if pvalue_padjust == 'padjust':
                    if fdr<diff_fdr_ci:
                        sig = 'yes'
                    else:
                        sig = 'no'
                if pvalue_padjust == 'pvalue':
                    if pvalue<diff_ci:
                        sig='yes'
                    else:
                        sig='no'
                """
                if pvalue < diff_ci and fdr < diff_fdr_ci:
                    sig = 'yes'
                else:
                    sig = 'no'
                """
                if group_info:
                    count_data = []
                    fpkm_data = []
                    for ss in samples_:
                        count_.append("{}_count".format(ss))
                        fpkm_.append("{}_fpkm".format(ss))
                        count_data.append(str(counts[ss]))
                        fpkm_data.append(str(fpkms[ss]))

                    w.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (gene, "\t".join(count_data),"\t".join(fpkm_data),control_count, other_count, control_fpkm, other_fpkm, '%0.4g' %logfc,'%0.4g' %logCPM, '%0.4g' % pvalue, '%0.4g' % fdr, sig, reg, ncbi))
                else:
                    w.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                    gene, control_count, other_count, control_fpkm, other_fpkm, '%0.4g' % logfc, '%0.4g' % logCPM,
                    '%0.4g' % pvalue, '%0.4g' % fdr, sig, reg, ncbi))


a = DiffStat()
path = "/mnt/ilustre/users/sanger-dev/workspace/20170509/Single_rsem_stringtie_zebra_fpkm_6/Expresstest/output/rsem"
a.get_express_info(path + "/transcripts.counts.matrix", path + "/transcripts.TMM.fpkm.matrix")
edgr_result = "/mnt/ilustre/users/sanger-dev/workspace/20170509/Single_rsem_stringtie_zebra_fpkm_6/Expresstest/DiffExp1/edger_result/transcripts.counts.matrix.A_vs_B.edgeR.DE_results"
# ERR1621569      ERR1621480      ERR1621391      ERR1621658
group_info = {"A": ["ERR1621569", "ERR1621480"], "B": ["ERR1621391", "ERR1621658"]}
output = "/mnt/ilustre/users/sanger-dev/sg-users/konghualei/ref_rna/tofiles"
a.diff_stat(express_info=a.express_info, edgr_result=edgr_result, control="A", other="B", output=output,
            group_info=group_info, regulate=True, diff_ci=0.05,diff_fdr_ci=0.05)
# a.get_express_info('/mnt/ilustre/users/sanger-dev/workspace/20161101/TestBase_tsn_50/ExpAnalysis/MergeRsem/genes.counts.matrix', '/mnt/ilustre/users/sanger-dev/workspace/20161101/TestBase_tsn_50/ExpAnalysis/MergeRsem/genes.TMM.fpkm.matrix')
# # print a.express_info
# a.diff_stat(express_info=a.express_info, edgr_result='/mnt/ilustre/users/sanger-dev/workspace/20161101/TestBase_tsn_50/ExpAnalysis/DiffExp/edger_result/genes.counts.matrix.E18_1_vs_E18_2.edgeR.DE_results', control='E18_1', other='E18_2', output='/mnt/ilustre/users/sanger-dev/workspace/20161101/TestBase_tsn_50/ExpAnalysis/MergeRsem/', group_info=None, regulate=True, diff_ci=0.05)
