# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import math
from scipy.stats.distributions import t


def group_detail(groupfile):
    with open(groupfile, 'r') as g:
        ginfo = g.readlines()
        group_dict = {}
        group_num = {}
        for line in ginfo[1:]:
            line = line.split()
            group_dict[line[0]] = line[1]
        gnames = group_dict.values()
        for gname in gnames:
            group_num[gname] = gnames.count(gname)
        return group_num


def stat_info(statfile, groupfile):
    group_num_dict = group_detail(groupfile)
    with open(statfile, 'r') as s:
        shead = s.readline().strip('\n').split('\t')
        mean_dict = {}
        sd_dict = {}
        taxon_list = []
        for gname in group_num_dict.keys():
            mean_dict[gname] = []
            sd_dict[gname] = []
        while True:
            sline = s.readline()
            sline_list = sline.strip('\n').split('\t')
            if not sline:
                break
            taxon_list.append(sline_list[0])
            for gname in group_num_dict.keys():
                gmean = "mean(" + gname
                for foo in shead:
                    if gmean in foo:
                        index_site = shead.index(foo)
                        mean_dict[gname].append(float(sline_list[index_site].strip('\"')))
                        sd_dict[gname].append(float(sline_list[index_site + 1].strip('\"')))
        return mean_dict, sd_dict, taxon_list


def student(statfile, groupfile, coverage):
    """
    计算影响大小及置信区间
    """
    (mean_dict, sd_dict, taxon_list) = stat_info(statfile, groupfile)
    group_num_dict = group_detail(groupfile)
    group_names = group_num_dict.keys()
    length = len(taxon_list)
    with open('student_CI.xls', 'w') as w:
        w.write('\teffectsize\tlowerCI\tupperCI\n')
        for i in range(length):
            # calculate effect size, and CI
            meanG1 = mean_dict[group_names[0]][i]
            meanG2 = mean_dict[group_names[1]][i]
            dp = meanG1 - meanG2
            varG1 = (sd_dict[group_names[0]][i])**2
            varG2 = (sd_dict[group_names[1]][i])**2
            n1 = group_num_dict[group_names[0]]
            n2 = group_num_dict[group_names[1]]
            
            dof = n1 + n2 -2
            dof = n1 + n2 - 2
            pooledVar = ((n1 - 1)*varG1 + (n2 - 1)*varG2) / (n1 + n2 - 2)
            sqrtPooledVar = math.sqrt(pooledVar)
            denom = sqrtPooledVar * math.sqrt(1.0/n1 + 1.0/n2)
            tCritical = t.isf(0.5 * (1.0-coverage), dof)  # 0.5 factor accounts from symmetric nature of distribution
            lowerCI = dp - tCritical*denom
            upperCI = dp + tCritical*denom
            w.write('%s\t%s\t%s\t%s\n' % (taxon_list[i], '%0.4g' % (dp), '%0.4g' % (lowerCI), '%0.4g' % (upperCI)))


def welch(statfile, groupfile, coverage):
    (mean_dict, sd_dict, taxon_list) = stat_info(statfile, groupfile)
    group_num_dict = group_detail(groupfile)
    group_names = group_num_dict.keys()
    length = len(taxon_list)
    with open('welch_CI.xls', 'w') as w:
        w.write('\teffectsize\tlowerCI\tupperCI\n')
        for i in range(length):
            meanG1 = mean_dict[group_names[0]][i]
            meanG2 = mean_dict[group_names[1]][i]
            dp = meanG1 - meanG2

            varG1 = (sd_dict[group_names[0]][i])**2
            varG2 = (sd_dict[group_names[1]][i])**2
            n1 = group_num_dict[group_names[0]]
            n2 = group_num_dict[group_names[1]]

            normVarG1 = varG1 / n1
            normVarG2 = varG2 / n2
            unpooledVar = normVarG1 + normVarG2
            sqrtUnpooledVar = math.sqrt(unpooledVar)
            dof = (unpooledVar*unpooledVar) / ( (normVarG1*normVarG1)/(n1-1) + (normVarG2*normVarG2)/(n2-1) )
            # CI
            tCritical = t.isf(0.5 * (1.0-coverage), dof) # 0.5 factor accounts from symmetric nature of distribution
            lowerCI = dp - tCritical*sqrtUnpooledVar
            upperCI = dp + tCritical*sqrtUnpooledVar
            w.write('%s\t%s\t%s\t%s\n' % (taxon_list[i], '%0.4g' % (dp), '%0.4g' % (lowerCI), '%0.4g' % (upperCI)))

