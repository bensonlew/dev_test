# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import numpy as np
import matplotlib.pyplot as plt

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
        pvalue_list = []
        for gname in group_num_dict.keys():
            mean_dict[gname] = []
            sd_dict[gname] = []
        while True:
            sline = s.readline()
            sline_list = sline.strip('\n').split('\t')
            if not sline:
                break
            length = len(sline_list[0].split(';'))
            taxon_list.append(sline_list[0].split(';')[length-1])
            pvalue_list.append(sline_list[6])
            for gname in group_num_dict.keys():
                gmean = "mean(" + gname
                for foo in shead:
                    if gmean in foo:
                        index_site = shead.index(foo)
                        mean_dict[gname].append(float(sline_list[index_site + 1].strip('\"')))
                        sd_dict[gname].append(float(sline_list[index_site + 2].strip('\"')))
        return mean_dict, sd_dict, taxon_list, pvalue_list

def errorbar(statfile, groupfile, cifile, outfile):
    (mean_dict, sd_dict, taxon_list,pvalue_list) = stat_info(statfile, groupfile)
    g1 = mean_dict.keys()[0]
    m1 = mean_dict[g1]
    g2 = mean_dict.keys()[1]
    m2 = mean_dict[g2]
    mean1 = []
    mean2 = []
    taxon = []
    lci = []
    uci = []
    es = []
    with open(cifile, 'rb') as c:
        cinfo = c.readlines()
        effectsize = []
        lowerci = []
        upperci = []
        for l in cinfo[1:]:
            line = l.strip('\n').split('\t')
            effectsize.append(float(line[1]))
            lowerci.append(float(line[2]))
            upperci.append(float(line[3]))
    for i in range(len(effectsize)):
        lowerci[i] = effectsize[i]-lowerci[i]
        upperci[i] = upperci[i] - effectsize[i]
    for i in range(len(pvalue_list)):
        if float(pvalue_list[i]) <= 0.05:
            mean1.append(m1[i])
            mean2.append(m2[i])
            taxon.append(taxon_list[i])
            lci.append(lowerci[i])
            uci.append(upperci[i])
            es.append(effectsize[i])
    if len(taxon) != 0:
        fig, (ax1,ax2) = plt.subplots(ncols=2,sharey=True)
        plt.sca(ax1)
        error_config = {'ecolor': '0.3'}
        n_groups = len(mean1)
        ax1.set_ylim(0,n_groups+1)
        index = np.arange(1,n_groups+1)
        bar_width = 0.35
        ax1.barh(index, mean1, bar_width, alpha=0.4, color='b', error_kw=error_config, label=g1)
        ax1.barh(index + bar_width, mean2, bar_width, alpha=0.4, error_kw=error_config, color='r', label=g2)
        plt.ylabel('taxon_name', fontsize=6)
        plt.xlabel('mean propotion(%)', fontsize=6)
        plt.yticks(range(1,len(mean1)+1), taxon, fontsize=6)
        plt.legend(fontsize=6)
        # plot error bar
        plt.sca(ax2)
        ax2.set_ylim(0,n_groups+1)
        x = np.array(es)
        y = np.arange(1,n_groups+1)
        error = [lci, uci]

        ax2.errorbar(x, y, xerr=error, fmt='o', alpha=0.4,ecolor='b')
        plt.xlabel('difference between proportions(%)', fontsize=6)
        # plt.yticks(range(1,n_groups+1), taxon_list, fontsize=6)
    else:
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        plt.ylabel('no active data to plot (all pvalue > 0.05)')
        plt.xlabel('no active data to plot (all pvalue > 0.05)')
        plt.title('no active data to plot (all pvalue > 0.05)')
    plt.savefig(outfile, dpi=400, bbox_inches='tight')
    plt.show()

# errorbar('C:\Users\ping.qiu.MAJORBIO\Desktop\student_result.xls','C:\Users\ping.qiu.MAJORBIO\Desktop\student_group','C:\Users\ping.qiu.MAJORBIO\Desktop\student_CI.xls', 'errorbar.png')
     
