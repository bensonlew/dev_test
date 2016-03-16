# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import numpy as np
import matplotlib.pyplot as plt


def stat_info(statfile, sample1):
    with open(statfile, 'rb') as s:
        sinfo = s.readlines()
        taxon = []
        pro1 = []
        pro2 = []
        if sample1 in sinfo[0][1]:
            index_1 = 1
            index_2 = 2
        else:
            index_2 = 1
            index_1 = 2
        for i in sinfo[1:]:
            if float(i.strip('\n').split('\t')[4]) <= 0.05:
                name = i.strip('\n').split('\t')[0].split(';')
                taxon.append(name[len(name)-1])
                pro1.append(i.strip('\n').split('\t')[index_1])
                pro2.append(i.strip('\n').split('\t')[index_2])
    return taxon, pro1, pro2


def barplot(statfile, sample1, sample2, outfile):
    taxon, pro1, pro2 = stat_info(statfile, sample1)
    if len(taxon) != 0:
        error_config = {'ecolor': '0.3'}
        n_groups = len(taxon)
        ax = plt.figure(figsize=(8, 3)).add_subplot(1, 1, 1)
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.4
        plt.bar(index, pro1, bar_width, alpha=opacity, color='b', error_kw=error_config, label=sample1)
        plt.bar(index + bar_width, pro2, bar_width, alpha=opacity, color='r', error_kw=error_config, label=sample2)
        plt.xlabel('taxon_name', fontsize=6)
        plt.ylabel('propotion of sequences(%)', fontsize=6)
        plt.title('bar plot of two sample', fontsize=8)
        ax.set_xticks(range(len(taxon)))
        # plt.tight_layout(rect=(0, 0.15, 1, 1))
        ax.set_xticklabels(list(taxon), rotation=90, fontsize=6)
        plt.legend(fontsize=6)
    else:
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        plt.ylabel('no active data to plot (all pvalue > 0.05)')
        plt.xlabel('no active data to plot (all pvalue > 0.05)')
        plt.title('no active data to plot (all pvalue > 0.05)')
    plt.savefig(outfile, dpi=400, bbox_inches='tight')
    # plt.show()
# barplot('C:\Users\ping.qiu.MAJORBIO\Desktop\check\\fisher_result.xls', 'sample1', 'sample2')


def profile_barplot(statfile, sample1, sample2, outfile):
    taxon, pro1, pro2 = stat_info(statfile, sample1)
    if len(taxon) != 0:
        error_config = {'ecolor': '0.3'}
        n_groups = len(taxon)
        index = np.arange(n_groups)
        bar_width = 0.40
        opacity = 0.4
        plt.barh(index, pro1, bar_width, alpha=opacity, color='b', error_kw=error_config, label=sample1)
        plt.barh(index + bar_width, pro2, bar_width, alpha=opacity, error_kw=error_config, color='r', label=sample2)
        plt.ylabel('taxon_name', fontsize=6)
        plt.xlabel('propotion of sequences(%)', fontsize=6)
        plt.title('profile bar plot of two sample', fontsize=8)
        plt.yticks(range(len(taxon)), taxon, fontsize=6)
        plt.legend(fontsize=6)
        # plt.show()
    else:
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        plt.ylabel('no active data to plot (all pvalue > 0.05)')
        plt.xlabel('no active data to plot (all pvalue > 0.05)')
        plt.title('no active data to plot (all pvalue > 0.05)')
    plt.savefig(outfile, dpi=400, bbox_inches='tight')

# profile_barplot('C:\Users\ping.qiu.MAJORBIO\Desktop\check\\fisher_result.xls', 'sample1', 'sample2')


def extended_error_bar(statfile, sample1, sample2, cifile, outfile):
    taxon, pro1, pro2 = stat_info(statfile, sample1)
    # deal with cifile, get lowerci,upperci,effectsize
    if len(taxon) != 0:
        with open(cifile, 'rb') as c:
            cinfo = c.readlines()
            effectsize = []
            lowerci = []
            upperci = []
            taxon_ci = []
            for l in cinfo[1:]:
                line = l.strip('\n').split('\t')
                effectsize.append(float(line[1]))
                lowerci.append(float(line[2]))
                upperci.append(float(line[3]))
                name = line[0].split(';')
                taxon_ci.append(name[len(name)-1])
        effectsize_plot = []
        lowerci_plot = []
        upperci_plot = []
        for n in taxon:
            index = taxon_ci.index(n)
            effectsize_plot.append(effectsize[index])
            lowerci_plot.append(lowerci[index])
            upperci_plot.append(upperci[index])
        for i in range(len(effectsize_plot)):
            lowerci_plot[i] = effectsize_plot[i]-lowerci_plot[i]
            upperci_plot[i] = upperci_plot[i] - effectsize_plot[i] 
        # plot profile bar
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 2, 1)
        # fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=True)
        error_config = {'ecolor': '0.3'}
        n_groups = len(taxon)
        index = np.arange(1, n_groups+1)
        bar_width = 0.35
        opacity = 0.4
        ax1.barh(index, pro1, bar_width, alpha=opacity, color='b', error_kw=error_config, label=sample1)
        ax1.barh(index + bar_width, pro2, bar_width, alpha=opacity, error_kw=error_config, color='r', label=sample2)
        plt.ylabel('taxon_name', fontsize=6)
        plt.xlabel('propotion(%)', fontsize=6)
        plt.yticks(range(1, len(taxon)+1), taxon, fontsize=6)
        plt.legend(fontsize=6)
        # plot error bar
        ax2 = fig.add_subplot(1, 2, 2)
        x = np.array(effectsize_plot)
        y = range(1, len(effectsize_plot)+1)
        error = [lowerci_plot, upperci_plot]
        ax2.errorbar(x, y, xerr=error, fmt='o', alpha=0.4,ecolor='b')
        plt.xlabel('difference between proportions(%)', fontsize=6)
    else:
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        plt.ylabel('no active data to plot (all pvalue > 0.05)')
        plt.xlabel('no active data to plot (all pvalue > 0.05)')
        plt.title('no active data to plot (all pvalue > 0.05)')
    plt.savefig(outfile, dpi=400, bbox_inches='tight')
    plt.show()

# extended_error_bar('C:\Users\ping.qiu.MAJORBIO\Desktop\CItest\\fisher_result', 'sample1', 'sample2', 'C:\Users\ping.qiu.MAJORBIO\Desktop\CItest\\fisher-CI-2')
