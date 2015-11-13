# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# __version__ = 'v1.0'
# __last_modified__ = '20151110'
"""

"""

import os
import argparse
import platform


def get_argu():
    """
    命令行模式下运行此脚本的参数获取方法
    路径请使用'/',不要使用'\\'
<<<<<<< HEAD
    :param return: 输出文件夹，距离矩阵，分组信息
=======

>>>>>>> f381101c0b2ea19c0657b1efce841f5806cef0b8
    """
    parse = argparse.ArgumentParser(prog='dbrda_r.py',
                                    usage='关于此脚本的说明',
                                    description='输入矩阵和分组信息文件，\
                                    利用R软件计算分析db-RDA。',
                                    epilog='请确保参数准确',
                                    version='v1.0',
                                    parents='')
    parse.add_argument('-d', '--distance_matrix',
                       required=True, help='输入距离矩阵，矩阵必须对称')
    parse.add_argument('-m', '--map_file',
                       required=True, help='分组信息文件，\
                       必须只有一种分组方案，表头注释‘#’开头，tab分隔符')
    parse.add_argument('-o', '--output',
                       required=True, help='输出文件夹')
    args = parse.parse_args()

    outputdir = args.output.rstrip('/')
    if os.path.exists(outputdir):
        pass
    else:
        os.makedirs(outputdir)
    dis_matrix = args.distance_matrix
    maping = args.map_file
    return outputdir, dis_matrix, maping


def create_r(outputdir, dis_matrix, maping):
    """
    生成可以运行的R脚本

    :param outputdir: 输出文件夹
    :param dis_matrix: 输入矩阵
    :param maping: 输入分组文件
    """
    tempr = open(outputdir + '/temp_r.R', 'w')
#     explain = u"""#本文件的基本说明:
# #本文件是进行dbRDA运算生成的中间文件，
# #程序正常运行结束后会被自动删除。
    # """
    # tempr.write(explain)
    r_script = """mydata <- read.table('%s')
mydata <- as.matrix(mydata)
Factor_ <- read.table('%s')
Factor_ <- as.factor(Factor_$V2)
library(vegan)
capscale.result<- capscale(as.dist(mydata)~Factor_,data.frame(Factor_))
sig.test.result <- permutest(capscale.result,permutations = 999)

sink('%s/db_rda_results.txt')
print(capscale.result)
print(sig.test.result)
sink(NULL)
pdf('%s/db_rda.pdf')
tempplot <- plot(capscale.result, display=c('wa','bp'))
dev.off()
write.table(tempplot$sites, '%s/db_rda_sites.temp.txt',sep = '\\t',col.names = TRUE)
write.table(tempplot$biplot, '%s/db_rda_factor.temp.txt',sep = '\\t')
""" % (dis_matrix, maping, outputdir, outputdir, outputdir, outputdir)
    tempr.write(r_script)


def run_r_script(script, delscript=True):
    """
    分平台运行R脚本，运行完成脚本会被删除
    :param script:R脚本路径（路径使用斜杠，不要使用反斜杠）
    """
    if platform.system() == 'Windows':
        os.system('R CMD BATCH --vanilla --slave %s ' % (script))
    elif platform.system() == 'Linux':
        os.system('Rscript %s' % (script))
    else:
        pass
    if delscript:
        os.remove(script)
        os.remove(os.path.dirname(script) + '/temp_r.Rout')


def format_result(outputdir):
    """
    对生成的文件进行格式整理，删除文件中的引号，添加开头的制表符
    :param outputdir:输出文件夹
    :pararm return:
    """
    sites = open('%s/db_rda_sites.temp.txt' % (outputdir))
    biplot = open('%s/db_rda_factor.temp.txt' % (outputdir))
    new_sites = open('%s/db_rda_sites.txt' % (outputdir), 'w')
    new_biplot = open('%s/db_rda_factor.txt' % (outputdir), 'w')
    new_sites.write('\t')
    new_biplot.write('\t')
    for line in sites:
        new_sites.write(line.replace('\"', ''))
    for line in biplot:
        new_biplot.write(line.replace('\"', ''))
    sites.close()
    biplot.close()
    new_biplot.close()
    new_sites.close()
    os.remove('%s/db_rda_sites.temp.txt' % (outputdir))
    os.remove('%s/db_rda_factor.temp.txt' % (outputdir))


def db_rda(dis_matrix, maping, outputdir):
    """
    输入距离矩阵，分组信息，输出文件夹，进行db_rda分析
    :pararm return: 成功完成返回‘0’
    """
    create_r(outputdir, dis_matrix, maping)
    script = outputdir + '/temp_r.R'
    run_r_script(script)
    format_result(outputdir)
    return 0


def main():
    """
    脚本运行主程序
    """
    outputdir, dis_matrix, maping = get_argu()
    create_r(outputdir, dis_matrix, maping)
    script = outputdir + '/temp_r.R'
    run_r_script(script)
    format_result(outputdir)

if __name__ == '__main__':
    main()
