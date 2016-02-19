# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# __version__ = 'v1.0'
# __last_modified__ = '20151117'
"""

"""

import os
import platform
from mako.template import Template
from biocluster.config import Config


def create_r_new(otu_file, env_file, output_dir, distance_algorithm):
    """
    生成可以运行的R脚本

    :param otu_file: 输出文件夹
    :param env_file: 环境因子文件
    :param output_dir: 输出文件夹
    :param distance_algorithm: 距离计算方法（此处计算方法是R中的vegdist方法）
    """
    output_dir = output_dir.rstrip('\\')
    output_dir = output_dir.rstrip('/')
    with open(env_file) as env:
        env_formula = ' + '.join(env.readline().strip().split('\t')[1:])
    this_file_dir = os.path.dirname(os.path.realpath(__file__))
    f = Template(filename=this_file_dir + '/db_rda.r')
    content_r = f.render(otu_file=otu_file, env_file=env_file, output_dir=output_dir,
                         distance_algorithm=distance_algorithm, env_formula=env_formula)
    tempr = open(output_dir + '/temp_r.R', 'w')
    tempr.writelines([i.rstrip() + '\n' for i in content_r.split('\r\n')])
    tempr.close()


def create_r(outputdir, dis_matrix, maping):
    """
    这是不完整的db_rda的R方法！！！

    生成可以运行的R脚本

    :param outputdir: 输出文件夹
    :param dis_matrix: 输入矩阵
    :param maping: 输入分组文件
    """
    tempr = open(outputdir + '/temp_r.R', 'w')
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
write.table(tempplot$sites, '%s/db_rda_sites.temp.txt', sep = '\\t', col.names = TRUE, quote = FALSE)
write.table(tempplot$biplot, '%s/db_rda_factor.temp.txt', sep = '\\t', quote = FALSE)
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
        os.system('%s/R-3.2.2/bin/Rscript %s' % (Config().SOFTWARE_DIR, script))
    else:
        pass
    if delscript:
        os.remove(script)
        if os.path.exists(os.path.dirname(script) + '/temp_r.Rout'):
            os.remove(os.path.dirname(script) + '/temp_r.Rout')


def db_rda(dis_matrix, maping, outputdir):
    """
    输入距离矩阵，分组信息，输出文件夹，进行db_rda分析
    :pararm return: 成功完成返回‘0’
    """
    create_r(outputdir, dis_matrix, maping)
    script = outputdir + '/temp_r.R'
    run_r_script(script)
    return 0


def db_rda_new(otu_file, env_file, output_dir, distance_algorithm='bray'):
    """
    """
    create_r_new(otu_file, env_file, output_dir, distance_algorithm)
    script = output_dir + '/temp_r.R'
    run_r_script(script)
    return 0
