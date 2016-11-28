
工具说明
==========================

Path
-----------

**tablemaker**

程序安装路径
-----------------------------------
/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/tablemaker-2.1.1


功能和用途描述
-----------------------------------

用于计算基因的表达量(count值)

使用程序
-----------------------------------

tablemaker：


主要命令及功能模块
-----------------------------------
# 调用cufflinks计算基因和转录本的表达量
tablemaker -p 15 -q -W -g *.gtf -o output *.bam

# 生成转录本的ctab格式表达量，通过ballgown进行差异表达分析
tablemaker -p 15 -q -g *.gtf -o output *.bam

参数设计
-----------------------------------
::
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "ref_rna.bam"},  # 样本比对后的bam文件,默认bam格式 "sam"
            {"name": "strand_specific", "type": "string", "default": "fr-unstranded"},  # PE测序，是否链特异性, 默认是无特异性 “None”
            {"name": "firststrand", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数  "fr-firststrand"
            {"name": "secondstrand", "type": "string", "default": "None"},  # 链特异性时选择负链, 默认不设置此参数  "fr-secondstrand"
            {"name": "express_ballgown", "type": "bool", "default": True},  # 是否生成ctab文件，传递给ballgown计算转录本差异表达分析差异表达分析
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  # 设置内存


运行逻辑
-----------------------------------
1. 调用cufflinks计算基因和转录本的表达量
2. 生成转录本的ctab格式表达量，通过ballgown进行差异表达分析
3. 需要指出是否链特异性 ``strand_specific``，若为链特异性，需要给出是正链或负链 `fr-firststrand`, `fr-secondstrand`：



