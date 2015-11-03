
工具说明
==========================

Path
-----------

**metastat.metastat**

功能和用途描述
-----------------------------------

用于调用具体的差异性分析的tool

主要命令及功能模块
-----------------------------------

metastat_package




参数设计
-----------------------------------

::
            {"name": "chi_input", "type": "infile", "format": "otuTable"},  # 卡方检验的输入文件
            {"name": "chi_pvalue_filter", "type": "float", "default": "0.05"},  # 卡方检验的显著性水平
            {"name": "chi_sample", "type": "string"},  # 卡方检验的输入样品名称
            {"name": "chi_correction", "type": "string", "default": No correction},  # 卡方检验的多重检验校正
            {"name": "chi_type", "type": "string", "default":two_side},  # 卡方检验的选择单尾或双尾检验
            {"name": "chi_CI1", "type": "string", "default": DP: Asymptotic },  # 卡方检验的CI参数
            {"name": "chi_CI2", "type": "float", "default": 0.95},  # 卡方检验的CI参数
            {"name": "chi_output", "type": "outfile", "format": "metastat"}  # 卡方检验的输出结果
            {"name": "fisher_input", "type": "infile", "format": "otuTable"},  # 费舍尔检验的输入文件
            {"name": "fisher_pvalue_filter", "type": "float", "default": "0.05"},  # 费舍尔检验的显著性水平
            {"name": "fisher_sample", "type": "string"},  # 费舍尔检验的输入样品名称
            {"name": "fisher_correction", "type": "string", "default": No correction},  # 费舍尔检验的多重检验校正
            {"name": "fisher_type", "type": "string", "default":two_side},  # 费舍尔检验的选择单尾或双尾检验
            {"name": "fisher_CI1", "type": "string", "default": DP: Asymptotic },  # 费舍尔检验的CI参数
            {"name": "fisher_CI2", "type": "float", "default": 0.95},  # 费舍尔检验的CI参数
            {"name": "fisher_output", "type": "outfile", "format": "metastat"}  # 费舍尔检验的输出结果
            {"name": "kruskal_input", "type": "infile", "format": "otuTable"},  # kruskal_wallis_H_test的输入文件
            {"name": "kruskal_pvalue_filter", "type": "float", "default": "0.05"},  # kruskal_wallis_H_test的显著性水平
            {"name": "kruskal_group", "type": "infile", "format": "groupfile"},  # kruskal_wallis_H_test的输入分组文件
            {"name": "kruskal_correction", "type": "string", "default": No correction},  # kruskal_wallis_H_test的多重检验校正
            {"name": "kruskal_post", "type": "string", "default":Tukey-Kranmer },  # kruskal_wallis_H_test的Pos-hoc test参数
            {"name": "kruskal_hoc", "type": "float", "default":0.95 },  # kruskal_wallis_H_test的Pos-hoc test参数
            {"name": "kruskal_output", "type": "outfile", "format": "metastat"}  # kruskal_wallis_H_test的输出结果
            {"name": "lefse_input", "type": "infile", "format": "otuTable"},  # lefse分析的输入文件
            {"name": "lefse_group", "type": "infile", "format": "groupfile"},  # lefse分析的输入分组文件
            {"name": "lefse_LDA", "type": "outfile", "format": "pdf"}  # lefse分析的输出结果
            {"name": "lefse_clado", "type": "outfile", "format": "pdf"}  # lefse分析的输出结果
            {"name": "lefse_xls", "type": "outfile", "format": "lefse"}  # lefse分析的输出结果
            {"name": "mann_input", "type": "infile", "format": "otuTable"},  # 秩和检验的输入文件
            {"name": "mann_pvalue_filter", "type": "float", "default": "0.05"},  # 秩和检验的显著性水平
            {"name": "mann_group", "type": "infile", "format": "groupfile"},  # 秩和检验的输入分组文件
            {"name": "mann_correction", "type": "string", "default": No correction},  # 秩和检验的多重检验校正
            {"name": "mann_type", "type": "string", "default":two_side},  # 秩和检验的选择单尾或双尾检验
            {"name": "mann_output", "type": "outfile", "format": "metastat"}  # 秩和检验的输出结果
            {"name": "non_parametric_input", "type": "infile", "format": "otuTable"},  # 非参T检验的输入文件
            {"name": "non_parametric_pvalue_filter", "type": "float", "default": "0.05"},  # 非参T检验的显著性水平
            {"name": "non_parametric_group", "type": "infile", "format": "groupfile"},  # 非参T检验的输入分组文件
            {"name": "non_parametric_correction", "type": "string", "default": No correction},  # 非参T检验的多重检验校正
            {"name": "non_parametric_type", "type": "string", "default":two_side},  # 非参T检验的选择单尾或双尾检验
            {"name": "non_parametric_output", "type": "outfile", "format": "metastat"}  # 非参T检验的输出结果
            {"name": "student_input", "type": "infile", "format": "otuTable"},  # T检验的输入文件
            {"name": "student_pvalue_filter", "type": "float", "default": "0.05"},  # T检验的显著性水平
            {"name": "student_group", "type": "infile", "format": "groupfile"},  # T检验的输入分组文件
            {"name": "student_correction", "type": "string", "default": No correction},  # T检验的多重检验校正
            {"name": "student_type", "type": "string", "default":two_side},  # T检验的选择单尾或双尾检验
            {"name": "student_output", "type": "outfile", "format": "metastat"}  # T检验的输出结果
            {"name": "welch_input", "type": "infile", "format": "otuTable"},  # welch_T检验的输入文件
            {"name": "welch_pvalue_filter", "type": "float", "default": "0.05"},  # welch_T检验的显著性水平
            {"name": "welch_group", "type": "infile", "format": "groupfile"},  # welch_T检验的输入分组文件
            {"name": "welch_correction", "type": "string", "default": No correction},  # welch_T检验的多重检验校正
            {"name": "welch_type", "type": "string", "default":two_side},  # welch_T检验的选择单尾或双尾检验
            {"name": "welch_output", "type": "outfile", "format": "metastat"}  # welch_T检验的输出结果
            {"name": "anova_input", "type": "infile", "format": "otuTable"},  # anova分析的输入文件
            {"name": "anova_pvalue_filter", "type": "float", "default": "0.05"},  # anova分析的显著性水平
            {"name": "anova_group", "type": "infile", "format": "groupfile"},  # anova分析的输入分组文件
            {"name": "anova_correction", "type": "string", "default": No correction},  # anova分析的多重检验校正
            {"name": "anova_post", "type": "string", "default":Tukey-Kranmer },  # anova分析的Pos-hoc test参数
            {"name": "anova_hoc", "type": "float", "default":0.95 },  # anova分析的Pos-hoc test参数
            {"name": "anova_output", "type": "outfile", "format": "metastat"}  # anova分析的输出结果




运行逻辑
-----------------------------------

当传入相应的tool相应的参数时，可进行相应的差异性分析tool，并获得该分析结果。