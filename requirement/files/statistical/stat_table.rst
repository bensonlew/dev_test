
格式说明
==========================

Path
-----------

**metastat.metastat**


功能和用途描述
-----------------------------------

用于存储差异性分析模块的结果文件信息


格式定义文档
-----------------------------------

第一行为分组名（两个样品检验时为样品名），第一列为某一水平上的物种名称，第二行为每个组别（两个样品检验时为样品）对应的mean、sd、variance以及p-value、q-value。其他行列为对应的数值。


格式举例
-----------------------------------

::

            多组分析：
                Enterotype1         Enterotype2         Enterotype3             
            phylum. mean1   sd1 variance1   mean2   sd2 variance2   mean3   sd3 variance3   p-value qvalue
            Acidobacteria   0.000205615 0.000144248 0.000198656 0.000348533 0.000198656 0.000144248 0.000348533 0.000198656 0.000144248 0.050222373 0.051511806
            Actinobacteria  0.036052463 0.037174538 0.003573378 0.014428233 0.003573378 0.037174538 0.014428233 0.003573378 0.037174538 0.001665215 0.004658096
            Apicomplexa 4.66E-06    1.30E-05    0   0   0   1.30E-05    0   0   1.30E-05    0.164394222 0.084307478

            两组分析：
                Enterotype1         Enterotype2             
            phylum. mean1   sd1 variance1   mean2   sd2 variance2   p-value qvalue
            Acidobacteria   0.000205615 0.000144248 0.000198656 0.000348533 0.000198656 0.000144248 0.050222373 0.051511806
            Actinobacteria  0.036052463 0.037174538 0.003573378 0.014428233 0.003573378 0.037174538 0.001665215 0.004658096
            Apicomplexa 4.66E-06    1.30E-05    0   0   0   1.30E-05    0.164394222 0.084307478
            Aquificae   0   0   1.95E-06    5.03E-07    1.95E-06    0   0.35064789  0.121230535

            两个样品：
                sample1         sample2             
            phylum. mean1   sd1 variance1   mean2   sd2 variance2   p-value qvalue
            Acidobacteria   0.000205615 0.000144248     0.000348533 0.000198656     0.050222373 0.051511806
            Actinobacteria  0.036052463 0.037174538     0.014428233 0.003573378     0.001665215 0.004658096
            Apicomplexa 4.66E-06    1.30E-05        0   0       0.164394222 0.084307478
            Aquificae   0   0       5.03E-07    1.95E-06        0.35064789  0.121230535


属性及其含义
-----------------------------------

* ``organism_number``    文件包含的物种的数量


相关方法
-----------------------------------

``get_test_result``  传入某一物种名字时，可以获取该物种名字在各分组（样品）的mean、sd、variance、pvalue、qvalue
``get_name``    获取文件中物种的名字
``feature_filter``  筛选一定范围的P值、q值的物种名字，返回值为物种名字以及对应的P值和q值
