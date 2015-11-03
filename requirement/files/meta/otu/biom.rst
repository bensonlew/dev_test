
格式说明
==========================

Path
-----------

**meta.otu.biom**


功能和用途描述
-----------------------------------

用于记录样本的观测信息

格式定义文档
-----------------------------------

biom : http://biom-format.org/documentation/format_versions/biom-2.1.html

格式举例
-----------------------------------

::
	{"id": "None","format": "Biological Observation Matrix 1.0.0","format_url": "http://biom-format.org","generated_by": "QIIME 1.9.1","date": "2015-09-08T14:44:40.677319","matrix_element_type": "float","shape": [7, 27],"type": "OTU table","matrix_type": "sparse","data": [[0,0,6.0],[0,1,1.0],[0,2,2.0],[0,3,5.0],[0,4,1.0],[0,5,1.0],[0,6,1.0],[1,0,4.0],[1,1,2.0],[1,3,4.0],[1,4,1.0],[1,7,4.0],[1,8,1.0],[1,9,2.0],[1,10,3.0],[1,11,1.0],[1,12,1.0],[1,13,2.0],[2,14,1.0],[2,15,1.0],[3,2,2.0],[3,6,3.0],[3,11,1.0],[3,12,2.0],[3,14,3.0],[3,16,1.0],[3,17,2.0],[3,18,1.0],[4,16,1.0],[4,17,3.0],[4,19,3.0],[4,20,8.0],[4,21,1.0],[4,22,5.0],[4,23,5.0],[4,24,4.0],[4,25,1.0],[5,0,1.0],[5,1,1.0],[5,9,2.0],[5,17,1.0],[6,9,1.0],[6,12,1.0],[6,26,1.0]],"rows": [{"id": "OTU1", "metadata": null},{"id": "OTU2", "metadata": null},{"id": "OTU3", "metadata": null},{"id": "OTU4", "metadata": null},{"id": "OTU5", "metadata": null},{"id": "OTU6", "metadata": null},{"id": "OTU7", "metadata": null}],"columns": [{"id": "N21", "metadata": null},{"id": "N29", "metadata": null},{"id": "N24", "metadata": null},{"id": "N14", "metadata": null},{"id": "N6", "metadata": null},{"id": "N23", "metadata": null},{"id": "N3", "metadata": null},{"id": "N19", "metadata": null},{"id": "N12", "metadata": null},{"id": "N7", "metadata": null},{"id": "N27", "metadata": null},{"id": "N22", "metadata": null},{"id": "N17", "metadata": null},{"id": "N18", "metadata": null},{"id": "N25", "metadata": null},{"id": "N28", "metadata": null},{"id": "N10", "metadata": null},{"id": "N26", "metadata": null},{"id": "N8", "metadata": null},{"id": "N15", "metadata": null},{"id": "N2", "metadata": null},{"id": "N11", "metadata": null},{"id": "N4", "metadata": null},{"id": "N9", "metadata": null},{"id": "N20", "metadata": null},{"id": "N13", "metadata": null},{"id": "N1", "metadata": null}]}

                                                                 
属性及其含义
-----------------------------------
* ``otu_num`` 	 otu数量
* ``sample_num``  样本数量
* ``metadata``    meta数据信息


相关方法
-----------------------------------

``convert_to_otu_table`` 将这个biom格式转化为同名的otu_table

