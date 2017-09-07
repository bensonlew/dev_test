merge_fastq
==========================

模块Path
-----------

**tools.paternity_test.merge_fastq**

功能描述
-----------------------------------

医学流程数据拆分后四条lane合并


主要命令及功能模块
-----------------------------------

```
os.system('zcat {} {} {} {} >> {}'.format(r1_path[0], r1_path[1], r1_path[2], r1_path[3], self.r1_path))
os.system('gzip {}'.format(self.r1_path))
```

参数设计
-----------------------------------

```
{"name": "sample_dir_name", "type": "string"}, # 被合并的样本的文件夹
{"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"}, # 拆分后获得的文件夹MED
{"name": "result_dir", "type": "string"}, # 合并后文件被存放的位置
{"name": "ws_single", "type": "string", "default": "false"}, # 是否为单端样本
```

运行逻辑
-----------------------------------

根据输入的参数sample_dir_name和data_dir找到需要被合并的样本，合并后将样本link到结果文件中result_dir


资源配置
-----------------------------------

```
self._cpu = 5
self._memory = '10G'
```