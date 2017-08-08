data_split
==========================

模块Path
-----------

**tools.paternity_test.data_split**

功能描述
-----------------------------------

医学流程的数据拆分

调用程序
-----------------------------------

bcl2fastq

安装路径
-----------------------------------

`/mnt/ilustre/users/sanger-dev/app/bioinfo/medical/bcl2fastq-2.17/bin/bcl2fastq`



主要命令及功能模块
-----------------------------------

```
index = self.option('data_dir').split(":")[0]
name = Config().get_netdata_config(index)
old_data_dir = name[index + "_path"] + "/" + self.option('data_dir').split(":")[1]


cmd = "{} -i {}Data/Intensities/BaseCalls/ -o {} --sample-sheet {} --use-bases-mask  y76,i6n,y76 --ignore-missing-bcl -R {} -r 4 -w 4 -d 2 -p 10 --barcode-mismatches 0".format(self.script_path,old_data_dir,self.output_dir,new_message_table, old_data_dir)
```

参数设计
-----------------------------------

```
{"name": "message_table", "type": "infile", "format": "paternity_test.tab"}, # 拆分信息表
{"name": "data_dir", "type": "string"},  # 下机文件夹名称
{"name": "ws_single", "type": "string"},  # 是否为单端的产筛样本
```

运行逻辑
-----------------------------------

根据输入的拆分表生成程序使用的拆分表，获取下机数据文件夹，调用bcl2fastq软件进行拆分


资源配置
-----------------------------------

```
self._cpu = 50
self._memory = '100G'
```

环境变量
-----------------------------------

```
self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/lib64')
self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/bin')
```