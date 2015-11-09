#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "qiuping"

import os
import argparse


def twogroup_input(inputfile,groupfile,group1,group2) :
    """
    针对两组样品比较的差异分析，对otu_table和分组文件进行数据处理
    """
    otu_data = """
#读取otu_table，整理成首行为样品名，首列为注释信息的数据框
otu_data <- read.table("'''+inputfile+'''",seq = "\\t")
samp <- t(otu_data[1,-1])
head = as.character(otu[1,1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
#读取分组文件，获取要比较的分组的样品
group <- group=read.table("'''+groupfile+'''",sep="\\t")
g1 <- "'''+group1+'''" 
g2 <- "'''+group2+'''" 
gsamp <- group[which(group[,2] %in% c(g1,g2)),1]
gsamp1 <- group[which(group[,2] %in% g1),1]
gsamp2 <- group[which(group[,2] %in% g2),1]
otu_data <- otu_data[,which(samp %in% gsamp)]
samp <- samp[which(samp %in% gsamp)]

    """

def outputfile(outputfile):
    """
    生成结果数据表
    """
    outresult = """
result <- cbind(result,qv$qvalue)
colnames(result) <- c(" ",paste("mean(",g1,")",sep=''),paste("sd(",g1,")",sep=''),paste("mean(",g2,")",sep=''),paste("sd(",g2,")",sep=''),"p-value","q-value")
result_order <- result[order(result[,6]),]  
write.table(result_order,"'''+outputfile+'''",sep="\\t",col.names=T,row.names=F)
    """

def student_t_test(ci = 0.95,type = "two.side",mul_test = "none"):
    """
    两组样品的T检验
    """
    student_t_test = """
result <- matrix(nrow = nrow(otu_data),ncol = 6)
for(i in 1:nrow(otu_data)){
  o1 <- as.numeric(as.verctor(unlist(otu_data[i,which(samp %in% gsamp1)])))
  o2 <- as.numeric(as.verctor(unlist(otu_data[i,which(samp %in% gsamp2)])))
  me1 <- mean(o1)
  me2 <- mean(o2)
  sd1 <- sd(o1)
  sd2 <- sd(o2)
  tt <- t.test(o1,o2,var.equal = TRUE,alternative = "'''+type+'''",conf.level = "'''+ci+'''")
  pvalue <- p.adjust(as.numeric(as.verctor(tt$p.value)),method = "'''+mul_test+'''")
  result[i,] = c(rownames(otu_data)[i],me1,sd1,me2,sd2,pvalue)
}
qv <- qvalue(as.numeric(result[,6]),lambda = 0.5)  
    """
    r_file = open("student_t_test.r",w)
    twogroup_input(inputfile,groupfile,group1,group2)
    r_file.write(otu_data"\n")
    r_file.write(student_t_test"\n")
    r_file.write(outresult)
    os.system("student_t_test.r")

def welch_t_test(ci = 0.95,type = "two.side",mul_test = "none"):
    """
    两组样品的T检验
    """
    welch_t_test = """
result <- matrix(nrow = nrow(otu_data),ncol = 6)
for(i in 1:nrow(otu_data)){
  o1 <- as.numeric(as.verctor(unlist(otu_data[i,which(samp %in% gsamp1)])))
  o2 <- as.numeric(as.verctor(unlist(otu_data[i,which(samp %in% gsamp2)])))
  me1 <- mean(o1)
  me2 <- mean(o2)
  sd1 <- sd(o1)
  sd2 <- sd(o2)
  tt <- t.test(o1,o2,var.equal = FALSE,alternative = "'''+type+'''",conf.level = "'''+ci+'''")
  pvalue <- p.adjust(as.numeric(as.verctor(tt$p.value)),method = "'''+mul_test+'''")
  result[i,] = c(rownames(otu_data)[i],me1,sd1,me2,sd2,pvalue)
}
qv <- qvalue(as.numeric(result[,6]),lambda = 0.5)  
    """
    r_file = open("welch_t_test.r",w)
    twogroup_input(inputfile,groupfile,group1,group2)
    r_file.write(otu_data"\n")
    r_file.write(welch_t_test"\n")
    r_file.write(outresult)
    os.system("welch_t_test.r")

def mann_U_test(ci = 0.95,type = "two.side",mul_test = "none"):
    """
    两组样品的秩和检验
    """
    mann_U_test = """





    """

    


