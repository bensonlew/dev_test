#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "qiuping"

import os


def two_group_test(inputfile, groupfile,outputfile,choose_test, ci="0.95", test_type="two.side", mul_test="none"):
    """
    两组样品的差异性分析，包括student T检验，welch T检验，wilcox秩和检验
    """
    t_test = '''
#读取otu_table，整理成首行为样品名，首列为注释信息的数据框
library(qvalue)
otu_data <- read.table("''' + inputfile + '''",sep = "\\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
#读取分组文件，获取要比较的分组的样品
group <- read.table("''' + groupfile + '''",sep="\\t")
group <- group[-1,]
gsamp <- group[,1]
g1 <- group[1,2]
g2 <- group[which(!group[,2] %in% g1),2]
g2 <- g2[1]
gsamp1=group[which(group[,2] %in% g1),1]
gsamp2=group[which(group[,2] %in% g2),1]
otu_data <- otu_data[,which(samp %in% gsamp)]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]
#对数据均一化
da <- otu_data
otu_data <-apply(da,2,function(x) as.numeric(x)/sum(as.numeric(x))) 
rownames(otu_data)<-rownames(da)
samp <- samp[which(samp %in% gsamp)]
result <- matrix(nrow = nrow(otu_data),ncol = 5)
pvalue <- 1
for(i in 1:nrow(otu_data)){
  o1 <- as.numeric(as.vector(unlist(otu_data[i,which(samp %in% gsamp1)])))
  o2 <- as.numeric(as.vector(unlist(otu_data[i,which(samp %in% gsamp2)])))
  me1 <- mean(o1)
  me2 <- mean(o2)
  sd1 <- sd(o1)
  sd2 <- sd(o2)
  test <- "''' + choose_test + '''"
  if(test == "student"){
    tt <- t.test(o1,o2,var.equal = TRUE,alternative = "''' + test_type + '''",conf.level = ''' + ci + ''')
  }else if(test == "welch"){
    tt <- t.test(o1,o2,var.equal = FALSE,alternative = "''' + test_type + '''",conf.level = ''' + ci + ''')
  }else{
    tt <- wilcox.test(o1,o2,alternative = "''' + test_type + '''",exact = F,conf.level = "''' + ci + '''")
  }
  pvalue <- c(pvalue,tt$p.value)
  result[i,] = c(rownames(otu_data)[i],me1,sd1,me2,sd2)
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "''' + mul_test + '''")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,6]),lambda = 0.5)
result <- cbind(result,qv$qvalue)
colnames(result) <- c(" ",paste("mean(",g1,")",sep=''),paste("sd(",g1,")",sep=''),paste("mean(",g2,")",sep=''),paste("sd(",g2,")",sep=''),"p-value","q-value")
result_order <- result[order(result[,6]),]  
write.table(result_order,"''' + outputfile + '''",sep="\\t",col.names=T,row.names=F)   
    '''


    r_file = open("two_group_test.r", 'w+')
    r_file.write(t_test)
    r_file.close()
    os.system("two_group_test")
    return 0


def two_sample_test(inputfile,outputfile,choose_test, sample1, sample2, ci="0.95", test_type="two.side", mul_test="none"):
    """
    两样品的卡方检验或者费舍尔检验，choose_test参数为选择做哪种检验的参数，分别为chi_square_test、fisher_exact_test
    """
    two_sample_test = '''
#读取otu_table，整理成首行为样品名，首列为注释信息的数据框
library(qvalue)
otu_data <- read.table("''' + inputfile + '''",sep = "\\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
s1 <- "''' + sample1 + '''"
s2 <- "''' + sample2 + '''"
otu_data <- otu_data[,which(samp %in% c(s1,s2))]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]

result <- matrix(nrow = nrow(otu_data),ncol = 3)
pvalue <- 1
for(i in 1:nrow(otu_data)){
  c1 <- as.numeric(as.vector(otu_data[i,1]))
  c2 <- sum(as.numeric(as.vector(otu_data$'''+sample1+'''))) - c1
  c3 <- as.numeric(as.vector(otu_data[i,2]))
  c4 <- sum(as.numeric(as.vector(otu_data$'''+sample2+'''))) - c3
  data <- matrix(c(c1,c2,c3,c4),ncol = 2)
  test <- "''' + choose_test + '''"
  if (test == "chi_square_test") {
    tt <- chisq.test(data)
  }else{
    tt <- fisher.test(data,alternative = "''' + test_type + '''",conf.level = ''' + ci + ''')
  }
  pvalue <- c(pvalue,tt$p.value)
  pro1 <- c1 / sum(as.numeric(as.vector(otu_data$'''+sample1+''')))
  pro2 <- c3 / sum(as.numeric(as.vector(otu_data$'''+sample2+''')))
  result[i,] = c(rownames(otu_data)[i],pro1,pro2)
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "''' + mul_test + '''")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,4]),lambda = 0.5)  
result <- cbind(result,qv$qvalue)
colnames(result) <- c(" ",paste("propotion(",s1,")",sep=''),paste("propotion(",s2,")",sep=''),"p-value","q-value")
result_order <- result[order(result[,4]),]  
write.table(result_order,"''' + outputfile + '''",sep="\\t",col.names=T,row.names=F)
    '''
    r_file = open("two_sample_test.r", 'w')
    r_file.write("%s" % two_sample_test)
    r_file.close()
    os.system("two_sample_test.r")
    return 0


def mul_group_test(inputfile, outputfile, groupfile,choose_test, mul_test="none"):
    """
    多组样本的差异性检验，包括：克鲁斯卡尔-Wallis秩和检验、anova分析
    """
    mul_group_test = '''
#读取otu_table，整理成首行为样品名，首列为注释信息的数据框
library(qvalue)
otu_data <- read.table("''' + inputfile + '''",sep = "\\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
#读取分组文件，获取要比较的分组的样品
group <- read.table("''' + groupfile + '''",sep="\\t")
gsamp=group[-1,1]
otu_data <- otu_data[,which(samp %in% gsamp)]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]
#对数据均一化
da <- otu_data
otu_data <-apply(da,2,function(x) as.numeric(x)/sum(as.numeric(x))) 
rownames(otu_data)<-rownames(da)

samp <- samp[which(samp %in% gsamp)]
test_data <- t(otu_data)
test_data <- as.data.frame(test_data)
test_data$group <- "" 
for(i in 1:nrow(test_data)){
  test_data[i,ncol(test_data)] <- as.character(group[which(group[,1] %in% rownames(test_data)[i]),2])
}
test_data$group <- as.factor(test_data$group)
#计算记录结果的数据表中的列数
colnum <- nlevels(test_data$group)
colnum <- colnum*2 + 1
result <- matrix(nrow = nrow(otu_data),ncol = colnum)
result <- as.data.frame(result)
pvalue <- 1
for(i in 2:(ncol(test_data)-1)){
  colnames(test_data)[i] <- "otu"
  test_data$otu <- as.numeric(as.vector(test_data$otu))
  test <- "''' + choose_test + '''"
  if (test == "kru_H"){
    tt <- kruskal.test(otu ~ group, data = test_data)
  }else{
    tt <- oneway.test(otu ~ group, data = test_data)
  }  
  pvalue <- c(pvalue,tt$p.value)
  
  #求每个组别的均值、标准差
  s <- split(test_data,test_data$group)
  Me <- lapply(s,function(x)mean(x[,c("otu")]))
  Sd <- lapply(s,function(x)sd(x[,c("otu")]))
  result[i,1] <- rownames(otu_data)[i]
  n <- 2
  for(len in 1:length(Me)){
    result[i,n] <- Me[[len]]
    result[i,n+1] <- Sd[[len]]
    n <- n + 2
  }
  
  result[i,colnum] <- pvalue
  colnames(test_data)[i] <- rownames(otu_data)[i]
}
result[1,1] <- " "
coln <- 2
s_name = names(s)
for(l in 1:(length(s_name))){
  result[1,coln] <- paste("mean(",s_name[l],")",sep='')
  result[1,coln+1] <- paste("sd(",s_name[l],")",sep='')
  coln <- coln+2
}
head <- t(result[1,])
result <- result[-1,]
colnames(result) <- head
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "''' + mul_test + '''")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,colnum+1]),lambda = 0.5)
qvalue <- qv$qvalue
result <- cbind(result,qvalue)
result_order <- result[order(result[,colnum+1]),]
write.table(result_order,"''' + outputfile + '''",sep="\\t",col.names=T,row.names=F)

    '''
    r_file = open("mul_group_test.r", 'w')
    r_file.write(mul_group_test)
    r_file.close()
    os.system("mul_group_test.r")
    return 0


        

