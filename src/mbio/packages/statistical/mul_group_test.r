#  last modified: 2016.01.25
#  Author: qiuping
#  This script is designes to identify differentially abundant features between multiple groups
library(boot)
library(qvalue)
library(stats)

#  a function to summary the values for boxplot, including min,Q1,median,Q3,max
boxplot_stat <- function(x){
  colnum <- nlevels(x$group)*5
  box_result <- matrix(nrow = ncol(x)-1,ncol = colnum)
  box_result <- as.data.frame(box_result)
  gname <- levels(x$group)
  for(i in 1:(ncol(x)-1)){
    cl = 1
    for(n in gname){
      sum <- as.numeric(summary(x[which(x$group %in% n),i]))[-4]
      for(l in 1:length(sum)){
        box_result[i,cl] <- sum[l]
        cl = cl + 1
      }
    }
  }
  head <- "1"
  for(n in gname){
    cname <- c(paste('min(',n,')',sep = ''),paste('Q1(',n,')',sep = ''),paste('Median(',n,')',sep = ''),paste('Q3(',n,')',sep = ''),paste('max(',n,')',sep = ''))
    head <- c(head,cname)
  }
  head <- head[-1]
  rownames(box_result) <- colnames(x)[-length(x)]
  colnames(box_result) <- head
  return (box_result)
}

#  a function to summary mean and sd of every group, x is a data frame
summary_stat <- function(x){
  #  write the result to a data frame
  colnum <- nlevels(x$group)*2
  stat_result <- matrix(nrow = ncol(x),ncol = colnum)
  stat_result <- as.data.frame(stat_result)
  s <- split(x,x$group)
  len <- ncol(s[[1]])-1
  for(i in 1:len){
    Me <- lapply(s,function(x)mean(as.numeric(as.vector(x[,i]))))
    Sd <- lapply(s,function(x)sd(as.numeric(as.vector(x[,i]))))
    n <- 1
    for(l in 1:length(Me)){
      stat_result[i+1,n] <- Me[[l]]
      stat_result[i+1,n+1] <- Sd[[l]]
      n <- n+2
    }
  }
  #  make the head of stat_result 
  coln <- 1
  s_name <- names(s)
  for(m in 1:(length(s_name))){
    stat_result[1,coln] <- paste("mean(",s_name[m],")",sep='')
    stat_result[1,coln+1] <- paste("sd(",s_name[m],")",sep='')
    coln <- coln+2
  }
  rownames(stat_result) <- c(" ",colnames(s[[1]])[-length(colnames(s[[1]]))])
  head <- stat_result[1,]
  stat_result <- stat_result[-1,]
  colnames(stat_result) <- head
  return (stat_result)
}

#  deal with the input file, come into being a data frame of R
data <- read.table('${inputfile}',sep = '\t',comment.char = '')
samp <- t(data[1,-1])
data <- data[-1,]
rownames(data) <- data[,1]
data <- data[,-1]
colnames(data) <- samp

group <- read.table('${groupfile}',sep = '\t',comment.char = '')
gsamp <- group[-1,1]
data <- data[,which(samp %in% gsamp)]
data <- data[apply(data,1,function(x)any(x>0)),]
da <- data
data <- apply(da,2,function(x)as.numeric(x)/sum(as.numeric(x)))
rownames(data) <- rownames(da)
data <- t(data)
data <- as.data.frame(data)
samp <- samp[which(samp %in% gsamp)]
data$group <- ""
for(i in 1:nrow(data)){
  data[i,ncol(data)] <- as.character(group[which(group[,1] %in% rownames(data)[i]),2])
}
data$group <- as.factor(data$group)

#  to do kruskal wallis H test or one way anova test
result <- summary_stat(data)
test <- '${choose_test}'
pvalue <- 1
for(i in 1:(ncol(data)-1)){
  if(test == 'kru_H'){
    st <- kruskal.test(data[[i]]~data$group)
  }else{
    st <- oneway.test(data[[i]]~data$group)
  }
  pvalue <- c(pvalue,st$p.value)
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = '${mul_test}')
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(pvalue),lambda = 0.5)
qvalue <- qv$qvalue
result <- cbind(result,qvalue)
result_order <- result[order(result$pvalue),]
write.table(result_order,"${outputfile}",sep="\t",col.names=T,row.names=T)

boxfile <- boxplot_stat(data)
write.table(boxfile,"${boxfile}",sep="\t",col.names=T,row.names=T)

