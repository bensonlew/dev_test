
library(qvalue)
otu_data <- read.table("${inputfile}",sep = "\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
group <- read.table("${groupfile}",sep="\t")
group <- group[-1,]
gsamp <- group[,1]
g1 <- group[1,2]
g2 <- group[which(!group[,2] %in% g1),2]
g2 <- g2[1]
gsamp1=group[which(group[,2] %in% g1),1]
gsamp2=group[which(group[,2] %in% g2),1]
otu_data <- otu_data[,which(samp %in% gsamp)]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]
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
  test <- "${choose_test}"
  if(test == "student"){
    tt <- t.test(o1,o2,var.equal = TRUE,alternative = "${test_type}",conf.level = ${ci})
  }else if(test == "welch"){
    tt <- t.test(o1,o2,var.equal = FALSE,alternative = "${test_type}",conf.level = ${ci})
  }else{
    tt <- wilcox.test(o1,o2,alternative = "${test_type}",exact = F,conf.level = ${ci})
  }
  pvalue <- c(pvalue,tt$p.value)
  result[i,] = c(rownames(otu_data)[i],me1,sd1,me2,sd2)
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "${mul_test}")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,6]),lambda = 0.5)
result <- cbind(result,qv$qvalue)
colnames(result) <- c(" ",paste("mean(",g1,")",sep=''),paste("sd(",g1,")",sep=''),paste("mean(",g2,")",sep=''),paste("sd(",g2,")",sep=''),"p-value","q-value")
result_order <- result[order(result[,6]),]  
write.table(result_order,"${outputfile}",sep="\t",col.names=T,row.names=F)
    