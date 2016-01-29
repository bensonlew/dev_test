
library(qvalue)
otu_data <- read.table("${inputfile}",sep = "\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
s1 <- "${sample1}"
s2 <- "${sample2}"
otu_data <- otu_data[,which(samp %in% c(s1,s2))]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]

result <- matrix(nrow = nrow(otu_data),ncol = 3)
pvalue <- 1
for(i in 1:nrow(otu_data)){
  c1 <- as.numeric(as.vector(otu_data[i,1]))
  c2 <- sum(as.numeric(as.vector(otu_data$"${sample1}"))) - c1
  c3 <- as.numeric(as.vector(otu_data[i,2]))
  c4 <- sum(as.numeric(as.vector(otu_data$"${sample2}"))) - c3
  data <- matrix(c(c1,c2,c3,c4),ncol = 2)
  test <- "${choose_test}"
  if (test == "chi_sq") {
    tt <- chisq.test(data)
  }else{
    tt <- fisher.test(data,alternative = "${test_type}",conf.level = ${ci})
  }
  pvalue <- c(pvalue,tt$p.value)
  pro1 <- c1 / sum(as.numeric(as.vector(otu_data$"${sample1}")))
  pro2 <- c3 / sum(as.numeric(as.vector(otu_data$"${sample2}")))
  result[i,] = c(rownames(otu_data)[i],pro1,pro2)
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "${mul_test}")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,4]),lambda = 0.5)  
result <- cbind(result,qv$qvalue)
colnames(result) <- c(" ",paste("propotion(",s1,")",sep=''),paste("propotion(",s2,")",sep=''),"p-value","q-value")
result_order <- result[order(result[,4]),]  
write.table(result_order,"${outputfile}",sep="\t",col.names=T,row.names=F,quote = F)
    