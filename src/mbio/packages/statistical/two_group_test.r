otu_data <- read.table("${inputfile}",sep = "\t",comment.char = '')
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp
#read groupfile to make the dataframe for test
group <- read.table("${groupfile}",sep="\t")
#group <- group[-1,]
gsamp <- group[,1]
g1 <- group[1,2]
g2 <- group[which(!group[,2] %in% g1),2]
g2 <- g2[1]
gsamp1=group[which(group[,2] %in% g1),1]
gsamp2=group[which(group[,2] %in% g2),1]
otu_data <- otu_data[,which(samp %in% gsamp)]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]
lendata <- nrow(otu_data)
if(lendata > 1){
  da <- otu_data
  otu_data <-apply(da,2,function(x) as.numeric(x)/sum(as.numeric(x)))
  rownames(otu_data)<-rownames(da)
}
samp <- samp[which(samp %in% gsamp)]
result <- matrix(nrow = nrow(otu_data),ncol = 5)
box_result <- matrix(nrow = nrow(otu_data),ncol = 11)
pvalue <- 1
for(i in 1:nrow(otu_data)){
  o1 <- as.numeric(as.vector(unlist(otu_data[i,which(samp %in% gsamp1)])))
  o2 <- as.numeric(as.vector(unlist(otu_data[i,which(samp %in% gsamp2)])))
  sum1 <- as.numeric(summary(o1))[-4]
  sum2 <- as.numeric(summary(o2))[-4]
  for(l in 1:length(sum1)){
    sum1[l] <- signif(sum1[l],4)
    sum2[l] <- signif(sum2[l],4)
  }
  me1 <- signif(mean(o1)*100,4)
  me2 <- signif(mean(o2)*100,4)
  sd1 <- signif(sd(o1)*100,4)
  sd2 <- signif(sd(o2)*100,4)
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
  box_result[i,] = c(rownames(otu_data)[i],sum1[1],sum1[2],sum1[3],sum1[4],sum1[5],sum2[1],sum2[2],sum2[3],sum2[4],sum2[5])
}
pvalue <- pvalue[-1]
pvalue <- p.adjust(as.numeric(pvalue),method = "none")
qv <- p.adjust(as.numeric(pvalue),method = "${mul_test}")
for(i in 1:length(pvalue)){
  pvalue[i] <- signif(pvalue[i],4)
  qv[i] <- signif(qv[i],4)
}
result <- cbind(result,pvalue)
result <- cbind(result,qv)
colnames(result) <- c(" ",paste(g1,"-mean",sep=''),paste(g1,"-sd",sep=''),paste(g2,"-mean",sep=''),paste(g2,"-sd",sep=''),"pvalue","corrected_pvalue")
result_order <- result[order(-(as.numeric(result[,2])+as.numeric(result[,4]))),]
if(lendata == 1){
  a <- data.frame(result_order)
  result_order <- t(a)
}
write.table(result_order,"${outputfile}",sep="\t",col.names=T,row.names=F,quote = F)
colnames(box_result) <- c(" ",paste("min(",g1,")",sep=''),paste("Q1(",g1,")",sep=''),paste("Median(",g1,")",sep=''),paste("Q3(",g1,")",sep=''),paste("max(",g1,")",sep=''),paste("min(",g2,")",sep=''),paste("Q1(",g2,")",sep=''),paste("Median(",g2,")",sep=''),paste("Q3(",g2,")",sep=''),paste("max(",g2,")",sep=''))
write.table(box_result,"${boxfile}",sep = '\t',col.names = T,row.names = F,quote = F)
