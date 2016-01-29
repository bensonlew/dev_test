library(qvalue)
otu_data <- read.table("${inputfile}",sep = "\t")
samp <- t(otu_data[1,-1])
otu_data <- otu_data[-1,]
rownames(otu_data) <- otu_data[,1]
otu_data <- otu_data[,-1]
colnames(otu_data) <- samp

group <- read.table("${groupfile}",sep="\t")
gsamp=group[,1]
otu_data <- otu_data[,which(samp %in% gsamp)]
otu_data <- otu_data[apply(otu_data,1,function(x)any(x>0)),]

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
colnum <- nlevels(test_data$group)
colnum <- colnum*2 + 1
result <- matrix(nrow = nrow(otu_data),ncol = colnum)
result <- as.data.frame(result)
pvalue <- 1
postlist <- list()
postdata <- data.frame()
for(i in 1:(ncol(test_data)-1)){
  colnames(test_data)[i] <- "otu"
  test_data$otu <- as.numeric(as.vector(test_data$otu))
  s <- split(test_data,test_data$group)
  Me <- lapply(s,function(x)mean(x[,c("otu")]))
  Sd <- lapply(s,function(x)sd(x[,c("otu")]))
  result[i+1,1] <- rownames(otu_data)[i]
  
  group_name <- names(s)
  com <- combn(group_name,2)
  
  test <- "${choose_test}"
  if (test == "kru_H"){
    tt <- kruskal.test(otu ~ group, data = test_data)
    for(cl in 1:ncol(com)){
      data1 <- s[[which(names(s) %in% com[,cl][1])]]
      data2 <- s[[which(names(s) %in% com[,cl][2])]]
      postdata[cl,1] <- paste(com[,cl][1],"-",com[,cl][2],sep = '')
      o1 <- as.vector(as.numeric(data1[,i]))
      o2 <- as.vector(as.numeric(data2[,i]))
      
      if(any(o1 != o2)){
        pi <- i
        pht <- wilcox.test(o1,o2,alternative = "${test_type}",exact = F,conf.level = 0.95, conf.int = TRUE)
        postdata[cl,2] <- as.vector(pht$conf.int)[1]
        postdata[cl,3] <- as.vector(pht$conf.int)[2]
        postdata[cl,4] <- as.vector(pht$p.value)
      }else{
        postdata[cl,2] <- NA
        postdata[cl,3] <- NA
        postdata[cl,4] <- NA
      }
    }
    
    
    post_rowname <- t(postdata[,1])
    postdata <- postdata[,-1]
    rownames(postdata) <- post_rowname
    colnames(postdata) <- c('lwr','upr','p.adj')
    postdata$p.adj <- p.adjust(as.vector(as.numeric(postdata$p.adj)),method = "${mul_test}")
    postlist$name <- postdata
    names(postlist)[i] <- rownames(otu_data)[i]
    
    
  }else{
    tt <- oneway.test(otu ~ group, data = test_data)
    av <- aov(otu ~ group, data = test_data)
    pht <- TukeyHSD(av)
    postlist$name <- pht$group[,-1]
    names(postlist)[i] <- rownames(otu_data)[i]
  }  
  pvalue <- c(pvalue,tt$p.value)
  n <- 2
  for(len in 1:length(Me)){
    result[i+1,n] <- Me[[len]]
    result[i+1,n+1] <- Sd[[len]]
    n <- n + 2
  }
  
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
pvalue <- p.adjust(as.numeric(pvalue),method = "BH")
result <- cbind(result,pvalue)
qv <- qvalue(as.numeric(result[,colnum+1]),lambda = 0.5)
qvalue <- qv$qvalue
result <- cbind(result,qvalue)
result_order <- result[order(result[,colnum+1]),]
write.table(result_order,"${outputfile}",sep="\t",col.names=T,row.names=F,quote = F)
write.table(postlist,"post_result",sep="\t",col.names=T,row.names=T,quote = F)


    