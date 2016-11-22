

input_matrix<-'${input_matrix}'
feature <- '${feature}'
meas<-'${meas}'
group<-'${group}'
 #### both row column none


library(ballgown)



a<-read.delim(group)
t<-length(a[,1])
f<-c()
for (i in c(1:t)){
if (a[i,2]=="A"){
f[i]<-0
}
else{f[i]<-1}
}

groupst<-f


bg = ballgown(dataDir=input_matrix, samplePattern='sample', meas='all')

if(file.exists("expr")==0){
dir.create("expr")
}

bgexon <- structure(bg)$exon
bgintron<- structure(bg)$intron
bgtrans<-structure(bg)$trans
write.table(bgexon,"expr/exon.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(bgintron,"expr/intron.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(bgtrans,"expr/tran.xls",sep="\t",col.names=T,row.names=T,quote=F)

pData(bg) = data.frame(id=sampleNames(bg), group=rep(f))

stat_results = stattest(bg, feature="gene", meas=meas, covariate='group',getFC = TRUE)

names(stat_results)[2:3]<-c("seq_id","log2FC(DeS2/DeS1)")
names(stat_results)[5]<-c("FDR")

stat_results<-data.frame(stat_results[,2],stat_results[,3],stat_results[,5])
names(stat_results)[1:3]<-c("seq_id","log2FC(DeS2/DeS1)","FDR")
write.table(stat_results,"expr/diff_expr.xls",sep="\t",col.names=T,row.names=F,quote=F)




##feature = c("gene", "exon", "intron", "transcript"), 
##meas = c("cov", "FPKM", "rcount", "ucount", "mrcount", "mcov")






