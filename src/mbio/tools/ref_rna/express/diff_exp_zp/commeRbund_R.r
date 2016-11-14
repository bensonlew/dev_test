

input_matrix<-'${input_matrix}'

library(cummeRbund)

cuff<-readCufflinks(input_matrix)

gene.fpkm<-fpkm(genes(cuff))
gene.counts<-count(genes(cuff))
isform.fpkm<-fpkm(isoforms(cuff))
gene.diff<-diffData(genes(cuff))

gene.diff.all<-data.frame(gene.diff[,1],gene.diff[,7],gene.diff[,10])

names(gene.diff.all)<-c("seq_id","log2FC(DeS2/DeS1)","FDR")


if(file.exists("expr")==0){
dir.create("expr")
}

write.table(gene.fpkm,"expr/gene_fpkm.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(gene.counts,"expr/gene_count.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(isform.fpkm,"expr/isform_fpkm.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(gene.diff,"expr/diff_expr.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(gene.diff.all,"expr/diff_expr_select.xls",sep="\t",col.names=T,row.names=F,quote=F)






