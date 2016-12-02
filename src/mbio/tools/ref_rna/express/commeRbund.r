

input_matrix<-'${input_matrix}'

library(cummeRbund)

cuff<-readCufflinks(input_matrix)

gene.fpkm<-fpkm(genes(cuff))
gene.counts<-count(genes(cuff))
isoform.fpkm<-fpkm(isoforms(cuff))
gene.diff<-diffData(genes(cuff))
dir.create("expr")

write.table(gene.fpkm,"expr/gene_fpkm.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(gene.counts,"expr/gene_count.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(isofrom.fpkm,"expr/isoform_fpkm.xls",sep="\t",col.names=T,row.names=T,quote=F)
write.table(gene.diff,"expr/diff_expr.xls",sep="\t",col.names=T,row.names=T,quote=F)







