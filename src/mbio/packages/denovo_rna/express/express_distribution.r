# __author__ = "qiuping"
# last_modify:20170110

fpkm = read.table("${input_matrix}", header=TRUE, check.names=0, row.names=1 )
fpkm = log(fpkm)/log(2)
maxfpkm = 20
samples=colnames(fpkm)
dens=c()
for(i in 1:length(samples)){
    a=fpkm[i]
    den=density(a[,1],n=601,from=-10,to=maxfpkm)
    dens=cbind(dens,den$y)
}
dens_new<-cbind(den$x,dens)
samples=c('log2fpkm',samples)
colnames(dens_new)=samples
write.table(dens_new, file="${outputfile}", sep='\t', quote=F, row.names=F, col.names=T)
