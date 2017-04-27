# -*- coding: utf-8 -*-
# __author__ = konghualei, 20170418

# fpkm = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/oldrsem/genes.TMM.fpkm.matrix"
# outfile_path = "/mnt/ilustre/users/sanger-dev/sg-users/konghualei/ref_rna/tofiles"
# fpkm = "/mnt/ilustre/users/sanger-dev/workspace/20170426/Single_geneset_venn_1/GenesetVenn/output/venn_graph.xls"
fpkm="${fpkm}"
outfile_path = "${outfile_path}"

library(gplots)
data = read.delim(fpkm, sep="\t",header=F,as.is=T)
# samples = colnames(data)

# gene_name = rownames(data)
data_venn = list()
samples=data[,1]

# venn_graph=matrix(ncol=2)
for(s in seq(dim(data)[1])){
    # gene_name_id = gene_name[which(data[,s]!=0)]
    # total_gene_id = paste(gene_name_id,collapse=",")
    # total_data=matrix(c(samples[s],total_gene_id),nrow=1)
    # venn_graph=rbind(venn_graph,total_data)
    gene_name_id = strsplit(data[s,2],",")[[1]]
    data_venn[[samples[s]]] = gene_name_id
}
# venn_graph=venn_graph[-1,]
# write.table(venn_graph, paste(outfile_path,"/venn_graph.xls",collapse="",sep=""), sep="\t",row.names=F,col.names=F,quote=F)
# print("venn_graph.xls files！")

venn_data=venn(data_venn, show.plot=F)
venn_info = attr(venn_data,"intersections")
venn_names = names(venn_info)
venn_table = matrix(ncol=3)

for(i in venn_names){
    if(regexpr(":",i) != -1){
        i_ = strsplit(i,":")[[1]]
        new_i = paste(i_,collapse="&")
    }else{
        new_i = i
    }                                       #change intersections 
    tmp = venn_info[[i]] 
    if(length(tmp)>1){
        tmp_name = paste(tmp,collapse=",")  #gene_id information
    }else{
        tmp_name = tmp
    }
    print(length(tmp))
    new_tmp = matrix(c(new_i,length(tmp),tmp_name),nrow=1)
    venn_table=rbind(venn_table, new_tmp)
}
venn_table = venn_table[-1,]
write.table(venn_table, paste(outfile_path,"/venn_table.xls",collapse="",sep=""), sep="\t",row.names=F,col.names=F,quote=F)
print("venn_table.xls files！")


