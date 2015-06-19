Cell_by_segmnetsSmooth<-function(df,saveFolder)
  for (seg in unique(df$segment)){
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = lc_qual))+ggtitle(paste("Segment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2<-power+geom_line(size=1.5,aes(linetype = NetworkGen))+facet_grid(NetworkType~.)
    ggsave(filename = paste(seg,".png"),plot = power2,path = saveFolder)
  }

######################################################################################
library(ggplot2)
###paths###
inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\Cells_smoothed_ref2-200.csv"
saveCellFolderSmooth <- "C:\\temp\\plots\\smoothed2"

df<-read.table(inputdfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
df$lc_qual<-paste(as.character(df$laccid),as.character(df$quality),sep=" : ")

Cell_by_segmnetsSmooth(df,saveCellFolderSmooth)