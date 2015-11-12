Cell_by_segmnetsSmooth<-function(df,saveFolder)
  for (seg in unique(df$segment)){
    print(seg)
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = lc_qual))+ggtitle(paste("Segment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2<-power+geom_line(size=1.5)+facet_grid(laccid~.)
    ggsave(filename = paste(seg,".png"),plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')
  }
Cell_by_segmnetsSmooth2<-function(df,saveFolder)
  for (seg in unique(df$segment)){
    print(seg)
    seg_df = df[df$segment == seg,]
    laccids = unique(seg_df$laccid)
    for (lc in laccids){
      lc_df = seg_df[seg_df$laccid == lc,]
      fname = paste(seg, lc,sep="_")
      fname2 = paste(fname,".png",sep="")
      
      power<-ggplot(lc_df,aes(x=ratio,y=Power,colour = lc_qual))+ggtitle(paste("Segment ",lc_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
      power2<-power+geom_line(size=1.5)
      ggsave(filename = fname2,plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')
    }
  }
######################################################################################
library(ggplot2)
###paths###
inputdfPath <-"C:\\temp\\metrocell\\averaged\\spb\\Cells_smoothed_ref-200-MegaFon-2G.csv"
saveCellFolderSmooth <- "C:\\temp\\metrocell\\averaged\\spb\\plots\\averaged"
#inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\Cells_smoothed_ref-200.csv"
#saveCellFolderSmooth <- "C:\\temp\\plots\\smoothed"

df<-read.table(inputdfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
df$lc_qual<-paste(as.character(df$laccid),as.character(df$quality),sep=" : ")

Cell_by_segmnetsSmooth(df,saveCellFolderSmooth)