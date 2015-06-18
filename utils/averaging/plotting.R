process_csv<-function(dfPath){
  df<-read.table(dfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
  df$lc_qual<-paste(as.character(df$laccid),as.character(df$quality),sep=" : ")
  return(df)
}

#########
#by laccid
#########
Cell_by_segments<-function(df,saveFolder)
  for (seg in unique(df$segment)){
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = User))+ggtitle(paste("Segment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2<-power+geom_line(size=1.5,aes(linetype = NetworkGen))+facet_grid(NetworkType~laccid,scale = "free_x")
    ggsave(filename = paste(seg,".png"),plot = power2,path = saveFolder, width = 10)
    
  }
User_by_segmnets<-function(df,saveFolder)
  for (seg in unique(df$segment)){
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = laccid))+ggtitle(paste("Segment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2<-power+geom_line(size=1.5,aes(linetype = NetworkGen))+facet_grid(User~.)
    ggsave(filename = paste(seg,".png"),plot = power2,path = saveFolder)
  }

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
inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\Cells_smoothed_unref-200.csv"
#inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\referenced\\msk\\cell\\pre_log_points.csv"

saveCellFolderSmooth <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\plots\\msk\\smoothed"
saveCellFolder <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\plots\\msk\\referenced/byCells"
saveUserFolder <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\plots\\msk\\referenced/byUsers"
###########
#binding csv - files at folder
df<-process_csv(inputdfPath)
#plots
#Cell_by_segments(df, saveCellFolder)
#User_by_segmnets(df, saveUserFolder)
Cell_by_segmnetsSmooth(df,saveCellFolderSmooth)
