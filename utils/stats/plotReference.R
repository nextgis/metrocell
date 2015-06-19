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


######################################################################################
library(ggplot2)
###paths###
inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\referenced\\msk\\cell\\pre_log_points.csv"
saveCellFolder <- "C:\\temp\\plots\\referenced/byCells"
saveUserFolder <- "C:\\temp\\plots\\referenced/byUsers"
###########
df<-read.table(dfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
#plots
Cell_by_segments(df, saveCellFolder)
User_by_segmnets(df, saveUserFolder)

