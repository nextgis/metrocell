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
segmentsByRace <- function(df,saveFolder,sep = '')
  for (seg in unique(df$segment)){
    print(seg)
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = User))+ggtitle(paste("TestSegment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2 = power +geom_line(size=1.5)+facet_grid(race_id~laccid)
    ggsave(filename = paste(seg,".png",sep = sep),plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')
  }
segmentsByRace2 <- function(df,saveFolder,sep = '')
  for (seg in unique(df$segment)){
    print(seg)
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = MNC))+ggtitle(paste("TestSegment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2 = power +geom_line(size=1.5,aes(linetype = User) )+facet_grid(laccid~.)
    ggsave(filename = paste(seg,".png",sep = sep),plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')
  }
keyPoints<- function(df,saveFolder,test_segments,sep = '')
  for (seg in test_segments){

      print(seg)
      seg_df = df[df$segment == seg,]
      power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = laccid))+ggtitle(paste("TestSegment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
      power2 = power +geom_line(size=1.5,aes(linetype = Active))+facet_grid(race_id~User)
      ggsave(filename = paste(seg,".png",sep = sep),plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')

  }
testSegmentsGeomSmoothed <- function(df,saveFolder,test_segments)
  for (seg in test_segments){
    print(seg)
    seg_df = df[df$segment == seg,]
    power<-ggplot(seg_df,aes(x=ratio,y=Power,colour = laccid))+ggtitle(paste("TestSegment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
    power2 = power +geom_smooth(size=1.5,aes(linetype = Active))+facet_grid(User~.)
    ggsave(filename = paste(seg,".png"),plot = power2,path = saveFolder,width = 400,height = 400,units = 'mm')
  }
######################################################################################
library(ggplot2)
test_segments = c('098-099','099-098','100-099','099-100','100-101','101-100','074-075','075-074','074-073','073-074','151-219','219-151','068-069','069-068')
###paths###
#inputdfPath <- "C:\\temp\\test\\preproc\\MegaPhone\\pre_log_points-2G.csv"
fullDfPath <- "C:\\temp\\test\\referenced\\cell\\pre_log_points.csv"

testdfPath <- "C:\\temp\\test\\Cells_smoothed_ref-200-trans2.csv"
testFolder <- "C:\\temp\\test\\plots\\tests"

testSetsPath<- "C:\\temp\\test\\testSets.csv"

saveCellFolder <- "C:\\temp\\test\\plots\\byCells"
saveUserFolder <- "C:\\temp\\test\\plots\\byUsers"
#saveTestFolder <- "C:\\temp\\test\\plots\\testsByRace"
keyPointsFolder <- "C:\\temp\\test\\plots\\keyPoints"
saveTestGeomSmoothedFolder<- "C:\\temp\\test\\plots\\testsGeomSmoothedByRace\\full"
#inputdfPath <- "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\data\\referenced\\msk\\cell\\pre_log_points.csv"
#saveCellFolder <- "C:\\temp\\plots\\referenced/byCells"
#saveUserFolder <- "C:\\temp\\plots\\referenced/byUsers"
###########
df<-read.table(testSetsPath,header = TRUE, sep = ",",encoding = 'UTF-8')

#plots
#Cell_by_segments(df, saveCellFolder)
#User_by_segmnets(df, saveUserFolder)
segmentsByRace2(df,testFolder)
#testSegmentsGeomSmoothed(df,saveTestGeomSmoothedFolder,test_segments)
#keyPoints(df,keyPointsFolder,test_segments)