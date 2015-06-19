setwd("~/GitHub/metrocell/data/referenced/msk/cell")
df = read.csv("pre_log_points-stops.csv",sep = ",")
setwd("~/GitHub/metrocell/data/")
smoothed = read.csv("Cells_smoothed_ref-200.csv",sep = ",")
saveFolder = "C:/temp/plots/stats/UsersbyLaccids2"

library(ggplot2)

data = smoothed[smoothed$NumRaces>3,]
segs = unique(data$segment)

for (seg in segs){
  seg_df = df[df$segment == seg,]
  laccids = unique(seg_df$laccid)
  for (lc in laccids){
    lc_df = seg_df[seg_df$laccid == lc,]
    fname = paste(seg, lc,sep="_")
    fname2 = paste(fname,".png",sep="")
    p = ggplot(data = lc_df,aes(x = User, y = Power))
    p2 = p+geom_boxplot()+
      geom_jitter(aes(colour = factor(race_id)))+
          theme(axis.text.x = element_text(angle = 45))
    
    ggsave(filename = fname2,plot = p2,path = saveFolder)
  }
}
