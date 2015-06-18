setwd("~/GitHub/metrocell/data/referenced/msk/cell")
df = read.csv("pre_log_points-stops.csv",sep = ",")
setwd("~/GitHub/metrocell/data/")
smoothed = read.csv("Cells_smoothed_ref-200.csv",sep = ",")
saveFolder = "stats/plots/powers"

library(ggplot2)

data = smoothed[smoothed$NumRaces>3,]
segs = unique(data$segment)

for (seg in segs){
  seg_df = df[df$segment == seg,]
  users = unique(seg_df$User)
  for (user in users){
    user_df = seg_df[seg_df$User == user,]
    fname = paste(seg, user,sep="_")
    fname2 = paste(fname,".png",sep="")
    p = ggplot(data = user_df,aes(x = factor(race_id), y = Power))
    p2 = p+geom_boxplot()+geom_jitter(aes(colour = factor(laccid)))+facet_grid(laccid~.)
     theme(axis.text.x = element_text(angle = 45))

    ggsave(filename = fname2,plot = p2,path = saveFolder)
  }
}


#p01 = ggplot(data = df,aes(user, stdev))
#p02 = ggplot(data = df,aes(user, mean))
#p2 = p01+geom_boxplot()+geom_jitter()
#ggsave(filename = "stdev.png",plot = p2,path = saveFolder)
#p2 = p02+geom_boxplot()+geom_jitter()
#ggsave(filename = "Power.png",plot = p2,path = saveFolder)