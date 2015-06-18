setwd("C:/Users/Alex/PycharmProjects/Metrocell")
df = read.csv("data/stats/Power.csv",sep = ",")
df= df[df$segment == '069-070',]
saveFolder = "data/stats/plots"

library(ggplot2)

p01 = ggplot(data = df,aes(user, stdev))
p02 = ggplot(data = df,aes(user, mean))
#p2 = p01+geom_boxplot()+geom_jitter()
#ggsave(filename = "stdev.png",plot = p2,path = saveFolder)
p2 = p02+geom_boxplot()+geom_jitter()
ggsave(filename = "power.png",plot = p2,path = saveFolder)