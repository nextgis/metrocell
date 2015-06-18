stdev<-'C:/Users/Alex/PycharmProjects/Metrocell/data/stdev.txt'
stations<- unique(df[df$Name!='ServiceLog',c('Name')])

sink(stdev)
for (st in stations){
  cat(st)
  cat('\n')
  cat('#######')
  cat('\n')
  by_station<- df[df$Name == st,]
  for (lc in unique(by_station$laccid)){
    by_laccid<-test[by_station$laccid == lc,]
    
    cat(lc)
    cat('\n')
    cat("#######")
    cat('\n')
    
    cat("Standart deviations by User:")
    cat('\n')
    cat(as.vector(unique(df$User)))
    cat('\n')
    cat(tapply(by_laccid$Power, by_laccid$User,sd))
    cat('\n')
    cat("General standart deviation")
    cat('\n')
    cat(sd(by_laccid$Power))
    cat('\n')
  }
}
sink(fileConn)
