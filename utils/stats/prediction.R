source('~/GitHub/metrocell/utils/stats/multiplot.R')
library(ggplot2)
#red line  - predicted 
#blue boundary : control ==1
#green fill : true segment(+) == predicted segment(+)  segment has been defined correct
#red fill : true segment(-) != predicted segment(+)    segment has been defined incorrect
#blue fill : true segment (+) != predicted segment(-)  segment has not been defined,although it was true


analyzedDfPath = "C://temp//test/predicted//analyzedDf.csv"
predictedDfPath = "C://temp//test/predicted//PredictedDf_PowCorrMethod.csv"
dbPath = "C://temp//test//Cells_smoothed_ref-200-sm2.csv"

saveFolder = "C://temp//test//plots//predicted//"

analyzedDf = read.table(analyzedDfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
predictedDf = read.table(predictedDfPath,header = TRUE, sep = ",",encoding = 'UTF-8')
db = read.table(dbPath,header = TRUE, sep = ",",encoding = 'UTF-8')

linenames <- c("PREDICTED"="blue","ANALYZED" = "red")
iters = unique(analyzedDf$iter)
for( iter in iters){
 print (iter)
 analyzedSegDfFull = analyzedDf[analyzedDf$iter == iter,]
 predictedSegsDfFull = predictedDf[predictedDf$iter == iter,]
 analLaccids = unique(analyzedSegDfFull[analyzedSegDfFull$grabbed == 1,]$laccid)
 #for (lc in analLaccids){
  full = list()
  analyzedSegDf = analyzedSegDfFull[analyzedSegDfFull$laccid %in% analLaccids, ]
  predictedSegsDf = predictedSegsDfFull[predictedSegsDfFull$laccid %in% analLaccids, ]
  predictedSegsDf = predictedSegsDf[predictedSegsDf$controls == 1,]
 
  predictedSegsDf= predictedSegsDf[predictedSegsDf$method == 'byCorr',]
 
  #fname = paste(iter,lc,sep = "_")
  fullname = paste(saveFolder,iter,sep = "")
  
  segments = unique(predictedSegsDf$segment)
  laccids = unique(predictedSegsDf$laccid)
  dbSegsDf = db[db$segment %in% segments,]
  dbSegsDf = dbSegsDf[dbSegsDf$laccid %in% laccids,]
  nums = unique(predictedSegsDf$sliceNumber)
  step = 0
 if (length(nums)>0){
  for (num in nums){
    step = step + 1
    predictedslice = predictedSegsDf[predictedSegsDf$sliceNumber == num,]
    segment = unique(predictedslice$segment)[1]
    dbSegDf = dbSegsDf[dbSegsDf$segment %in% segment,]
    p<- ggplot()+ggtitle(paste("Segment ",segment, sep= ':'))+
      theme(plot.title = element_text(face = 'bold'))+
          geom_line(data = analyzedSegDf,aes(x = ratio,y = Power,colour = "ANALYZED"),size = 1) +
            geom_line(data = analyzedSegDf[analyzedSegDf$grabbed == 1,],aes(x = ratio,y = Power,colour = "ANALYZED"),size = 2)
    
    p2 = p + geom_line(data = predictedslice,aes(x = ratio,y = Power,colour = "PREDICTED"),size = 2,colour = 'blue') +
      geom_line(data = dbSegDf,aes(x = ratio,y = Power,colour = "PREDICTED"),size = 1) + 
      geom_text(data = predictedslice,aes(label = coeffs,x= Inf,y=Inf),hjust = 0.7,vjust = 1)
    p2 = p2 +scale_colour_manual(name = "Anal-Pred Lines",values = linenames)  
    p2 = p2 + facet_grid(laccid~.)
    if (segment %in% unique(analyzedSegDf$segment)){
      if (unique(predictedslice$maxCorrMinDelta)[1] == 1){
        col = 'green'
      }else{
        col = 'blue'
      }
      p2 = p2 + geom_rect(data = analyzedSegDf,aes(xmin = -Inf,xmax = Inf,ymin = -Inf,ymax = Inf),fill = col ,colour= col,size = 2,alpha = 0)
     }else{
    
      if (unique(predictedslice$maxCorrMinDelta)[1] == 1){
        p2 = p2+ geom_rect(data = analyzedSegDf,aes(xmin = -Inf,xmax = Inf,ymin = -Inf,ymax = Inf),fill = 'red',colour = 'red',size = 2 ,alpha = 0)
      }
     }
    #if (unique(predictedslice$controls)[1] == 1){
    #  p2 = p2 + geom_rect(data = analyzedSegDf,aes(xmin = -Inf,xmax = Inf,ymin = -Inf,ymax = Inf),fill = 'blue',color = 'blue',size = 2,alpha = 0)
    #}
    
    full[[step]]<-p2
  }
  ncol = length(nums)/3
  if (round(ncol) == ncol){
    ncol = round(ncol)
  }else{
   ncol = round(ncol)+1
  }
  
  jpeg(filename = paste(fullname,".png",sep = ""),width = 1280,height = 1024, quality = 400, bg = "white", res = NA, restoreConsole = TRUE)
  multiplot(plotlist = full,cols = ncol)
  dev.off()

 } 
  
}

#}
