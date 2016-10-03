#library("ggplot2", lib.loc="/Library/Frameworks/R.framework/Versions/3.2/Resources/library")
#library("RPostgreSQL", lib.loc="/Library/Frameworks/R.framework/Versions/3.2/Resources/library")

#libLoc = '/home/alexander_semenov/R/x86_64-pc-linux-gnu-library/3.2'
#libLoc2 = '/home/alexander_semenov/R/x86_64-pc-linux-gnu-library/3.3'
library('ggplot2')
library('RPostgreSQL')

push_to_db<-function(con,plot_name,id_from,id_to,city,MNC){
  del_query = paste("DELETE FROM signal_power_plots
                          WHERE id_from = '",id_from,"' AND ",
                    "id_to='",id_to, "' AND ",
                    "city = '",city ,"'",sep = '')
  try(rs<-dbSendQuery(con,del_query))
  if (!(class(rs) == "try-error")){
    if(dbGetInfo(rs, what = "rowsAffected") > 250){
      warning("Rolling back transaction")
      dbRollback(con)
    }else{
      dbCommit(con)
    }
  }
  dbRollback(con)
  insert_sql<-paste("INSERT INTO signal_power_plots(id_from,id_to,city,filename,ptype,mnc) 
                    VALUES('",id_from,"','",id_to,"','",city,"','",plot_name,"',","0,'",MNC,"')" ,sep='')
 
  rs<-dbSendQuery(con,insert_sql)
  if(dbGetInfo(rs, what = "rowsAffected") > 250){
    warning("Rolling back transaction")
    dbRollback(con)
  }else{
    dbCommit(con)
  }
}

update_mnc<-function(df){

  false_lc = c()
  true_lc = c()
  df2 = df
  
  df2$laccid_mnc = paste(df2$laccid,df2$MNC,sep = '_')
  laccids = unique(df2$laccid_mnc)
  for (lc in laccids){

    splitted = as.vector(strsplit(lc,'_')[[1]])
    if (splitted[3] == '-1'){
      false_lc = append(false_lc,lc)
    }else{
      true_lc = append(true_lc,lc)
    }
  }
  for (lc in unique(true_lc)){
    laccid = as.vector(strsplit(lc,'_')[[1]])
  
    df[(df$LAC == laccid[1])&(df$CID == laccid[2]),'MNC'] = laccid[3]
  }
  
  return(df) 
}

georeferencing_raw<- function(host,port,user,password,dbname,plot_base,id_from,id_to,city){
  #georeferencing function
  tablename = "parsed_cell"
  plot_folder = paste(plot_base,'/',city,'/',tablename,sep='')
  drv<- dbDriver("PostgreSQL")
  con<- dbConnect(drv,dbname = dbname,host = host,port = port,user = user,password = password)
  print (con)
  dbRollback(con)
  if (dbExistsTable(con,tablename)){
     seg_df <- dbGetQuery(con,paste("SELECT * FROM ",tablename," WHERE  
                                  city =  '", city ,
                                "' AND id_from = '", id_from ,
                                "' AND id_to = '", id_to , 
                                "' AND move_type = 'move'",
                                sep = ''
                                )
                           )
     seg_df = seg_df[(seg_df$ratio>0)&(seg_df$ratio<1),]
     seg_df = seg_df[(seg_df$LAC!='-1')&(seg_df$CID!='-1'),]
     seg_df$laccid = paste(seg_df$LAC  , seg_df$CID,sep = '_')
     seg_df = update_mnc(seg_df)
     GENs <- unique(seg_df$NetworkGen)
     for (GEN in GENs){
       plot_folder_gen = paste(plot_folder,GEN,sep='/')
       if (!dir.exists(plot_folder_gen)) dir.create(plot_folder_gen)
       gen_df = seg_df[seg_df$NetworkGen == GEN,]
       MNCs = unique(gen_df$MNC)
       for (MNC in MNCs){
          
          plot_folder_mnc = paste(plot_folder_gen,MNC,sep='/')
          if (!dir.exists(plot_folder_mnc)) dir.create(plot_folder_mnc)
          
          mnc_df = gen_df[gen_df$MNC == MNC,]
          power<-ggplot(mnc_df,aes(x=ratio,y=Power,colour = laccid))+ggtitle(paste("Segment ",id_from,' - ',id_to, sep= ''))+theme(plot.title = element_text(face = 'bold'))
          power2 = power +geom_line(size=1.5,aes(linetype = Active))+facet_grid(User~race_id)
            
          plot_name = paste(id_from,'_',id_to,".png",sep = '')
          ggsave(filename = plot_name,plot = power2,path = plot_folder_mnc,width = 400,height = 400,units = 'mm')
          push_to_db(con,plot_name,id_from,id_to,city,MNC)
       }
    }
  }else{
    print("There is no such table!")
  }
  dbDisconnect(con)
}

georeferencing_averaged<- function(host,port,user,password,dbname,plot_base,id_from,id_to,city){
  #georeferencing function
  tablename <- "averaged_cell_meta"
  drv<- dbDriver("PostgreSQL")
  plot_folder = paste(plot_base,city,'averaged_cell',sep='/')
  con<- dbConnect(drv,dbname = dbname,host = host,port = port,user = user,password = password)
  if (dbExistsTable(con,tablename)){
    seg_df <- dbGetQuery(con,paste("SELECT * FROM ",tablename," WHERE  
                                   city =  '", city ,
                                   "' AND segment_id='",paste(id_from,id_to,sep='-'),
                                   "' ",
                                   sep = ''
    ))

    seg_df = seg_df[(seg_df$ratio>0)&(seg_df$ratio<1),]
    seg_df$laccid = paste(seg_df$LAC  , seg_df$CID,sep = '_')
    GENs <- unique(seg_df$NetworkGen)
    for (GEN in GENs){
      plot_folder_gen = paste(plot_folder,GEN,sep='/')
      if (!dir.exists(plot_folder_gen)) dir.create(plot_folder_gen)
      
      gen_df = seg_df[seg_df$NetworkGen == GEN,]
      MNCs = unique(gen_df$MNC)
      for (MNC in MNCs){
        plot_folder_mnc = paste(plot_folder_gen,MNC,sep='/')
        if (!dir.exists(plot_folder_mnc)) dir.create(plot_folder_mnc)
        mnc_df = gen_df[gen_df$MNC == MNC,]
        power<-ggplot(mnc_df,aes(x=ratio,y=Power,colour = laccid))+ggtitle(paste("Segment ",seg_df$segment[1], sep= ':'))+theme(plot.title = element_text(face = 'bold'))
        power2 = power + geom_line(size=1.5)
        plot_name = paste(id_from,'_',id_to,".png",sep = '')
        ggsave(filename = plot_name,plot = power2,path = plot_folder_mnc,width = 400,height = 400,units = 'mm')
        #saved_ref_path<-plot_folder_mnc + '/' + plot_name
        push_to_db(con,plot_name,id_from,id_to,city,MNC)
      }
    }
    
    
  }else{
    print("There is no such table!")
  }
}
# main init

myArgs <- commandArgs(trailingOnly = TRUE)
print(myArgs)
plot_func = myArgs[1]
host = myArgs[2]
port = myArgs[3]
user = myArgs[4]
password = myArgs[5]
dbname = myArgs[6]
id_from = myArgs[7]
id_to = myArgs[8]
city = myArgs[9]


cons = dbListConnections(dbDriver("PostgreSQL"))
for (con in cons){
  dbDisconnect(con)
}
#plot_func = 'georeferencing_raw'
#host = "192.168.250.1"
#port = "5432"
#dbname = "metrocell"
#user = "alexander_semenov"
#password = {"SWVP9oMSgZUv"}
#id_from = '33'
#id_to = '32'
#city = 'spb'
#plot_base = '/Users/sasfeat/Documents/metrocell/data/plots'

plot_base = '/home/alexander_semenov/metrocell/data/plots'
do.call(plot_func,list(host,port,user,password,dbname,plot_base,id_from,id_to,city))

#georeferencing_raw(host,port,user,password,dbname,plot_base,id_from,id_to,city)
