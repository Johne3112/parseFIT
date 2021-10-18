readFITmessage <- function(df,message){
  
  myData <- df %>% dplyr::filter(Message == message)
  for(j in 1:nrow(myData)){
    datarow <- list()
    for(i in seq(from=4, to=ncol(myData)-1, by=3)){
      param <- tolower(as.character(myData[j,i]))
      if( !is.na(param) & param != ''){
        if(!(param %in% names(datarow))){
          val = as.numeric(myData[j,i+1])
          datarow[[param]] <- val
        }
      } else {
        break
      } 
    }
    datarow <- as.data.frame(datarow)
    if(j == 1){
      dfout <- datarow
    } else {
      dfout <- rbind.fill(dfout,datarow)
    }
  }
  
  #convert lat and long to degrees
  if('position_lat' %in% colnames(dfout)){
    dfout$position_lat <- dfout$position_lat * ( 180 / 2^31 )
  }
  if('position_long' %in% colnames(dfout)){
    dfout$position_long <- dfout$position_long * ( 180 / 2^31 )
  }
  
  if(message %in% c("record")){
    #aggregate and order by timestamp
    dfout <- dfout %>% dplyr::group_by(timestamp) %>% dplyr::summarise_each( ~ max(.,na.rm = TRUE))
    dfout <- do.call(data.frame,lapply(dfout, function(x) replace(x, is.infinite(x),NA)))
    dfout <- tidyr::fill(dfout, names(dfout))
    dfout <-dfout[order(dfout$timestamp),]
  }
  
  if(message %in% c("lap")){
    #order by timestamp
    dfout <-dfout[order(dfout$start_time),]
  }
  
  return(dfout)
  
}

readFIT <- function(fnfit,downloadedfit){
  
  for(i in 1:length(fnfit)){
    
    fncsv <- paste0(fnfit[[i]],'.csv')
    
    myinput <- paste('java -jar java/FitCSVTool.jar -b',fnfit[[i]],fncsv, sep = " ")
    
    system(myinput)
    
    df <- read.csv(fncsv, stringsAsFactors = FALSE)
    df <- df %>% dplyr::filter(Type == 'Data')
    
    if(i == 1){
      dfout <- df
    } else {
      if(ncol(dfout) >= ncol(df)){
        dfout <- rbind.fill(dfout,df)
      } else {
        dfout <- rbind.fill(df,dfout)
      }
    }
    
    
    
    ##delete the temp files
    if (file.exists(fnfit[[i]])) 
      #Delete file if it exists
      file.remove(fnfit[[i]])
    if (file.exists(fncsv)) 
      #Delete file if it exists
      file.remove(fncsv)
    if(!is.null(downloadedfit$fn[[i]])){
      if (file.exists(downloadedfit$fn[[i]])) 
        file.remove(downloadedfit$fn[[i]])
    }
  }
  
  myFIT <- list()
  messages <- unique(dfout$Message)
  print(messages)
  for(m in messages){
    myFIT[[m]] <- readFITmessage(dfout,m)
  }
  
  return(myFIT)
  
}


parseFIT <- function(fnfitlist,fnlist,downloadedfit){
  
  fnfitparse <- NULL
  
  for(i in 1:length(fnfitlist)){
    
    fnfit <- fnfitlist[[i]]
    fn <- fnlist[[i]]
    
    print(fn)
    
    #check if its a gz zipped file
    if(substr(fnfit,nchar(fnfit)-2,nchar(fnfit)) == ".gz"){
      gunzip(fnfit, remove = FALSE,overwrite = TRUE)
      fnfit <- substr(fnfit,1,nchar(fnfit)-3)
    } else if(substr(fnfit,nchar(fnfit)-3,nchar(fnfit)) == ".zip"){
      td = tempdir(check=TRUE)
      unzip(fnfit,overwrite = TRUE, exdir = td)
      fn <- substr(fn,1,nchar(fn)-4)
      fnfit <- paste(td,"/",fn,"_ACTIVITY.fit", sep = "")
    }
    
    fnfitparse <- append(fnfitparse,fnfit)
    
  }
  
  data <- readFIT(fnfitparse,downloadedfit)
  
  return(data)  
  
}
