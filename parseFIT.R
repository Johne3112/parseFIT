library(tidyverse)


readFITmessage <- function(df,message){
  
  if(message == 'hrv'){
    dfout <- gethrv(df)
  } else {
    myData <- df %>% dplyr::filter(Message == message)
    dfout <- list() #NEW
    for(j in 1:nrow(myData)){
      myrow <- paste0("row_",j)
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

      dfout[[myrow]] <- datarow #NEW
      
    }
    
    dfout <- lapply(dfout, data.frame, stringsAsFactors = FALSE)
    dfout <- dplyr::bind_rows(dfout)

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

readFIT <- function(fnfit){
  
  for(i in 1:length(fnfit)){
    
    fncsv <- paste0(fnfit[[i]],'.csv')
    
    # myinput <- paste('java -jar ~/EnDuRA_Users/java/FitCSVTool.jar -b',fnfit,fncsv, sep = " ")
    myinput <- paste0('java -jar java/FitCSVTool.jar -b "',fnfit[[i]],'" "',fncsv,'"')
    
    system(myinput)
    
    df <- read.csv(fncsv, stringsAsFactors = FALSE)
    # df <- readr::read_csv(fncsv)
    # df <- df %>% dplyr::filter(Type == 'Data')
    df <- df %>% dplyr::filter(.[[1]] == "Data")
    
    if(i == 1){
      dfout <- df
    } else {
      if(ncol(dfout) >= ncol(df)){
        dfout <- rbind.fill(dfout,df)
      } else {
        dfout <- rbind.fill(df,dfout)
      }
    }
    
    
  }
  
  myFIT <- list()
  messages <- unique(dfout$Message)
  print(messages)
  for(m in messages){
    myFIT[[m]] <- readFITmessage(dfout,m)
  }
  
  ##if hrv  in messages bind to record, else set RR to zero
  if('hrv' %in% messages){
    dfhrv <- myFIT$hrv %>% dplyr::group_by(timestamp) %>% dplyr::summarise(RR = mean(RR, na.rm = TRUE))
    myFIT$record <- dplyr::left_join(myFIT$record, dfhrv, by = "timestamp")
  } else {
    myFIT$record$RR <- 0
  }
  
  return(myFIT)
  
}


parseFIT <- function(fnfitlist,fnlist){
  
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
  
  data <- readFIT(fnfitparse)
  
  return(data)  
  
}


read_and_parse_FIT <- function(){
  #main function to parse a fit file
  
    myFIT <- file.choose()
    df_FIT <- parseFIT(myFIT,myFIT)
    return(df_FIT)

}


### CALL THIS LINE TO BE PROMPTED TO LOAD AND PARSE A FIT FILE
FIT_list <- read_and_parse_FIT()

view(FIT_list$record)
