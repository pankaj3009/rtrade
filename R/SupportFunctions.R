# Tabulate index changes
library(rredis)
library(RQuantLib)
library(quantmod)
library(zoo)
options(scipen=999)

#data.env<- new.env()
if(is.null(getOption("datafolder"))){
  datafolder="/home/psharma/Dropbox/rfiles/data/"
}else{
  datafolder=getOption("datafolder")
}


specify_decimal <- function(x, k) as.numeric(trimws(format(round(x, k), nsmall=k)))

reverseEngRSI<-function(C,targetRSI,n){
  expPer=2*n-1
  auc=TTR::EMA(ifelse(C>Ref(C,-1),C-Ref(C,-1),0),expPer)
  adc=TTR::EMA(ifelse(Ref(C,-1)>C,Ref(C,-1)-C,0),expPer)
  x=(n-1)*(adc*targetRSI/(100-targetRSI)-auc)
  RevEngRSI=ifelse(x>=0,C+x,C+x*(100-targetRSI)/targetRSI)
  RevEngRSI
}

createIndexConstituents <-
  function(redisdb, pattern, threshold = "2000-01-01") {
    rredis::redisConnect()
    rredis::redisSelect(redisdb)
    rediskeys = redisKeys()
    dfstage1 <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    dfstage2 <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    symbols <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    rediskeysShortList <- as.character()
    thresholdDate = as.Date(threshold, format = "%Y-%m-%d", tz = "Asia/Kolkata")
    for (i in 1:length(rediskeys)) {
      l = length(grep(pattern, rediskeys[i]))
      if (l > 0) {
        if (thresholdDate < as.Date(
          substring(rediskeys[i], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata"
        )) {
          rediskeysShortList <- rbind(rediskeysShortList,
                                      rediskeys[i])
        }
      }
    }
    rediskeysShortList <- sort(rediskeysShortList)
    if(length(rediskeysShortList)==0){
      thresholdDate=NULL
      # add the last record date matching pattern
      for (i in 1:length(rediskeys)) {
        l = length(grep(pattern, rediskeys[i]))
        if (l > 0) {
          if (is.null(thresholdDate) || thresholdDate < as.Date(
            substring(rediskeys[i], nchar(pattern) + 2),
            format = "%Y%m%d",
            tz = "Asia/Kolkata"
          )) {
            thresholdDate=as.Date(
              substring(rediskeys[i], nchar(pattern) + 2),
              format = "%Y%m%d",
              tz = "Asia/Kolkata"
            )
            rediskeysShortList <- rediskeys[i]
          }
        }
      }
    }
    for (i in 1:length(rediskeysShortList)) {
      seriesstartdate = substring(rediskeysShortList[i], nchar(pattern) + 2)
      seriesenddate = ifelse(
        i == length(rediskeysShortList),
        Sys.Date(),
        as.Date(
          substring(rediskeysShortList[i + 1], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata"
        ) - 1
      )
      symbols <-
        data.frame(
          symbol = unlist(rredis::redisSMembers(rediskeysShortList[i])),
          startdate = as.Date(
            seriesstartdate,
            format = "%Y%m%d",
            tz = "Asia/Kolkata"
          ),
          enddate = as.Date(seriesenddate, origin = "1970-01-01"),
          stringsAsFactors = FALSE
        )
      for (j in 1:nrow(symbols)) {
        existingindex = which(
          dfstage2$symbol == symbols$symbol[j] &
            dfstage2$enddate == symbols$startdate[j] - 1
        )
        if (length(existingindex) > 0) {
          #replace row in dfstage2
          dfstage2[existingindex, ] <-
            data.frame(
              symbol = symbols$symbol[j],
              startdate = dfstage2[existingindex, c("startdate")],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
        } else{
          dfstage2 = rbind(
            dfstage2,
            data.frame(
              symbol = symbols$symbol[j],
              startdate = symbols$startdate[j],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
          )
        }
      }
    }
    rredis::redisClose()
    dfstage2

  }

createFNOConstituents <-
  function(redisdb, pattern, threshold = "2000-01-01") {
    rredis::redisConnect()
    rredis::redisSelect(redisdb)
    rediskeys = redisKeys()
    dfstage1 <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    dfstage2 <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    symbols <- data.frame(
      symbol = character(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    rediskeysShortList <- as.character()
    thresholdDate = as.Date(threshold, format = "%Y-%m-%d", tz = "Asia/Kolkata")
    for (i in 1:length(rediskeys)) {
      l = length(grep(pattern, rediskeys[i]))
      if (l > 0) {
        if (thresholdDate < as.Date(
          substring(rediskeys[i], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata"
        )) {
          rediskeysShortList <- rbind(rediskeysShortList,
                                      rediskeys[i])
        }
      }
    }
    rediskeysShortList <- sort(rediskeysShortList)
    for (i in 1:length(rediskeysShortList)) {
      seriesstartdate = as.Date(ifelse(
        i == 1,
        thresholdDate,
        as.Date(
          substring(rediskeysShortList[i - 1], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata",
          origin = "1970-01-01"
        )
      ) + 1,
      origin = "1970-01-01")
      seriesenddate = as.Date(
        substring(rediskeysShortList[i], nchar(pattern) +
                    2),
        format = "%Y%m%d",
        tz = "Asia/Kolkata"
      )
      symbols <-
        data.frame(
          symbol = names(unlist(
            redisHGetAll(rediskeysShortList[i])
          )),
          startdate = as.Date(
            seriesstartdate,
            format = "%Y%m%d",
            tz = "Asia/Kolkata"
          ),
          enddate = as.Date(seriesenddate, tz = "Asia/Kolkata"),
          stringsAsFactors = FALSE
        )
      for (j in 1:nrow(symbols)) {
        existingindex = which(
          dfstage2$symbol == symbols$symbol[j] &
            (dfstage2$enddate + 1) == (symbols$startdate[j])
        )
        if (length(existingindex) > 0) {
          #replace row in dfstage2
          dfstage2[existingindex, ] <-
            data.frame(
              symbol = symbols$symbol[j],
              startdate = dfstage2[existingindex, c("startdate")],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
        } else{
          dfstage2 = rbind(
            dfstage2,
            data.frame(
              symbol = symbols$symbol[j],
              startdate = symbols$startdate[j],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
          )
        }
      }
    }
    rredis::redisClose()
    dfstage2

  }

createFNOSize <-
  function(redisdb, pattern, threshold = "2000-01-01") {
    rredis::redisConnect()
    rredis::redisSelect(redisdb)
    rediskeys = redisKeys()
    dfstage2 <- data.frame(
      symbol = character(),
      contractsize = numeric(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    symbols <- data.frame(
      symbol = character(),
      contractsize = numeric(),
      startdate = as.Date(character()),
      enddate = as.Date(character()),
      stringsAsFactors = FALSE
    )
    rediskeysShortList <- character()
    thresholdDate = as.Date(threshold, format = "%Y-%m-%d", tz = "Asia/Kolkata")
    for (i in 1:length(rediskeys)) {
      l = length(grep(pattern, rediskeys[i]))
      if (l > 0) {
        if (thresholdDate < as.Date(
          substring(rediskeys[i], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata"
        )) {
          rediskeysShortList <- rbind(rediskeysShortList,
                                      rediskeys[i])
        }
      }
    }
    rediskeysShortList <- sort(rediskeysShortList)

    for (i in 1:length(rediskeysShortList)) {
      seriesenddate = substring(rediskeysShortList[i], nchar(pattern) + 2)
      seriesstartdate = ifelse(
        i == 1,
        as.Date("2000-01-01"),
        as.Date(
          substring(rediskeysShortList[i - 1], nchar(pattern) + 2),
          format = "%Y%m%d",
          tz = "Asia/Kolkata"
        ) + 1
      )
      symbolsunformatted <-
        unlist(redisHGetAll(rediskeysShortList[i]))
      symbols <-
        data.frame(
          symbol = names(symbolsunformatted),
          contractsize = as.numeric(symbolsunformatted),
          enddate = as.Date(
            seriesenddate,
            format = "%Y%m%d",
            tz = "Asia/Kolkata"
          ),
          startdate = as.Date(seriesstartdate, origin = "1970-01-01"),
          stringsAsFactors = FALSE
        )
      for (j in 1:nrow(symbols)) {
        existingindex = which(
          dfstage2$symbol == symbols$symbol[j] &
            dfstage2$contractsize == symbols$contractsize[j] &
            dfstage2$enddate == symbols$startdate[j] - 1
        )
        if (length(existingindex) > 0) {
          #replace row in dfstage2
          dfstage2[existingindex, ] <-
            data.frame(
              symbol = symbols$symbol[j],
              contractsize = symbols$contractsize[j],
              startdate = dfstage2[existingindex, c("startdate")],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
        } else{
          dfstage2 = rbind(
            dfstage2,
            data.frame(
              symbol = symbols$symbol[j],
              contractsize = symbols$contractsize[j],
              startdate = symbols$startdate[j],
              enddate = symbols$enddate[j],
              stringsAsFactors = FALSE
            )
          )
        }
      }
    }
    rredis::redisClose()
    dfstage2

  }

getMostRecentSymbol <- function(symbol) {
  symbolchange=getSymbolChange()
  if(length(symbol)==1){
    out <- linkedsymbols(symbolchange, symbol,TRUE)$symbol
    out <- out[length(out)]
  }else{
    out=lapply(symbol,linkedsymbols,symbolchange=symbolchange,complete=TRUE)
    out=sapply(out,function(x) x[[2]][length(x[[2]])])
  }
  out
}

readAllSymbols <-
  function(redisdb, pattern, threshold = "2000-01-01") {
    rredis::redisConnect()
    rredis::redisSelect(redisdb)
    rediskeys = redisKeys()
    symbols <- data.frame(
      brokersymbol = character(),
      exchangesymbol = character(),
      stringsAsFactors = FALSE
    )
    rediskeysShortList <- as.character()
    for (i in 1:length(rediskeys)) {
      l = length(grep(pattern, rediskeys[i]))
      if (l > 0) {
        rediskeysShortList <- rbind(rediskeysShortList, rediskeys[i])
      }
    }
    rediskeysShortList <- sort(rediskeysShortList)
    rediskeysShortList <-
      rediskeysShortList[length(rediskeysShortList)]
    symbols <- unlist(redisHGetAll(rediskeysShortList))
    symbols <-
      data.frame(
        exchangesymbol = names(symbols),
        brokersymbol = symbols,
        stringsAsFactors = FALSE
      )
    return(symbols)
  }

createPNLSummary <- function(redisdb,pattern,start,end) {
  #pattern = strategy name (Case sensitive)
  #start,end = string as "yyyy-mm-dd"
  #mdpath = path to market data files for valuing open positions
  #realtrades<-createPNLSummary(0,"swing01","2017-01-01","2017-01-31","/home/psharma/Seafile/rfiles/daily-fno/")
  rredis::redisConnect()
  rredis::redisSelect(redisdb)
  actualtrades <-
    data.frame(
      symbol = character(),
      trade = character(),
      size = as.numeric(),
      entrytime = as.POSIXct(character()),
      entryprice = numeric(),
      exittime = as.POSIXct(character()),
      exitprice = as.numeric(),
      percentprofit = as.numeric(),
      bars = as.numeric(),
      exitreason=character(),
      brokerage = as.numeric(),
      netpercentprofit = as.numeric(),
      pnl = as.numeric(),
      key = character(),
      netposition=as.numeric(),
      stringsAsFactors = FALSE
    )
  periodstartdate = as.Date(start, tz = "Asia/Kolkata")
  periodenddate = as.Date(end, tz = "Asia/Kolkata")

  rediskeysShortList <- as.character()
  rediskeysShortList=rredis::redisKeys(pattern)
  if(!is.null(rediskeysShortList)){
    rediskeysShortList <- sort(rediskeysShortList)
    # loop through keys and generate pnl
    for (i in 1:length(rediskeysShortList)) {
      data <- unlist(redisHGetAll(rediskeysShortList[i]))
      exitprice = 0
      entrydate = data["entrytime"]
      exitdate = data["exittime"]
      entrydate = as.Date(entrydate, tz = "Asia/Kolkata")
      exitdate = ifelse(
        is.na(exitdate),
        Sys.Date(),
        as.Date(
          exitdate,
          format = "%Y-%m-%d %H:%M:%S",
          tz = "Asia/Kolkata",
          origin = "1970-01-01"
        )
      )
      if (exitdate >= periodstartdate &&
          entrydate <= periodenddate) {
        if (grepl("opentrades", rediskeysShortList[i])) {
          symbol = data["entrysymbol"]
          symbolsvector = unlist(strsplit(symbol, "_"))
          md=loadSymbol(symbol,days=100000)
          index = which(as.Date(md$date, tz = "Asia/Kolkata") == exitdate)
          if (length(index) == 0) {
            index = nrow(md)
          }
          exitprice = md$settle[index]
        } else{
          exitprice = as.numeric(data["exitprice"])
        }
        percentprofit = (exitprice - as.numeric(data["entryprice"])) / as.numeric(data["entryprice"])
        percentprofit = ifelse(data["entryside"] == "BUY",
                               percentprofit,
                               -percentprofit)
        entrybrokerage=as.numeric(data["entrybrokerage"])
        entrybrokerage=ifelse(is.na(entrybrokerage),0,entrybrokerage)
        exitbrokerage=as.numeric(data["exitbrokerage"])
        exitbrokerage=ifelse(is.na(exitbrokerage),0,exitbrokerage)
        exitsize=as.numeric(data["exitsize"])
        exitsize=ifelse(is.na(exitsize),0,exitsize)
        brokerage = entrybrokerage + exitbrokerage
        netpercentprofit = percentprofit - (brokerage / (
          as.numeric(data["entryprice"]) *
            as.numeric(data["entrysize"])
        ))
        netprofit = (exitprice - as.numeric(data["entryprice"])) * as.numeric(data["entrysize"]) -
          brokerage
        netprofit = ifelse(data["entryside"] == "BUY", netprofit, -netprofit)
        exitreason=ifelse(is.na(as.character(data["exitreason"])),"Open",as.character(data["exitreason"]))
        df = data.frame(
          symbol = data["parentsymbol"],
          trade = data["entryside"],
          size = as.numeric(data["entrysize"]),
          entrytime = as.POSIXct(data["entrytime"], tz = "Asia/Kolkata"),
          entryprice = as.numeric(data["entryprice"]),
          exittime = as.POSIXct(data["exittime"], tz = "Asia/Kolkata"),
          exitprice = exitprice,
          percentprofit = percentprofit,
          bars = 0,
          exitreason=exitreason,
          brokerage = brokerage,
          netpercentprofit = netpercentprofit,
          pnl = netprofit,
          key = rediskeysShortList[i],
          netposition=ifelse(data["entryside"]=="BUY",as.numeric(data["entrysize"])-exitsize,-as.numeric(data["entrysize"])+exitsize),
          stringsAsFactors = FALSE
        )
        rownames(df) <- NULL
        actualtrades = rbind(actualtrades, df)

      }
    }
  }
  actualtrades
}

getExpiryDate <- function(mydate) {
  #mydate = Date object
  if(is.na(mydate)){
    return(mydate)
  }
  eom = RQuantLib::getEndOfMonth(calendar = "India", as.Date(mydate, tz = "Asia/Kolkata"))
  weekday = as.POSIXlt(eom, tz = "Asia/Kolkata")$wday + 1
  adjust = weekday - 5
  possible.holidays.at.eom=5-weekday
  month.of.thursday=13
  if(possible.holidays.at.eom>0){
    month.of.thursday=as.POSIXlt(eom+possible.holidays.at.eom, tz = "Asia/Kolkata")$mon
  }
  if (weekday > 5) {
    #expirydate=as.Date(modAdvance(-adjust,"India",as.Date(eom),0,2))
    expirydate = eom - (weekday - 5)
  } else if (weekday == 5|(weekday<5 & month.of.thursday==as.POSIXlt(eom, tz = "Asia/Kolkata")$mon)) {
    expirydate = as.Date(eom)
  } else{
    #expirydate=as.Date(modAdvance(-(5+adjust),"India",as.Date(eom),0,2))
    expirydate = eom - 7 + (5 - weekday)
  }
  if (as.Date(mydate, tz = "Asia/Kolkata") > expirydate) {
    expirydate = getExpiryDate(as.Date(mydate) + 20)
  }
  expirydate = RQuantLib::adjust("India", expirydate, 2)
  as.Date(expirydate, tz = "Asia/Kolkata")
}

getPriceArrayFromRedis <-
  function(redisdb, symbol, duration, type, starttime,endtime,tz="Asia/Kolkata") {
    # Retrieves OHLCS prices from redisdb (currently no 9) starting from todaydate till end date.
    #symbol = redis symbol name in db=9
    #duration = [tick,daily]
    # type = [OPT,STK,FUT]
    # todaydate = starting timestamp for retrieving prices formatted as "YYYY-mm-dd HH:mm:ss"
    rredis::redisConnect()
    rredis::redisSelect(as.numeric(redisdb))
    start = as.numeric(as.POSIXct(starttime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
    end = as.numeric(as.POSIXct(endtime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
    a <-
      redisZRangeByScore(paste(symbol, duration, type, sep = ":"), min = start, max=end)
    rredis::redisClose()
    price = jsonlite::stream_in(textConnection(gsub("\\n", "", unlist(a))))
    if (nrow(price) > 0) {
      price$value = as.numeric(price$value)
      high = max(price$value)
      low = min(price$value)
      open = price$value[1]
      close = price$value[nrow(price)]
      timeformat = as.POSIXct(first(price$time)/1000, origin="1970-01-01")
      timeformat=strftime(timeformat,format="%Y-%m-%d",tz=tz)
      timeformat=as.POSIXct(timeformat)
      out = data.frame(
        date = timeformat,
        open = open,
        high = high,
        low = low,
        close = close,
        settle = close
      )
    } else{
      out = data.frame()
    }
    return(out)
  }

getVWAP<-function(redisdb,symbol,starttime,endtime){
  price<-getPriceHistoryFromRedis(redisdb,symbol,"tick","close",starttime,endtime)
  size <-getPriceHistoryFromRedis(redisdb,symbol,"tick","size",starttime,endtime)
  out<-merge(size,price,by=c('date'),all.x=TRUE)
  names(out)<-c("date","size","close")
  out$close=na.locf(out$close)
  out<-out[complete.cases(out),]
  weighted.mean(out$close,out$size)
}

getVWAPUsingDayVolume<-function(redisdb,symbol,starttime,endtime){
  price<-getPriceHistoryFromRedis(redisdb,symbol,"tick","close",starttime,endtime)
  size <-getPriceHistoryFromRedis(redisdb,symbol,"tick","dayvolume",starttime,endtime)
  out<-merge(size,price,by="date",all.x=TRUE)
  names(out)<-c("date","size","close")
  size<-zoo(out$size)
  size=lag(size,0:-1,na.pad = TRUE)
  size=size$lag0-size$"lag-1"
  out$close=na.locf(out$close)
  out<-out[complete.cases(out),]
  weighted.mean(out$close,out$size)
}

getPriceHistoryFromRedis <-
  function(redisdb, symbol, duration, type, starttime,endtime) {
    # Retrieves OHLCS prices from redisdb (currently no 9) starting from todaydate till end date.
    #symbol = redis symbol name in db=9
    #duration = [tick,daily]
    # type = [OPT,STK,FUT]
    # todaydate = starting timestamp for retrieving prices formatted as "YYYY-mm-dd HH:mm:ss"
    rredis::redisConnect()
    rredis::redisSelect(as.numeric(redisdb))
    start = as.numeric(as.POSIXct(starttime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
    end = as.numeric(as.POSIXct(endtime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
    a <-
      redisZRangeByScore(paste(symbol, duration, type, sep = ":"), min = start, max=end)
    rredis::redisClose()
    price = jsonlite::stream_in(textConnection(gsub("\\n", "", unlist(a))))
    if (nrow(price) > 0) {
      price$value = as.numeric(price$value)
      out = data.frame(
        date = as.POSIXlt(price$time/1000,tz="Asia/Kolkata",origin="1970-01-01"),
        value = price$value
      )
    } else{
      out = data.frame()
    }
    return(out)
  }

getIntraDayBars<-function(redisdb,symbol,duration,type,starttime,endtime,minutes){
  exchangesymbol=strsplit(symbol,"_")[[1]][1]
  symboltype=strsplit(symbol,"_")[[1]][2]
  if(symboltype=="STK" |symboltype=="IND"){
    md=kGetOHLCV(paste("symbol",tolower(exchangesymbol),sep="="),df=data.frame(),start=starttime,end=endtime,name="india.nse.equity.s1.1sec", aValue = minutes, aUnit = "Minutes")
  }
  md$close=dplyr::lead(md$close)
  md<-md[complete.cases(md),]
  newstarttime=starttime
  if(nrow(md)>0){
    newstarttime=md$date[nrow(md)]
    newstarttime=newstarttime+minutes*60
    newstarttime=strftime(newstarttime,format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")
  }
  if(newstarttime<endtime){
    mdtodayseries=getPriceHistoryFromRedis(redisdb,symbol,duration,type,newstarttime,endtime)
    mdtodayseries=convertToXTS(mdtodayseries)
    mdtodayseries=mdtodayseries["T09:15/T15:30"]
    if(!is.null(mdtodayseries)){
      mdtoday=to.period(mdtodayseries,period="minutes",k=minutes,indexAt = "startof")
      mdtoday=convertToDF(mdtoday)
      rownames(mdtoday)=c()
      names(mdtoday)=c("open","high","low","close","date")
      mdtoday$symbol=exchangesymbol
      mdtoday$volume=0
      md=rbind(md,mdtoday)
      md$date=round(md$date,"mins")
      md$date=as.POSIXct(md$date)
    }
  }

  # update splitinformation
  splitinfo=getSplitInfo(exchangesymbol)
  if(nrow(splitinfo)>0){
    splitinfo=splitinfo[rev(order(splitinfo$date)),]
    md$dateonly=as.Date(md$date,tz="Asia/Kolkata")
    splitinfo$date=as.Date(splitinfo$date,tz="Asia/Kolkata")
    splitinfo$splitadjust=cumprod(splitinfo$newshares)/cumprod(splitinfo$oldshares)
    md=merge(md,splitinfo[,c("date","splitadjust")],by.x=c("dateonly"),by.y=c("date"),all.x = TRUE)
    md=md[ , -which(names(md) %in% c("dateonly"))]
    md$splitadjust=ifelse(is.na(md$splitadjust),1,md$splitadjust)
    md$splitadjust=Ref(md$splitadjust,1)
    md$splitadjust[nrow(md)]=md$splitadjust[nrow(md)-1]
    md$splitadjust=ifelse(md$splitadjust==Ref(md$splitadjust,-1),1,md$splitadjust)
    md=md[rev(order(md$date)),]
    md$splitadjust=cumprod(md$splitadjust)
    md=md[order(md$date),]
    md$splitadjust[1]=md$splitadjust[2]
  }else{
    md$splitadjust=1
  }
  md$settle=md$close
  md$aopen=md$open/md$splitadjust
  md$ahigh=md$high/md$splitadjust
  md$alow=md$low/md$splitadjust
  md$asettle=md$settle/md$splitadjust
  md$aclose=md$close/md$splitadjust
  md$avolume=md$volume*md$splitadjust
  md$delivered=0
  md$adelivered=0
  md

}


GetCurrentPosition <-
  function(scrip,
           portfolio,
           path = NULL,
           deriv=FALSE,
           splitadjustedPortfolio=TRUE,
           trades.till = Sys.time(),
           position.on = Sys.time()) {
    # Returns the current position for a scrip after calculating all rows in portfolio.
    #scrip = String
    #Portfolio = df containing columns[symbol,exittime,trade,size]
    # path of market data file that holds split information, if any
    # optional trades.till is compared to the entrytime to filter records used for position calculation. All records equal or before startdate are included for position calc
    # optional position.on is compared to the exittime to filter records used for position calculation. All records AFTER and INCLUDING enddate are included for position calc.
    # final position calculation subset is intersection of optional startdate/ optional enddate filters.
    position <- 0
    handlesplits=FALSE
    if(!is.null(path)){
      if(deriv){
        symbolsvector = unlist(strsplit(scrip, "_"))
        md=loadSymbol(scrip,days=1000000)
      }else{
        md=loadSymbol(scrip,days=1000000)
      }
      if("splitadjust" %in% colnames(md)){
        handlesplits=TRUE
      }
    }
    if (nrow(portfolio) > 0) {
      portfolio <- portfolio[portfolio$entrytime <= trades.till, ]
      for (row in 1:nrow(portfolio)) {
        if (((portfolio[row, 'exitreason']=="Open") || portfolio[row, 'exittime'] >= position.on ) &&  portfolio[row, 'symbol'] == scrip) {
          splitadjustment=1
          if(handlesplits){
            if(deriv){
              symbolsvector = unlist(strsplit(name, "_"))
              md=loadSymbol(scrip,days=1000000)
            }else{
              md=loadSymbol(scrip,days=1000000)
            }
            buyindex= which(md$date == portfolio[row,'entrytime'])
            currentindex= which(md$date == position.on)
            if(!splitadjustedPortfolio){
              splitadjustment=md$splitadjust[buyindex]/md$splitadjust[currentindex]
            }else{
              splitadjustment=1 # dont do any split adjustment as portfolio is alredy split adjusted
            }

          }
          position = position + ifelse(grepl("BUY", portfolio[row, 'trade']),
                                       floor(portfolio[row, 'size']*splitadjustment),
                                       -floor(portfolio[row, 'size']*splitadjustment))
        }
      }
    }
    position
  }

CalculateDailyPNL <-
  function(portfolio,
           pnl,
           brokerage,
           margin=1,
           marginOnUnrealized=FALSE,
           intraday=FALSE,
           realtime=FALSE,
           timeZone="Asia/Kolkata") {
    #Returns realized and unrealized pnl for each day
    # Should be called after portfolio is constructed
    # portfolio = df containing columns[symbol,entrytime,exittime,entryprice,exitprice,trade,size]
    # portfolio$exitdate should be NA for rows that still have open positions.
    # pnl = pnl<-data.frame(bizdays=as.Date(subset$date,tz="Asia/Kolkata"),realized=0,unrealized=0,brokerage=0)
    #brokerage = % brokerage of trade value
    #realized and unrealized profits are cumulative till date
    # realized and unrealized pnl are gross of brokerage. To get net pnl, dont forget to subtract brokerage
    # margin weights longnpv and shortnpv for margin
    # marginOnUnrealized=TRUE ensures that cashdeployed includes unrealized profit, as this unrealized profit can result in margin calls.
    # intraday flag should be set to false for EOD strategies, as entrytime/exittime might not match market data time
    print(paste("calculate dailypnl realtime",realtime,sep=":"))
    pnl$positioncount=0
    pnl$longnpv=0
    pnl$shortnpv=0
    if (nrow(portfolio) > 0) {
      #for (l in 1:107){
      for (l in 1:nrow(portfolio)) {
        name = portfolio[l, 'symbol']
        entrydate=as.POSIXct(strftime(portfolio[l, 'entrytime'],format="%Y-%m-%d"))
        exitdate=as.POSIXct(strftime(portfolio[l, 'exittime'],format="%Y-%m-%d"))
        md=loadSymbol(name,days=1000000,realtime = realtime)
        handlesplits=FALSE
        if("splitadjust" %in% colnames(md)){
          handlesplits=TRUE
        }
        md=unique(md)
        entryindex = which(md$date == entrydate)
        if(portfolio[l,c("exitreason")]=="Open"){
          # we do not have an exit date
          exitindex = nrow(md)
          altexitindex = which(md$date == pnl$bizdays[nrow(pnl)])
          exitindex = ifelse(length(altexitindex) > 0, altexitindex, exitindex)
          unrealizedpnlexists = TRUE
        }else{
          exitindex=last(which(md$date <= exitdate))
          unrealizedpnlexists = FALSE
        }

        mtm = portfolio[l, "entryprice"]
        size = portfolio[l, 'size']
        dtindex = 0
        if (length(entryindex) == 1) {
          dtindexstart = which(pnl$bizdays == md$date[entryindex])
          dtindexend = which(pnl$bizdays == md$date[exitindex])
          cumunrealized = seq(0,0,length.out = (dtindexend -  dtindexstart + 1))
          side = portfolio[l, 'trade']
          positionindexstart = which(pnl$bizdays == md$date[entryindex])
          positionindexend = which(pnl$bizdays == md$date[exitindex])
          pnl$positioncount[positionindexstart:(positionindexend)]<-pnl$positioncount[positionindexstart:(positionindexend)]+1
          entryprice=NA_real_
          if(entryindex==exitindex){
            loopend=entryindex
          }else{
            loopend=exitindex-1
          }
          for (index in entryindex:loopend) {
            #entryindex,exitindex are indices on md
            #dtindex,dtindexstart,dtindexend are indices on pnl
            dtindex = which(pnl$bizdays == md$date[index])# it is possible that bizdays does not match the md$dates!
            if (length(dtindex) > 0) {
              if(handlesplits){
                newprice=md$asettle[index]
              }else{
                newprice = md$settle[index]
              }
              if(entryindex==index){
                entryprice=mtm
              }
              newsplitadjustment=1
              pnl$unrealized[dtindex:nrow(pnl)] <-  pnl$unrealized[dtindex:nrow(pnl)] + ifelse(grepl("BUY", side),(newprice*newsplitadjustment-mtm)*size,(mtm-newprice*newsplitadjustment) * size)
              cumunrealized[(dtindex - dtindexstart + 1)] = ifelse(grepl("BUY", side),(newprice*newsplitadjustment-mtm) * size,(mtm-newprice*newsplitadjustment ) * size )
              if(marginOnUnrealized){
                pnl$longnpv[dtindex]=pnl$longnpv[dtindex]+ifelse(grepl("BUY", side),(newprice*newsplitadjustment)*size,0)
                pnl$shortnpv[dtindex]=pnl$shortnpv[dtindex]+ifelse(grepl("SHORT", side),(newprice*newsplitadjustment)*size,0)
              }else if(!marginOnUnrealized) {
                pnl$longnpv[dtindex]=pnl$longnpv[dtindex]+ifelse(grepl("BUY", side),(entryprice*newsplitadjustment)*size,0)
                pnl$shortnpv[dtindex]=pnl$shortnpv[dtindex]+ifelse(grepl("SHORT", side),(entryprice*newsplitadjustment)*size,0)
              }
              mtm <- newprice
              if (index == entryindex) {
                pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + calculateBrokerage(portfolio[l,],brokerage,calculation = "ENTRY")
              }
            }
          }
          newsplitadjustment=md$splitadjust[(exitindex)]/md$splitadjust[(exitindex-1)]
          newsplitadjustment=1
          lastdaypnl = ifelse(grepl("BUY", side),(portfolio[l, 'exitprice']*newsplitadjustment - mtm) * size,(mtm - portfolio[l, 'exitprice']*newsplitadjustment) * size)
          relativeindex=exitindex-loopend
          if (!unrealizedpnlexists) {
            pnl$realized[(dtindex+relativeindex):nrow(pnl)] <-pnl$realized[(dtindex+relativeindex):nrow(pnl)] + sum(cumunrealized) + lastdaypnl
            pnl$brokerage[(dtindex+relativeindex):nrow(pnl)] = pnl$brokerage[(dtindex+relativeindex):nrow(pnl)] + calculateBrokerage(portfolio[l,],brokerage,calculation = "EXIT")
            pnl$unrealized[(dtindex+relativeindex):nrow(pnl)] = pnl$unrealized[(dtindex+relativeindex):nrow(pnl)] - sum(cumunrealized)
          } else{
            pnl$unrealized[(dtindex+relativeindex):nrow(pnl)] = pnl$unrealized[(dtindex+relativeindex):nrow(pnl)] + lastdaypnl
            if(marginOnUnrealized & relativeindex>0){
              pnl$longnpv[dtindex+relativeindex]=pnl$longnpv[dtindex+relativeindex]+ifelse(grepl("BUY", side),(portfolio[l, 'exitprice']*newsplitadjustment) * size,0)
              pnl$shortnpv[dtindex+relativeindex]=pnl$shortnpv[dtindex+relativeindex]+ifelse(grepl("SHORT", side),(portfolio[l, 'exitprice']*newsplitadjustment) * size,0)
            }else if(!marginOnUnrealized & relativeindex>0){
              pnl$longnpv[dtindex+relativeindex]=pnl$longnpv[dtindex+relativeindex]+ifelse(grepl("BUY", side),(entryprice*newsplitadjustment) * size,0)
              pnl$shortnpv[dtindex+relativeindex]=pnl$shortnpv[dtindex+relativeindex]+ifelse(grepl("SHORT", side),(entryprice*newsplitadjustment) * size,0)

            }
          }
        }
      }
    }
    pnl$cashdeployed=pnl$longnpv*margin+pnl$shortnpv*margin-pnl$realized+pnl$brokerage
    pnl$cashflow=c(NA_real_,diff(pnl$cashdeployed))
    pnl$cashflow[1]=pnl$cashdeployed[1]
    pnl$cashflow=-round(pnl$cashflow,0)
    if(marginOnUnrealized){
      pnl$cashflow[nrow(pnl)]=pnl$cashflow[nrow(pnl)]+(pnl$longnpv+pnl$shortnpv)[nrow(pnl)]*margin
    }
    else{
      pnl$cashflow[nrow(pnl)]=last(pnl$cashflow+pnl$unrealized+(pnl$longnpv+pnl$shortnpv)*margin)
    }
    pnl

  }


MapToFutureTrades<-function(itrades,rollover=FALSE,tz="Asia/Kolkata"){
  itrades$entrymonth <- as.Date(sapply(itrades$entrytime, getExpiryDate), tz = tz,origin="1970-01-01")
  nextexpiry <- as.Date(sapply(as.Date(itrades$entrymonth + 20, tz = tz,,origin="1970-01-01"), getExpiryDate), tz = tz,,origin="1970-01-01")
  itrades$entrycontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(itrades$entrytime, tz = tz,origin="1970-01-01"),itrades$entrymonth) < 1,nextexpiry,itrades$entrymonth),tz = tz,origin="1970-01-01")
  itrades$exitmonth <- as.Date(sapply(itrades$exittime, getExpiryDate), tz = tz,origin="1970-01-01")
  nextexpiry <- as.Date(sapply(as.Date(itrades$exitmonth + 20, tz = tz,origin="1970-01-01"), getExpiryDate), tz = tz,origin="1970-01-01")
  itrades$exitcontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(itrades$exittime, tz = tz,origin="1970-01-01"),itrades$exitmonth) < 1,nextexpiry,itrades$exitmonth),tz = tz,origin="1970-01-01")

  tradesToBeRolledOver=data.frame()
  if(rollover){
    for (i in 1:nrow(itrades)) {
      if ((as.Date(itrades$exittime[i],tz="Asia/Kolkata")>itrades$entrycontractexpiry[i] & itrades$entrycontractexpiry[i]!=itrades$exitcontractexpiry[i])
          | (as.Date(itrades$exittime[i],tz="Asia/Kolkata")==itrades$entrycontractexpiry[i] & itrades$entrycontractexpiry[i]!=itrades$exitcontractexpiry[i] & itrades$exitreason[i]=="Open")
      ) {
        df.copy = itrades[i, ]
        df.copy$entrytime=as.POSIXct(format(itrades$entrycontractexpiry[i]),tz="Asia/Kolkata")
        df.copy$entrycontractexpiry=itrades$exitcontractexpiry[i]
        itrades$exittime[i]=as.POSIXct(format(itrades$entrycontractexpiry[i]),tz="Asia/Kolkata")
        itrades$exitreason[i]="Rollover"
        tradesToBeRolledOver=rbind(tradesToBeRolledOver,df.copy)
      }
    }
    itrades=rbind(itrades,tradesToBeRolledOver)
  }

  # Substitute contract
  expiry = format(itrades[, c("entrycontractexpiry")], format = "%Y%m%d")
  itrades$shortname=sapply(strsplit(itrades$symbol,"_"),"[",1)
  itrades$symbol = paste(itrades$shortname, "FUT", expiry, "", "", sep = "_")

  # Substitute Price Array
  for(i in 1:nrow(itrades)){
    itrades$entryprice[i]=MarkToModelPrice(itrades$symbol[i],itrades$entrytime[i],itrades$entryprice[i])
    itrades$exitprice[i]=MarkToModelPrice(itrades$symbol[i],itrades$exittime[i],itrades$exitprice[i])
  }
  itrades[order(itrades$entrytime),]
}

# futureTradePrice<-function(futureSymbol,tradedate,underlyingtradeprice){
#   # Return unadjusted futureprice for the tradedate
#   underlying=sapply(strsplit(futureSymbol[1],"_"),"[",1)
#   expiry=sapply(strsplit(futureSymbol[1],"_"),"[",3)
#   md=loadUnderlyingSymbol(futureSymbol[1],days=1000000)
#   if(!("splitadjust" %in% names(md))){
#     md$splitadjust=1
#   }
#   md<-unique(md)
#   underlyingprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#   if(nrow(underlyingprice)==0){
#     indicesForUnderlyingPrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
#     underlyingprice=md[last(indicesForUnderlyingPrice,1),]
#   }
#   adjustment=0
#   md=loadSymbol(futureSymbol[1],days=1000000)
#   md<-unique(md)
#   futureprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#   if(nrow(futureprice)==0){
#     indicesForFuturePrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
#     futureprice=md[last(indicesForFuturePrice,1),]
#   }
#
#   if(nrow(futureprice)==1 & nrow(underlyingprice)==1){
#       adjustment=futureprice$settle-underlyingprice$settle
#       return (underlyingtradeprice+adjustment)
#   }else{
#     print(paste("Future price not found for symbol",futureSymbol, "for date",tradedate,sep=" "))
#     return (0)
#   }
#
# }

MapToOptionTradesLO<-function(itrades,rollover=FALSE,tz="Asia/Kolkata",sourceInstrument="CASH",underlying="FUT",...){
  itrades$entrymonth <- as.Date(sapply(itrades$entrytime, getExpiryDate), tz = tz,origin="1970-01-01")
  nextexpiry <- as.Date(sapply(as.Date(itrades$entrymonth + 20, tz = tz,,origin="1970-01-01"), getExpiryDate), tz = tz,,origin="1970-01-01")
  itrades$entrycontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(itrades$entrytime, tz = tz,origin="1970-01-01"),itrades$entrymonth) < 1,nextexpiry,itrades$entrymonth),tz = tz,origin="1970-01-01")
  itrades$exitmonth <- as.Date(sapply(itrades$exittime, getExpiryDate), tz = tz,origin="1970-01-01")
  nextexpiry <- as.Date(sapply(as.Date(itrades$exitmonth + 20, tz = tz,origin="1970-01-01"), getExpiryDate), tz = tz,origin="1970-01-01")
  itrades$exitcontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(itrades$exittime, tz = tz,origin="1970-01-01"),itrades$exitmonth) < 1,nextexpiry,itrades$exitmonth),tz = tz,origin="1970-01-01")
  itrades<-getStrikeByClosestSettlePrice(itrades,tz,sourceInstrument,underlying)
  tradesToBeRolledOver=data.frame()
  if(rollover){
    for (i in 1:nrow(itrades)) {
      if ((as.Date(itrades$exittime[i],tz=tz)>itrades$entrycontractexpiry[i] & itrades$entrycontractexpiry[i]!=itrades$exitcontractexpiry[i])
          | (as.Date(itrades$exittime[i],tz=tz)==itrades$entrycontractexpiry[i] & itrades$entrycontractexpiry[i]!=itrades$exitcontractexpiry[i] & itrades$exitreason[i]=="Open")
      ) {
        df.copy = itrades[i, ]
        df.copy$entrytime=as.POSIXct(format(itrades$entrycontractexpiry[i]),tz="Asia/Kolkata")
        df.copy$entrycontractexpiry=itrades$exitcontractexpiry[i]
        df.copy=getStrikeByClosestSettlePrice(df.copy,tz,sourceInstrument,underlying)
        itrades$exittime[i]=as.POSIXct(format(itrades$entrycontractexpiry[i]),tz="Asia/Kolkata")
        itrades$exitreason[i]="Rollover"
        tradesToBeRolledOver=rbind(tradesToBeRolledOver,df.copy)
      }
    }
    itrades=rbind(itrades,tradesToBeRolledOver)
  }

  # Substitute contract
  expiry = format(itrades[, c("entrycontractexpiry")], format = "%Y%m%d")
  itrades$shortname=sapply(strsplit(itrades$symbol,"_"),"[",1)
  itrades$symbol = paste(itrades$shortname, "OPT", expiry, ifelse(itrades$trade=="BUY","CALL","PUT"), itrades$strike, sep = "_")

  # Substitute Price Array
  for(i in 1:nrow(itrades)){
    #itrades$entryprice[i]=MarkToModelPrice(itrades$symbol[i],itrades$entrytime[i],itrades$entryprice[i],underlying="FUT")
    #itrades$exitprice[i]=ifelse(itrades$exitprice[i]>0,MarkToModelPrice(itrades$symbol[i],itrades$exittime[i],itrades$exitprice[i],underlying="FUT"),0)
    itrades$entryprice[i]=MarkToModelPrice(itrades$symbol[i],itrades$entrytime[i],itrades$entryprice[i],underlying=underlying,sourceInstrument=sourceInstrument)
    itrades$exitprice[i]=ifelse(itrades$exitprice[i]>0,MarkToModelPrice(itrades$symbol[i],itrades$exittime[i],itrades$exitprice[i],underlying=underlying,sourceInstrument=sourceInstrument),0)
  }
  itrades$trade="BUY"
  itrades[order(itrades$entrytime),]

}

MarkToModelPrice<-function(symbol,tradedate,underlyingtradeprice,underlying="CASH",sourceInstrument="CASH",closetime="15:30:00",ticksize=0.05){
  symbolvector=unlist(strsplit(symbol,"_"))
  expiry=symbolvector[3]
  md=loadUnderlyingSymbol(symbol,underlyingType=underlying,days=1000000)
  underlyingprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
  if(nrow(underlyingprice)==0){
    indicesForUnderlyingPrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
    underlyingprice=md[last(indicesForUnderlyingPrice,1),]
  }
  if(nrow(underlyingprice)==0){
    return (NA_real_)
  }
  if(symbolvector[2]=="OPT"){
    voldate=tradedate
    md=loadSymbol(symbol,days=1000000)
    optionprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
    if(nrow(optionprice)==0){
      indicesForOptionPrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
      optionprice=md[last(indicesForOptionPrice,1),]
      voldate=optionprice$date
      voldate=as.POSIXct(strftime(voldate,format="%Y-%m-%d"))
      md=loadUnderlyingSymbol(symbol,underlyingType=underlying,days=1000000)
      underlyingprice=md[md$date==as.POSIXct(strftime(voldate,format="%Y-%m-%d")),]
    }
    if(nrow(optionprice)==0 | nrow(underlyingprice)==0){
      return (-1)
    }
    vol=0
    ExpiryFormattedDate=as.POSIXct(paste(strftime(as.POSIXct(symbolvector[3],format="%Y%m%d")),closetime))
    if(as.numeric(format(tradedate, "%H"))==0 && as.numeric(format(tradedate, "%M"))==0 && as.numeric(format(tradedate, "%S"))==0 ){
      tradedate=as.POSIXct(paste(strftime(tradedate),closetime))
    }
    voldate=as.POSIXct(paste(strftime(voldate),closetime))
    ytmforvolcalc=as.numeric(difftime(ExpiryFormattedDate,voldate,units=c("days")))/365
    ytm=as.numeric(difftime(ExpiryFormattedDate,tradedate,units=c("days")))/365
    if(ytm==0){
      if(symbolvector[4]=="CALL"){
        return(max(0,underlyingprice$asettle-as.numeric(symbolvector[5])))
      }else{
        return(max(0,as.numeric(symbolvector[5])-underlyingprice$asettle))
      }
    }
    if(ytmforvolcalc==0 ){
      # we have option expiry price. Recalculate vol for prior date
      md=loadUnderlyingSymbol(symbol,underlyingType=underlying,days=1000000)
      underlyingprice=md[md$date<as.POSIXct(strftime(voldate,format="%Y-%m-%d")),]
      underlyingprice=last(underlyingprice)
      md=loadSymbol(symbol,days=1000000,realtime=FALSE)
      optionprice=md[md$date<as.POSIXct(strftime(voldate,format="%Y-%m-%d")),]
      optionprice=last(optionprice)
      voldate=as.POSIXct(paste(strftime(optionprice$date),closetime))
      ytmforvolcalc=as.numeric(difftime(ExpiryFormattedDate,voldate,units=c("days")))/365
    }
    vol=tryCatch( {EuropeanOptionImpliedVolatility(tolower(symbolvector[4]),value=optionprice$asettle,underlying=underlyingprice$asettle,strike=as.numeric(symbolvector[5]),dividendYield=0.01,riskFreeRate=0.065,maturity=ytmforvolcalc,volatility=0.1)},error=function(err){0.01})
    if(sourceInstrument=="CASH" && underlying!="CASH"){
      futureSymbol=paste(symbolvector[1],"_FUT_",symbolvector[3],"__",sep="")
      underlyingtradeprice=MarkToModelPrice(futureSymbol,tradedate,underlyingtradeprice)
    }
    out=EuropeanOption(tolower(symbolvector[4]),underlying=underlyingtradeprice,strike=as.numeric(symbolvector[5]),dividendYield=0.01,riskFreeRate=0.065,maturity=ytm,volatility=vol)$value
    # round
    out=round(out/ticksize)*ticksize
    return (out)
  }
  else if(symbolvector[2]=="FUT"){
    # Return unadjusted futureprice for the tradedate
    md=loadSymbol(symbol,days=1000000)
    futureprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
    if(nrow(futureprice)==0){
      indicesForFuturePrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
      futureprice=md[last(indicesForFuturePrice,1),]
      voldate=futureprice$date
      voldate=as.POSIXct(strftime(voldate,format="%Y-%m-%d"))
      md=loadUnderlyingSymbol(symbol,underlyingType=underlying,days=1000000)
      underlyingprice=md[md$date==as.POSIXct(strftime(voldate,format="%Y-%m-%d")),]
    }
    if(nrow(futureprice)==0 | nrow(underlyingprice)==0){
      return (NA_real_)
    }
    adjustment=0
    if(nrow(futureprice)==1 & nrow(underlyingprice)==1){
      adjustment=futureprice$settle-underlyingprice$settle
      return (underlyingtradeprice+adjustment)
    }else{
      print(paste("Future price not found for symbol",futureSymbol, "for date",tradedate,sep=" "))
      return (NA_real_)
    }
  }
  else if (symbolvector[2]=="STK" ||symbolvector[2]=="IND"){
    return (underlyingprice$asettle)
  }
}


# optionTradePrice<-function(optionSymbol,tradedate,underlyingtradeprice,closetime="15:30:00",underlying="CASH"){
#   # Return unadjusted futureprice for the tradedate
#   expiry=sapply(strsplit(optionSymbol[1],"_"),"[",3)
#   md=loadUnderlyingSymbol(optionSymbol[1],underlyingType=underlying,days=1000000)
#   if(!("splitadjust" %in% names(md))){
#     md$splitadjust=1
#   }
#   md<-unique(md)
#   underlyingprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#   if(nrow(underlyingprice)==0){
#     indicesForUnderlyingPrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
#     underlyingprice=md[last(indicesForUnderlyingPrice,1),]
#   }
#   adjustment=0
#   md=loadSymbol(optionSymbol[1],days=1000000,realtime=FALSE)
#   md<-unique(md)
#   vectorOptionSymbol=unlist(strsplit(optionSymbol, "_"))
#   optionprice=md[md$date==as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#   if(nrow(optionprice)==0){
#     indicesForOptionPrice=which(md$date<=as.POSIXct(strftime(tradedate,format="%Y-%m-%d")))
#     optionprice=md[last(indicesForOptionPrice,1),]
#   }
#   vol=0
#   if(nrow(optionprice)==1){
#     ExpiryFormattedDate=as.POSIXct(paste(strftime(as.POSIXct(vectorOptionSymbol[3],format="%Y%m%d")),closetime))
#     if(as.numeric(format(tradedate, "%H"))==0 && as.numeric(format(tradedate, "%M"))==0 && as.numeric(format(tradedate, "%S"))==0 ){
#       tradedate=as.POSIXct(paste(strftime(tradedate),closetime))
#     }
#     voldate=as.POSIXct(paste(strftime(optionprice$date),closetime))
#     ytmforvolcalc=as.numeric(difftime(ExpiryFormattedDate,voldate,units=c("days")))/365
#     ytm=as.numeric(difftime(ExpiryFormattedDate,tradedate,units=c("days")))/365
#     if(ytmforvolcalc==0 & ytm>0){
#       # we have option expiry price. Recalculate vol for prior date
#       md=loadUnderlyingSymbol(optionSymbol[1],underlyingType=underlying,days=1000000)
#       md<-unique(md)
#       underlyingprice=md[md$date<as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#       underlyingprice=last(underlyingprice)
#       md=loadSymbol(optionSymbol[1],days=1000000,realtime=FALSE)
#       md<-unique(md)
#       optionprice=md[md$date<as.POSIXct(strftime(tradedate,format="%Y-%m-%d")),]
#       optionprice=last(optionprice)
#       voldate=as.POSIXct(paste(strftime(optionprice$date),closetime))
#       ytmforvolcalc=as.numeric(difftime(ExpiryFormattedDate,voldate,units=c("days")))/365
#     }
#     if(ytm>0){
#       vol=tryCatch( {EuropeanOptionImpliedVolatility(tolower(vectorOptionSymbol[4]),value=optionprice$asettle,underlying=underlyingprice$asettle,strike=as.numeric(vectorOptionSymbol[5]),dividendYield=0.01,riskFreeRate=0.065,maturity=ytmforvolcalc,volatility=0.1)},error=function(err){0.01})
#     }
#   }
#   if(vol>0){
#     return (EuropeanOption(tolower(vectorOptionSymbol[4]),underlying=underlyingtradeprice,strike=as.numeric(vectorOptionSymbol[5]),dividendYield=0.01,riskFreeRate=0.065,maturity=ytm,volatility=vol)$value)
#   }else if(vol==0 && ytm==0 && nrow(optionprice)==1){
#     # we are on expiry
#     return (optionprice$asettle)
#     }else{
#     return (NA_real_)
#   }
#
# }


# getmtm<-function(symbol,date=NULL,realtime=FALSE,redisdb=9,tz="Asia/Kolkata"){
#   if(realtime){
#     rredis::redisConnect()
#     rredis::redisSelect(as.numeric(redisdb))
#     key=paste(symbol,"tick","close",sep=":")
#     json=rredis::redisZRange(key,-1,-1)
#     if(!is.null(json)){
#       json=jsonlite::fromJSON(as.character(json))
#       json$time=as.POSIXct(json$time/1000,tz=tz,origin="1970-01-01")
#       json$value=as.numeric(json$value)
#     }
#     return(json)
#   }else{
#     md=loadSymbol(symbol,cutoff=date,days=365)
#     mtm=list()
#     mtm$value=last(md[md$date<=date,c("asettle")])
#     mtm$time=last(md[md$date<=date,c("date")])
#     return(mtm)
#   }
# }

getmtm<-function(symbol,date=Sys.time(),realtime=FALSE,notfound=-0.01){
  md=loadSymbol(symbol,cutoff=date,days=365,realtime=realtime)
  mtm=list()
  if(nrow(md)>0){
    mtm$value=last(md[md$date<=date,c("asettle")])
    mtm$time=last(md[md$date<=date,c("date")])
  }else{
    mtm$value=notfound
    mtm$time=as.POSIXct(as.character(Sys.Date()))
  }
  return(mtm)
}

revalPortfolio<-function(trades,kBrokerage,realtime=FALSE,allocation=1){
  indicesToMTM=which(trades$exitreason=="Open" | (is.na(trades$exitprice) & !is.na(trades$entryprice)))
  for( i in seq_along(indicesToMTM)){
    index=indicesToMTM[i]
    symbol=trades$symbol[index]
    mtm=getmtm(symbol,realtime=realtime)
    if(!is.null(mtm)){
      trades$exittime[index]=mtm$time
      trades$exitprice[index]=mtm$value
    }
  }
  trades$size=round(trades$size*allocation)
  trades$brokerage=calculateBrokerage(trades,kBrokerage)
  trades$pnl<-ifelse(trades$exitprice==0|trades$entryprice==0,0,(trades$exitprice-trades$entryprice)*trades$size-trades$brokerage)
  trades$pnl<-ifelse(trades$trade=="BUY",trades$pnl,-trades$pnl)
  trades
}

sharpe <- function(returns, risk.free.rate = 0.07) {
  sqrt(252) * (mean(returns) - (risk.free.rate / 365)) / sd(returns)
}

getAllStrikesForExpiry <- function(derivSymbol) {
  datafolder=paste(datafolder,"daily/opt/",sep="")
  symbolsvector=unlist(strsplit(derivSymbol,"_"))
  potentialSymbols = list.files(
    paste(datafolder, symbolsvector[3], sep = ""),
    pattern = paste(symbolsvector[1], "_OPT", sep = "")
  )
  b <- lapply(potentialSymbols, strsplit, split = "_")
  c <- sapply(b, tail, n = 1L)
  d <- sapply(c, tail, n = 1L)
  e <- sapply(d, strsplit, "\\.rds")
  #as.numeric(sapply(e,function(x){x[length(x)-1]}))
  e<-as.numeric(e)
  if(symbolsvector[1]=="NSENIFTY"){
    inclusion=which(e%%100==0)
    e<-e[inclusion]
  }
  e
}

getClosestStrike <-
  function(tradedate,
           futureSymbol,
           underlyingprice,
           sourceInstrument,
           tz="Asia/Kolkata") {
    # date is posixct
    # expiry is string %Y%m%d
    # underlyingshortname is just the symbol short name like RELIANCE
    symbolsvector=unlist(strsplit(futureSymbol,"_"))
    date=strftime(tradedate,format="%Y%m%d",tz=tz)
    folder=paste(datafolder,"static/strikes/",sep="")
    strikeFile=paste(folder,date,"_strikes.rds",sep="")
    strikes=readRDS2(strikeFile)
    if(nrow(strikes)==0){
      # get last available strikes for date.
      files=list.files(folder,pattern="*.rds")
      files.date=sapply(strsplit(files,"_"),"[",1)
      index=last(which(files.date<=date))
      strikefile=paste(folder,files[index],sep="")
      strikes=readRDS2(strikefile)
    }
    strikes=filter(strikes,SYMBOL==symbolsvector[1],EXPIRY_DT==symbolsvector[3])$allstrikes
    strikes=as.numeric(unlist(strsplit(strikes,",")))
    strikes = strikes[complete.cases(strikes)]
    if(symbolsvector[1]=="NSENIFTY"){
      inclusion=which(strikes%%100==0)
      strikes<-strikes[inclusion]
    }
    if(sourceInstrument=="CASH"){
      price=MarkToModelPrice(futureSymbol,tradedate,underlyingprice)
    }else{
      price=underlyingprice
    }
    if (price > 0) {
      strikeIndex <-
        sapply(price, function(x, s) {
          which.min(abs(s - x))
        }, s = strikes)
      out = strikes[strikeIndex]
    } else{
      out = NA_real_
    }
    out
  }

getClosestStrikeUniverse <-
  function(dfsignals,timeZone) {
    dfsignals$strike = NA_real_
    for (i in 1:nrow(dfsignals)) {
      if (dfsignals$buy[i] > 0 ||
          dfsignals$sell[i] > 0 || dfsignals$short[i] > 0 ||
          dfsignals$cover[i] > 0) {
        symbolsvector = unlist(strsplit(dfsignals$symbol[i], "_"))
        dfsignals$strike[i] = getClosestStrike(dfsignals$date[i],symbolsvector[1],strftime(dfsignals$entrycontractexpiry[i],"%Y%m%d",tz = timeZone))
      }
    }
    dfsignals
    #dfsignals[!is.na(dfsignals$strike),]

  }

getStrikeByClosestSettlePrice <- function(itrades,timeZone,sourceInstrument="CASH",underlying="FUT",realtime=FALSE,tz="Asia/Kolkata") {
  itrades$strike = NA_real_
  for (i in 1:nrow(itrades)) {
    symbolsvector = unlist(strsplit(itrades$symbol[i], "_"))
    if(symbolsvector[2]=="OPT"){
      itrades$strike[i]=symbolsvector[5]
    }else{
      expiry=strftime(itrades$entrycontractexpiry[i],"%Y%m%d",tz = timeZone)
      futureSymbol=paste(symbolsvector[1],"_FUT_",expiry,"__",sep="")
      itrades$strike[i] = getClosestStrike(itrades$entrytime[i],futureSymbol,itrades$entryprice[i],sourceInstrument,tz=tz)
    }
  }
  itrades
}

getMaxOIStrike <-
  function(dates,
           underlyingshortname,
           expiry,
           right) {
    strikesUniverse <-
      getAllStrikesForExpiry(underlyingshortname, expiry)
    strikesUniverse = strikesUniverse[complete.cases(strikesUniverse)]
    oi <- numeric(length = length(dates))
    strike <- numeric(length = length(dates))
    symbols = paste(
      underlyingshortname,
      "_OPT_",
      expiry,
      "_",
      right,
      "_",
      strikesUniverse,
      ".Rdata",
      sep = ""
    )
    for (i in 1:length(dates)) {
      cdate = dates[i]
      tempoi <- numeric(length = length(strikesUniverse))
      for (s in 1:length(strikesUniverse)) {
        md=loadSymbol(symbols[s])
        index = which(md$date == cdate)
        if (length(index) == 1) {
          tempoi[s] = md[index, 'oi']
        }
      }
      index = which.max(tempoi)
      oi[i] = tempoi[index]
      strike[i] = strikesUniverse[index]
    }
    data.frame(
      date = as.POSIXct(dates, tz = "Asia/Kolkata"),
      strike = strike,
      oi = oi
    )
    strike
  }


QuickChart<-function(symbol,...){
  md<-loadSymbol(symbol,...)
  if(nrow(md)==0){
    type=tolower(strsplit(symbol,"_")[[1]][2])
    path=paste(datafolder,"daily/",type,sep="")
    symbols=sapply(strsplit(list.files(path),"\\."),'[',1)
    symbols=sapply(strsplit(symbols,"_"),'[',1)
    shortlist=symbols[grep(substr(symbol,1,5),symbols)]
    if(length(shortlist)>0){
      print(paste("Did you mean..",paste(shortlist,collapse=" or "),sep=" "))
    }
    return(md)
  }
  symbolname=NULL
  dtindex=which(names(md)=="date")
  if(length(grep("aopen",names(md)))>0){
    symbolname = convertToXTS(md, c("aopen", "ahigh", "alow", "asettle", "avolume"),dateIndex = dtindex)
  }
  customTheme = chartTheme(
    "white",
    up.col = "dark green",
    dn.col = "dark red",
    main.col = "#000000",
    sub.col = "#000000",
    border = "#000000",
    dn.up.col = "dark red",
    up.up.col = "dark green",
    dn.dn.col = "dark red",
    up.dn.col = "dark green",
    up.border = "#000000",
    dn.border = "#000000",
    dn.up.border = "#000000",
    up.up.border = "#000000",
    dn.dn.border = "#000000",
    up.dn.border = "#000000"
  )
  names(symbolname)<-c("open","high","low","close","volume")
  name<-as.character(md$symbol[1])
  start=strftime(md$date[1],format="%Y-%m-%d")
  end=strftime(md$date[nrow(md)],format="%Y-%m-%d")
  out<-symbolname[paste(start, "::", end, sep = "")]
  if(!is.null(out)){
    out.md<-convertToDF(out)
    trend.md<-RTrade::Trend(out.md$date,out.md$high,out.md$low,out.md$close)
    swinglevel<<-xts(trend.md$swinglevel,out.md$date)
    # library(pryr)
    #   print(where("swinglevel"))
    #    print(where("chartSeries"))
    #    plot(addTA(swinglevel,on=1, type='s',lty=3))
    trend<<-xts(trend.md$trend,out.md$date)
    #    plot(addTA(trend,type='s'))
    chartSeries(symbolname,TA=list("addTA(swinglevel,on=1,type='s',lty=3)","addTA(trend,type='s')"),
                subset = paste(start, "::", end, sep = ""),
                theme = customTheme,
                name=name,...)
    md<-out.md
  }
}

changeTimeFrame<-function(md,src=NULL, dest=NULL){
  names<-as.character()
  data<-xts()
  index<-as.integer()
  out<-data.frame()
  if(!is.null(nrow(md)) & !is.null(src) & !is.null(dest)){
    if(src=="daily"){
      if(dest=="weekly"){
        names<-names(md)
        names<-names[!names %in% c("symbol","date")]
        data<-convertToXTS(md,names)
        fridays = as.POSIXlt(time(data))$wday == 5
        indx <- c(0,which(fridays),nrow(data))
      }else if(dest=="monthly"){
        names<-names(md)
        names<-names[!names %in% c("symbol","date")]
        data<-convertToXTS(md,names)
        months = as.POSIXlt(time(data))$mon
        indx <- c(0,which(diff(months)!=0),nrow(data))
      }
    }else if(src=="persecond"){
      k=gsub("[^[:digit:]]", "", dest)
      if(k!=""){
        symbol=md$symbol[nrow(md)]
        period=gsub("[[:digit:]]", "", dest)
        mdseries=convertToXTS(md[,c("date","close")])
        mdseries=mdseries["T09:15/T15:30"]
        if(!is.null(mdseries)){
          mdtoday=to.period(mdseries,period="minutes",k=k,indexAt = "startof")
          mdtoday=convertToDF(mdtoday)
          rownames(mdtoday)=c()
          names(mdtoday)=c("open","high","low","close","date")
          mdtoday$symbol=symbol
          mdtoday$volume=0
          md=mdtoday
          md$date=lubridate::floor_date(md$date, paste(k,period))
        }
      }
    }

    if(length(names)>0){
      for(i in seq_along(names)){
        if(grepl("open",names[i])||grepl("aopen",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=first))
        }else
          if(grepl("high",names[i])||grepl("ahigh",names[i])){
            assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=max))
          }else
            if(grepl("low",names[i])||grepl("alow",names[i])){
              assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=min))
            }else
              if(grepl("volume",names[i])||grepl("delivered",names[i])||grepl("avolume",names[i])){
                assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=sum))
              }else{
                assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=last))
              }
      }
    }

    if(length(names)>=2){
      out<-get(names[1])
      for(i in 2:length(names)){
        out<- merge (out,get(names[i]))
      }
    }

  }
  if(nrow(out)>0){
    out<-RTrade::convertToDF(out)
    out$symbol<-md$symbol[1]
    rownames(out)<-NULL
    dateindex=ncol(out)-1
    out<-out[,c(dateindex,seq(1:(dateindex-1)),dateindex+1)]
    return(out)
  }else{
    return(md)
  }
}


loadSymbol<-function(symbol,realtime=FALSE,cutoff=NULL,src="daily",dest="daily",days=365,tz="Asia/Kolkata",...){
  if(is.null(cutoff)){
    cutoff=Sys.time()
  }else{
    cutoff=as.POSIXct(cutoff,tz=tz)
  }
  if(days==365 && src=="persecond"){
    days=3
  }
  if(src=="persecond" && dest=="daily"){
    dest="5minutes"
  }

  if(dest=="weekly"){
    starttime=cutoff-7*days*24*60*60
  }else if(dest=="monthly"){
    starttime=cutoff-30*days*24*60*60
  }else{
    starttime=cutoff-days*24*60*60
  }
  md=readRDS3(symbol,starttime,cutoff,src) # md in src duration
  # add realtime data if needed
  symbolsvector = unlist(strsplit(symbol, "_"))
  type=toupper(symbolsvector[2])
  if(nrow(md)>0 && !is.na(type) && src=="daily"){
    if(realtime){
      realtimestart=as.POSIXct(ifelse(nrow(md)>0,last(md$date)+1*24*60*60,Sys.time()-1*24*60*60),origin="1970-01-01",tz=tz)
      newrow=getRealTimeData(symbol,realtimestart,src,tz)
      md <- rbind(md, newrow)
      # update splitinformation
      splitinfo=getSplitInfo(symbolsvector[1])
      if(nrow(splitinfo)>0){
        if("splitadjust" %in% names(md)){
          md=subset(md,select= -splitadjust)
        }
        splitinfo$splitadjust=splitinfo$newshares/splitinfo$oldshares
        splitinfo=aggregate(splitadjust~date,splitinfo,prod)
        splitinfo=splitinfo[rev(order(splitinfo$date)),]
        md$dateonly=as.Date(md$date,tz="Asia/Kolkata")
        splitinfo$date=as.Date(splitinfo$date,tz="Asia/Kolkata")
        splitinfo$splitadjust=cumprod(splitinfo$splitadjust)
        md=merge(md,splitinfo[,c("date","splitadjust")],by.x=c("dateonly"),by.y=c("date"),all.x = TRUE)
        md$splitadjust=Ref(md$splitadjust,1)
        md$splitadjust=ifelse(!is.na(md$splitadjust) & !is.na(Ref(md$splitadjust,-1)),NA_real_,md$splitadjust)
        md$splitadjust=na.locf(md$splitadjust,na.rm = FALSE,fromLast = TRUE)
        lastsplitadjust=last(splitinfo[splitinfo$date>last(md$dateonly),"splitadjust"])
        if(is.na(lastsplitadjust)||length(lastsplitadjust)==0){
          lastsplitadjust=1
        }
        md=md[ , -which(names(md) %in% c("dateonly"))]
        md$splitadjust=ifelse(is.na(md$splitadjust),lastsplitadjust,md$splitadjust)
      }else{
        md$splitadjust=1
      }
    }

    if(!("splitadjust" %in% names(md))){
      md$splitadjust=1
    }
    md$aopen=md$open/md$splitadjust
    md$ahigh=md$high/md$splitadjust
    md$alow=md$low/md$splitadjust
    md$aclose=md$close/md$splitadjust
    md$asettle=md$settle/md$splitadjust
    md$avolume=md$volume*md$splitadjust

    # convert md to destination duration
    md<-changeTimeFrame(md,src,dest)
    rownames(md)=NULL
  }

  if(nrow(md)>0 && !is.na(type) && src=="persecond"){
    if(realtime){
      realtimestart=as.POSIXct(ifelse(nrow(md)>0,last(md$date)+1,Sys.time()-1*24*60*60),origin="1970-01-01",tz=tz)
      newrow=getRealTimeData(symbol,realtimestart,src,tz)
      md <- rbind(md, newrow)
    }
    # convert md to destination duration
    md<-changeTimeFrame(md,src,dest)
    # update splitinformation
    splitinfo=getSplitInfo(symbolsvector[1])
    if(nrow(splitinfo)>0){
      if("splitadjust" %in% names(md)){
        md=subset(md,select= -splitadjust)
      }
      splitinfo$splitadjust=splitinfo$newshares/splitinfo$oldshares
      splitinfo=aggregate(splitadjust~date,splitinfo,prod)
      splitinfo=splitinfo[rev(order(splitinfo$date)),]
      md$dateonly=as.Date(md$date,tz="Asia/Kolkata")
      splitinfo$date=as.Date(splitinfo$date,tz="Asia/Kolkata")
      splitinfo$splitadjust=cumprod(splitinfo$splitadjust)
      md=merge(md,splitinfo[,c("date","splitadjust")],by.x=c("dateonly"),by.y=c("date"),all.x = TRUE)
      md$splitadjust=Ref(md$splitadjust,1)
      md$splitadjust=ifelse(!is.na(md$splitadjust) & !is.na(Ref(md$splitadjust,-1)),NA_real_,md$splitadjust)
      md$splitadjust=na.locf(md$splitadjust,na.rm = FALSE,fromLast = TRUE)
      lastsplitadjust=last(splitinfo[splitinfo$date>last(md$dateonly),"splitadjust"])
      if(length(lastsplitadjust)==0 || is.na(lastsplitadjust)){
        lastsplitadjust=1
      }
      md=md[ , -which(names(md) %in% c("dateonly"))]
      md$splitadjust=ifelse(is.na(md$splitadjust),lastsplitadjust,md$splitadjust)
    }else{
      md$splitadjust=1
    }

    md$settle=md$close
    md$aopen=md$open/md$splitadjust
    md$ahigh=md$high/md$splitadjust
    md$alow=md$low/md$splitadjust
    md$aclose=md$close/md$splitadjust
    md$asettle=md$settle/md$splitadjust
    md$avolume=md$volume*md$splitadjust
    rownames(md)=NULL
  }

  if(nrow(md)==0){
    type=tolower(strsplit(symbol,"_")[[1]][2])
    path=paste(datafolder,"daily/",type,sep="")
    symbols=sapply(strsplit(list.files(path),"\\."),'[',1)
    symbols=sapply(strsplit(symbols,"_"),'[',1)
    shortlist=symbols[grep(substr(symbol,1,5),symbols)]
    if(length(shortlist)>0){
      print(paste("Did you mean..",paste(shortlist,collapse=" or "),sep=" "))
    }
    return(md)
  }
  md[md$date<=as.POSIXct(cutoff,tz=tz),]

}

loadUnderlyingSymbol<-function(symbol,underlyingType="CASH",...){
  if(underlyingType=="CASH"){
    symbolsvector=unlist(strsplit(symbol,"_"))
    symbolsvector[3]=""
    symbolsvector[4]=""
    symbolsvector[5]=""
    if(grepl("NIFTY",symbolsvector[1])){
      symbolsvector[2]="IND"
    }else{
      symbolsvector[2]="STK"
    }
    underlying=paste(symbolsvector,collapse="_")
    loadSymbol(underlying,...)
  }else{
    symbolsvector=unlist(strsplit(symbol,"_"))
    symbolsvector[2]="FUT"
    symbolsvector[4]=""
    symbolsvector[5]=""
    underlying=paste(symbolsvector,collapse="_")
    loadSymbol(underlying,...)
  }

}

getRealTimeData<-function(symbol,realtimestart,bar="daily",tz="Asia/Kolkata"){
  newrow=data.frame()
  today=strftime(Sys.Date(),tz=tz,format="%Y-%m-%d")
  start=strftime(realtimestart,tz=tz,format="%Y-%m-%d %H:%M:%S")
  symbolsvector = unlist(strsplit(symbol, "_"))
  type=toupper(symbolsvector[2])
  if(!is.na(type)){
    if(bar=="daily"){
      newrow=getPriceArrayFromRedis(9,symbol,"tick","close",start,paste(today, "15:30:00"),tz)
      if(nrow(newrow)==1){
        if(type=="STK"){
          newrow <-
            data.frame(
              "date" = newrow$date[1],
              "open" = newrow$open[1],
              "high" = newrow$high[1],
              "low" = newrow$low[1],
              "close" = newrow$close[1],
              "settle" = newrow$close[1],
              "volume" = 0,
              "tradecount"=0,
              "delivered"=0,
              "tradedvalue"=0,
              "symbol" = symbol,
              "splitadjust" = 1,
              stringsAsFactors = FALSE
            )

        }

        if(type=="IND"){
          newrow <-
            data.frame(
              "date" = newrow$date[1],
              "open" = newrow$open[1],
              "high" = newrow$high[1],
              "low" = newrow$low[1],
              "close" = newrow$close[1],
              "settle" = newrow$close[1],
              "volume" = 0,
              "tradedvalue"=0,
              "pe"=0,
              "pb"=0,
              "dividendyield"=0,
              "symbol" = symbol
            )
        }

        if(type=="FUT" ||type=="OPT"){
          newrow <-
            data.frame(
              "date" = newrow$date[1],
              "open" = newrow$open[1],
              "high" = newrow$high[1],
              "low" = newrow$low[1],
              "close" = newrow$close[1],
              "settle" = newrow$close[1],
              "volume" = 0,
              "oi"=0,
              "tradevalue"=0,
              "symbol" = symbol,
              stringsAsFactors = FALSE
            )
        }
      }
    }else{
      mdtodayseries=getPriceHistoryFromRedis(9,symbol,"tick","close",start,paste(today, "15:30:00"))
      if(nrow(mdtodayseries)>0){
        newrow <-
          data.frame(
            "date" = mdtodayseries$date,
            "open" = mdtodayseries$value,
            "high" = mdtodayseries$value,
            "low" = mdtodayseries$value,
            "close" = mdtodayseries$value,
            "volume" = 0,
            "symbol" = symbol,
            stringsAsFactors = FALSE
          )
      }
    }
  }

  newrow
}


getTickDataToDF<-function(symbol,date=NULL,redisdb=9,tz="Asia/Kolkata"){
  if(is.null(date)){
    date=as.character(Sys.Date())
  }
  s=symbol
  targetfile=paste(datafolder,"tick","/",date,"/",symbol,"_",date,".rds",sep="")
  if(file.exists(targetfile)){
    return(readRDS(targetfile))
  }
  starttime=paste(date,"09:05:00")
  endtime=paste(date,"15:35:00")
  starttime=as.numeric(as.POSIXct(starttime,origin="1970-01-01",tz=tz))*1000
  endtime=as.numeric(as.POSIXct(endtime,origin="1970-01-01",tz=tz))*1000
  redisConnect()
  redisSelect(redisdb)
  keys=rredis::redisKeys(pattern=paste(symbol,"tick*",sep=":"))
  df=data.frame()
  for(k in keys){
    values=rredis::redisZRangeByScore(k,starttime,endtime)
    values=unlist(values)
    if(!is.null(values)){
      time=sapply(values,function(x) fromJSON(x)$time)/1000
      names(time)=NULL
      assign(strsplit(k,":")[[1]][3],unname(as.numeric(sapply(values,function(x) fromJSON(x)$value))))
      out=data.frame(date=time)
      out[strsplit(k,":")[[1]][3]]=get(strsplit(k,":")[[1]][3])
      assign(strsplit(k,":")[[1]][3],out)
      if(nrow(df)==0){
        df=get(strsplit(k,":")[[1]][3])
      }else{
        df=merge(df,get(strsplit(k,":")[[1]][3]),all.x = TRUE,all.y = TRUE)
      }
    }
  }
  redisClose()
  #df <- mutate_all(df, function(x) as.numeric(as.character(x)))
  if(nrow(df)>0){
    df$date=as.POSIXct(df$date,origin="1970-01-01",tz="Asia/Kolkata")
    df$symbol=symbol
  }
  df
}

readRDS3<-function(symbol,starttime,endtime,duration){
  md=data.frame()
  symbolsvector = unlist(strsplit(symbol, "_"))
  type=tolower(symbolsvector[2])
  folder=paste(datafolder,duration,"/",type,"/",sep="")
  if(duration=="persecond"){
    dates=list.dirs(folder,full.names = FALSE,recursive=FALSE)
    #    starttime_s=substring(strftime(starttime),1,10)
    #    endtime_s=substring(strftime(endtime),1,10)
    starttime_s=substring(strftime(starttime),1,10)
    endtime_s=strftime(endtime)
    dates=dates[dates>=starttime_s & dates<=endtime_s]
    for(d in seq_len(length(dates))){
      filename=paste(folder,dates[d],"/",symbol,"_",dates[d],".rds",sep="")
      md=rbind(md,readRDS2(filename))
    }
  }else{
    if(type=="fut"||type=="opt"){
      filename=paste(folder,symbolsvector[3],"/",symbol,".rds",sep="")
    }else{
      filename=paste(folder,symbol,".rds",sep="")
    }
    md=readRDS2(filename)
  }
  md=md[md$date>=starttime & md$date<=endtime,]
  md
}

readRDS2=function(filename){
  if(file.exists(filename)){
    readRDS(filename)
  }else{
    data.frame()
  }
}


getSplitInfo<-function(symbol="",complete=FALSE){
  splitinfo=readRDS(paste(datafolder,"static/splits.rds",sep=""))
  splitinfo$date=as.POSIXct(splitinfo$date,tz="Asia/Kolkata",format="%Y%m%d")

  if(symbol==""){
    return(splitinfo)
  }else{
    # a<-unlist(rredis::redisSMembers("symbolchange")) # get values from redis in a vector
    # origsymbols=sapply(strsplit(a,"_"),"[",2)
    # newsymbols=sapply(strsplit(a,"_"),"[",3)
    symbolchange=getSymbolChange()
    # linkedsymbols=RTrade::linkedsymbols(origsymbols,newsymbols,symbol)
    linkedsymbols=linkedsymbols(symbolchange,symbol,complete)$symbol
    linkedsymbols=paste("^",linkedsymbols,"$",sep="")
    indices=unlist(sapply(linkedsymbols,grep,splitinfo$symbol))
    splitinfo=splitinfo[indices,]
    if(nrow(splitinfo)>0){
      splitinfo=splitinfo[order(splitinfo$date),]
    }
  }
  splitinfo
}

getSymbolChange<-function(){
  symbolchange=readRDS(paste(datafolder,"static/symbolchange.rds",sep=""))
  symbolchange$effectivedate=as.POSIXct(symbolchange$effectivedate,tz="Asia/Kolkata",format="%Y%m%d")
  symbolchange
}

slope <- function (x,array=TRUE,period=252) {
  if(!array){
    pointslope(x)
  }else{
    value<-rollapply(x,period,pointslope)
    value=c(rep(0,(length(x)-length(value))),value)
    unname(value)

  }
}

r2 <- function (x,array=TRUE,period=252) {
  if(!array){
    pointr2(x)
  }else{
    value<-rollapply(x,period,pointr2)
    value<-c(rep(0,(length(x)-length(value))),value)
    unname(value)
  }
}

regress<-function(x,array=TRUE,period=252){
  if(!array){
    pointregress(x)
  }else{
    value<-rollapply(x,period,pointregress)
    value<-rbind(matrix(0,length(x)-nrow(value),2),value)
    unname(value)
  }
}

lmprediction <- function (x,array=TRUE,period=252) {
  if(!array){
    pointpredict(x)
  }else{
    value<-rollapply(x,period,pointpredict)
    value<-c(rep(0,(length(x)-length(value))),value)
    unname(value)
  }
}

pointslope<-function(x){
  res <- (lm(log(x) ~ seq(1:length(x))))
  res$coefficients[2]
}

pointr2 <- function(x) {
  res <- (lm(log(x) ~ seq(1:length(x))))
  summary(res)$r.squared
}

pointregress <- function(x) {
  res <- (lm(log(x) ~ seq(1:length(x))))
  c(res$coefficients[2],summary(res)$r.squared)
}

pointpredict<-function(x){
  y=log(x)
  x1= seq(1:length(x))
  res <- lm(y ~ x1)
  out=predict(res,data.frame(x1=length(x)))
  exp(out)
}


CashFlow <- function(portfolio, settledate, brokerage) {
  vcash = rep(0, length(settledate))
  for (p in 1:nrow(portfolio)) {
    index = which(settledate == as.Date(portfolio[p, 'entrytime'], tz = "Asia/Kolkata"))
    vcash[index] = vcash[index] - portfolio[p, 'size'] * portfolio[p, 'entryprice'] - portfolio[p, 'size'] * portfolio[p, 'entryprice'] *
      brokerage
  }
  for (p in 1:nrow(portfolio)) {
    if (!is.na(portfolio[p, 'exittime'])) {
      index = which(settledate == as.Date(portfolio[p, 'exittime'], tz = "Asia/Kolkata"))
      vcash[index] = vcash[index] + portfolio[p, 'size'] *
        portfolio[p, 'exitprice'] - portfolio[p, 'size'] *
        portfolio[p, 'exitprice'] * brokerage
    }
  }
  vcash

}

xirr<-function(cf,dates,interval = c(-0.9, 10)){
  indices=which(cf!=0)
  cf1=cf[indices]
  dates1=as.Date(dates[indices])
  return (tryCatch(tvm::xirr(cf1,dates1,interval),error=function(err){NA_real_}))
}

# xirr <- function(cf, dates,par=c(0,1),trace=FALSE) {
#   # Secant method.
#   indices=which(cf!=0)
#   cf=cf[indices]
#   dates=dates[indices]
#   secant <-
#     function(par,
#              fn,
#              tol = 1.e-07,
#              itmax = 100,
#              trace = FALSE,
#              ...) {
#       # par = a starting vector with 2 starting values
#       # fn = a function whose first argument is the variable of interest
#       if (length(par) != 2)
#         stop("You must specify a starting parameter vector of length 2")
#       p.2 <- par[1]
#       p.1 <- par[2]
#       f <- rep(NA, length(par))
#       f[1] <- fn(p.1, ...)
#       f[2] <- fn(p.2, ...)
#       iter <- 1
#       pchg <- abs(p.2 - p.1)
#       fval <- f[2]
#       if (trace)
#         cat("par: ", par, "fval: ", f, "\n")
#       while (pchg >= tol &
#              abs(fval) > tol & iter <= itmax) {
#         p.new <- p.2 - (p.2 - p.1) * f[2] / (f[2] - f[1])
#         p.new=max(-0.99,p.new)
#         pchg <- abs(p.new - p.2)
#         fval <-
#           ifelse(is.na(fn(p.new, ...)), 1, fn(p.new, ...))
#         p.1 <- p.2
#         p.2 <- p.new
#         f[1] <- f[2]
#         f[2] <- fval
#         iter <- iter + 1
#         if (trace)
#           cat("par: ", p.new, "fval: ", fval, "\n")
#       }
#       list(par = p.new,
#            value = fval,
#            iter = iter)
#     }
#
#   # Net present value.
#   npv <-
#     function(irr, cashflow, times)
#       sum(cashflow / (1 + irr) ^ times)
#
#   times <-
#     as.numeric(difftime(dates, dates[1], units = "days")) / 365.24
#
#   r <- secant(
#     par = c(0, 1),
#     fn = npv,
#     cashflow = cf,
#     times = times
#   )
#
#   return(r$par)
# }

placeRedisOrder<-function(trades,referenceDate,parameters,redisdb,map=FALSE,reverse=FALSE,setDisplaySize=TRUE,setLimitPrice=FALSE,kTimeZone="Asia/Kolkata"){

  if(!"entry.splitadjust" %in% names(trades)){
    trades$entry.splitadjust=1
  }

  if(!"exit.splitadjust" %in% names(trades)){
    trades$exit.splitadjust=1
  }

  if(reverse){
    trades$side=ifelse(trades$side=="BUY","SHORT","BUY")
  }

  entryindices=which(trades$entrytime == referenceDate)
  exitindices=which(trades$exittime == referenceDate & trades$exitreason!="Open")

  # Exit
  if(length(exitindices)>0){
    redisConnect()
    redisSelect(redisdb)
    out <- trades[exitindices,]
    for (o in 1:nrow(out)) {
      change = 0
      side = "UNDEFINED"
      # calculate starting positions by excluding trades already considered in this & prior iterations.
      # Effectively, the abs(startingposition) should keep reducing for duplicate symbols.
      startingpositionexcluding.this=GetCurrentPosition(out[o, "symbol"], trades[-exitindices[1:o],],trades.till = referenceDate-1,position.on = referenceDate-1)
      if(grepl("BUY",out[o,"trade"])){
        change=-out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"]
        side="SELL"
      }else{
        change=out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"]
        side="COVER"
      }
      startingposition = startingpositionexcluding.this-change
      if(map){
        parameters$ParentDisplayName=out[o,"mapsymbol"]
        parameters$ChildDisplayName=out[o,"mapsymbol"]
      }else{
        parameters$ParentDisplayName=out[o,"symbol"]
        parameters$ChildDisplayName=out[o,"symbol"]
      }
      parameters$OrderSide=side
      parameters$StrategyOrderSize=out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"]
      parameters$StrategyStartingPosition=as.character(abs(startingposition))
      parameters$OrderReason="REGULAREXIT"
      if(setDisplaySize){
        parameters$DisplaySize=out[o,"size"]
      }
      if(setLimitPrice){
        parameters$LimitPrice=out[o,"exitprice"]
      }
      parameters$BarrierLimitPrice=out[o,"barrierlimitprice.exit"]
      if("barrierlimitprice.exit" %in% names(trades)){
        parameters$BarrierLimitPrice=out[o,"barrierlimitprice.exit"]
      }

      redisString=toJSON(parameters,dataframe = c("columns"),auto_unbox = TRUE)
      redisString<-gsub("\\[","",redisString)
      redisString<-gsub("\\]","",redisString)
      redisRPush(paste("trades", parameters$OrderReference, sep = ":"),charToRaw(redisString))
    }

  }

  #Entry
  if(length(entryindices)>0){
    redisConnect()
    redisSelect(redisdb)
    out <- trades[entryindices,]
    for (o in 1:nrow(out)) {
      endingposition=GetCurrentPosition(out[o, "symbol"], trades,position.on = referenceDate)
      change = 0
      side = "UNDEFINED"
      if(grepl("BUY",out[o,"trade"])){
        change=out[o,"size"]
        side="BUY"
      }else if (grepl("SHORT",out[o,"trade"])){
        change=-out[o,"size"]
        side="SHORT"
      }
      startingposition = endingposition - change
      if(map){
        parameters$ParentDisplayName=out[o,"mapsymbol"]
        parameters$ChildDisplayName=out[o,"mapsymbol"]
      }else{
        parameters$ParentDisplayName=out[o,"symbol"]
        parameters$ChildDisplayName=out[o,"symbol"]
      }
      parameters$OrderSide=side
      parameters$StrategyOrderSize=out[o,"size"]
      parameters$StrategyStartingPosition=as.character(abs(startingposition))
      parameters$OrderReason="REGULARENTRY"

      if(setDisplaySize){
        parameters$DisplaySize=out[o,"size"]
      }
      if(setLimitPrice){
        parameters$LimitPrice=out[o,"entryprice"]
      }

      if("tp" %in% names(trades)){
        parameters$TakeProfit=out[o,"tp"]
      }
      if("sl" %in% names(trades)){
        parameters$StopLoss=out[o,"sl"]
      }
      if("barrierlimitprice.entry" %in% names(trades)){
        parameters$BarrierLimitPrice=out[o,"barrierlimitprice.entry"]
      }

      redisString=toJSON(parameters,dataframe = c("columns"),auto_unbox = TRUE)
      redisString<-gsub("\\[","",redisString)
      redisString<-gsub("\\]","",redisString)
      redisRPush(paste("trades", parameters$OrderReference, sep = ":"),charToRaw(redisString))
    }
  }
}

generateExecutionSummary<-function(trades,bizdays,backteststart,backtestend,strategyname,kSubscribers,kBrokerage,kCommittedCapital,kMargin=1,kMarginOnUnrealized=FALSE,kInvestmentReturn=0.06,kOverdraftPenalty=0.2,executiondb=0,realtime=FALSE,...){
  # print parameters
  print(paste("bizdays:",paste(bizdays,collapse = ",")))
  print(paste("backteststart",backteststart,sep = ":"))
  print(paste("backtestend",backtestend,sep = ":"))
  print(paste("strategyname",strategyname,sep = ":"))
  print(paste("kSubscribers:",paste(kSubscribers,collapse = ",")))
  print(paste("kBrokerage",kBrokerage,sep=":"))
  print(paste("kCommittedCapital",kCommittedCapital,sep=":"))
  print(paste("kMargin",kMargin,sep=":"))
  print(paste("kMarginOnUnrealized",kMarginOnUnrealized,sep=":"))
  print(paste("kInvestmentReturn",kInvestmentReturn,sep=":"))
  print(paste("kOverdraftPenalty",kOverdraftPenalty,sep=":"))
  print(paste("executiondb",executiondb,sep=":"))
  print(paste("realtime",realtime,sep=":"))



  # 1.1 Strategy Metrics
  if(nrow(trades)>0){
    for(i in seq_len(nrow(kSubscribers))){
      name=kSubscribers$name[i]
      account=kSubscribers$account[i]
      receiver.email=kSubscribers$email[i]
      allocation=kSubscribers$allocation[i]
      externalfile=kSubscribers$externalfile[i]
      strategydb=kSubscribers$redisdb[i]
      if(!(is.na(account) && is.na(externalfile))){ # if both values are na, there is no recon poss with execution. Skip
        print(paste(i,":","Calculating Strategy Metrics"))
        itrades=revalPortfolio(trades,kBrokerage,realtime=realtime, allocation=allocation)
        exittime=itrades$exittime[!is.na(itrades$exittime)]
        BackTestEndTime=max(exittime)
        #bizdays=bizdays[bizdays>=as.POSIXct(backteststart,tz=kTimeZone) & bizdays<=as.POSIXct(backtestend,tz=kTimeZone)]
        pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
        #        cumpnl<-CalculateDailyPNL(itrades,pnl,kBrokerage,margin=kMargin,marginOnUnrealized = kMarginOnUnrealized,realtime=realtime)
        cumpnl<-CalculateDailyPNL(itrades,pnl,kBrokerage,margin=kMargin,marginOnUnrealized = kMarginOnUnrealized,realtime = realtime)
        cumpnl$idlecash=kCommittedCapital*allocation-cumpnl$cashdeployed
        cumpnl$daysdeployed=as.numeric(c(diff.POSIXt(cumpnl$bizdays),0))
        cumpnl$investmentreturn=ifelse(cumpnl$idlecash>0,cumpnl$idlecash*cumpnl$daysdeployed*kInvestmentReturn/365,cumpnl$idlecash*cumpnl$daysdeployed*kOverdraftPenalty/365)
        cumpnl$investmentreturn=cumsum(cumpnl$investmentreturn)

        pnl <-  cumpnl$realized + cumpnl$unrealized - cumpnl$brokerage + cumpnl$investmentreturn
        dailypnl <-  pnl - Ref(pnl, -1)
        dailypnl <-  ifelse(is.na(dailypnl),0,dailypnl)
        dailyreturn <-  ifelse(cumpnl$longnpv +cumpnl$shortnpv== 0, 0,dailypnl / kCommittedCapital)
        sharpe <- sharpe(dailyreturn)
        sharpe=formatC(sharpe,format="f",digits=2)
        xirr=xirr(cumpnl$cashflow,cumpnl$bizdays)*100
        #xirr=xirr(cumpnl$cashflow,cumpnl$bizdays,trace = TRUE)*100
        xirr=formatC(xirr,format="f",digits=2)
        xirr=paste0(xirr,"%")

        daysOfStrategy=as.numeric(min(Sys.Date(),backtestend)) - as.numeric(as.Date(backteststart))
        yearsOfStrategy=daysOfStrategy/365
        if(yearsOfStrategy<1){
          yearsOfStrategy=1
        }
        annualizedSimpleReturn=formatC(sum(itrades$pnl)*36500/(daysOfStrategy*kCommittedCapital),format="f",digits=2)
        annualizedSimpleReturn=paste0(annualizedSimpleReturn,"%")

        WinRatio=sum(itrades$pnl>0)*100/nrow(itrades)
        WinRatio=paste0(specify_decimal(WinRatio,2),"%")

        Exposure=last(cumpnl$longnpv+cumpnl$shortnpv)*kMargin
        Exposure=formatC(Exposure,format="d",big.mark = ",", digits = 0)

        ProfitToday=last(dailypnl)
        ProfitToday=formatC(ProfitToday,format="d",big.mark = ",",digits=0)

        Metrics=c("Client Name","Recent Day Profit","Exposure","Holding Period Profit","Annual Return (Simple)","Investment Income","IRR (Excluding Investment Income)","Sharpe (Including Investment Income)","Win Ratio")
        Strategy=c(name,ProfitToday,Exposure,formatC(specify_decimal(sum(itrades$pnl),0),format="d",big.mark = ","),annualizedSimpleReturn,formatC(specify_decimal(last(cumpnl$investmentreturn),0),format="d",big.mark = ","),xirr,sharpe,WinRatio)
        Metrics=cbind(Metrics,Strategy)

        # 1.2 Execution metrics
        print(paste(i,":","Calculating Execution Metrics"))

        if(is.na(externalfile)){
          pattern=paste("*trades*",tolower(trimws(strategyname)),"*",toupper(account),sep="")
          ExecutionsRedis=createPNLSummary(executiondb,pattern,backteststart,backtestend)
        }else{
          ExecutionsRedis=read.csv(externalfile,header = TRUE,stringsAsFactors = FALSE)
          ExecutionsRedis$entrytime=as.POSIXct(ExecutionsRedis$entrytime,format="%d-%m-%Y")
          ExecutionsRedis$exittime=as.POSIXct(ExecutionsRedis$exittime,format="%d-%m-%Y")
          ExecutionsRedis$netposition=ifelse(ExecutionsRedis$exitreason=="Open",ExecutionsRedis$size,0)
        }
        ExecutionsRedis=revalPortfolio(ExecutionsRedis,kBrokerage,realtime=realtime,1)
        #bizdays=bizdays[bizdays>=as.POSIXct(backteststart,tz=kTimeZone) & bizdays<=as.POSIXct(kBackTestEndDate,tz=kTimeZone)]
        ExecutionsRedis=filter(ExecutionsRedis,entrytime>=first(bizdays))
        pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
        #cumpnl<-CalculateDailyPNL(ExecutionsRedis,pnl,kBrokerage,margin=kMargin,marginOnUnrealized = kMarginOnUnrealized,realtime=realtime)
        cumpnl<-CalculateDailyPNL(ExecutionsRedis,pnl,kBrokerage,margin=kMargin,marginOnUnrealized = kMarginOnUnrealized,realtime=realtime)
        cumpnl$idlecash=kCommittedCapital*allocation-cumpnl$cashdeployed
        cumpnl$daysdeployed=as.numeric(c(diff.POSIXt(cumpnl$bizdays),0))
        cumpnl$investmentreturn=ifelse(cumpnl$idlecash>0,cumpnl$idlecash*cumpnl$daysdeployed*kInvestmentReturn/365,cumpnl$idlecash*cumpnl$daysdeployed*kOverdraftPenalty/365)
        cumpnl$investmentreturn=cumsum(cumpnl$investmentreturn)

        pnl <-  cumpnl$realized + cumpnl$unrealized - cumpnl$brokerage + cumpnl$investmentreturn
        dailypnl <-  pnl - Ref(pnl, -1)
        dailypnl <-  ifelse(is.na(dailypnl),0,dailypnl)
        dailyreturn <-  ifelse(cumpnl$longnpv +cumpnl$shortnpv== 0, 0,dailypnl / kCommittedCapital)
        sharpe <- sharpe(dailyreturn)
        sharpe=formatC(sharpe,format="f",digits=2)
        xirr=tryCatch(xirr(cumpnl$cashflow,cumpnl$bizdays)*100,error=function(err){NA_real_})
        xirr=formatC(xirr,format="f",digits=2)
        xirr=paste0(xirr,"%")

        annualizedSimpleReturn=formatC(sum(ExecutionsRedis$pnl)*36500/(daysOfStrategy*kCommittedCapital),format="f",digits=2)
        annualizedSimpleReturn=paste0(annualizedSimpleReturn,"%")

        WinRatio=sum(ExecutionsRedis$pnl>0)*100/nrow(ExecutionsRedis)
        WinRatio=paste0(specify_decimal(WinRatio,2),"%")

        Exposure=last(cumpnl$longnpv+cumpnl$shortnpv)*kMargin
        Exposure=formatC(Exposure,format="d",big.mark = ",", digits = 0)

        ProfitToday=last(dailypnl)
        ProfitToday=formatC(ProfitToday,format="d",big.mark = ",",digits=0)

        Execution=c(name,ProfitToday,Exposure,formatC(specify_decimal(sum(ExecutionsRedis$pnl),0),format="d",big.mark = ","),annualizedSimpleReturn,formatC(specify_decimal(last(cumpnl$investmentreturn),0),format="d",big.mark = ","),xirr,sharpe,WinRatio)
        Metrics=cbind(Metrics,Execution)

        body=paste0("Key Metrics: StartDate=",backteststart,", EndDate=",BackTestEndTime,tableHTML(Metrics,border=1),"<br><br>")

        # 2.0 Open Trades
        print(paste(i,":","Identifying Expected Open trades"))

        trades.selected.columns=filter(itrades,exitreason=="Open") %>% select(symbol,trade,size,entrytime,entryprice,exittime,mtmprice=exitprice,pnl=pnl)
        if(nrow(trades.selected.columns)>0){
          trades.selected.columns$pnl=formatC(specify_decimal(trades.selected.columns$pnl,0),format="d",big.mark = ",")
        }
        body=paste0(body,"Open Trades Expected by Algorithm",tableHTML(trades.selected.columns,border=1),"<br>")

        openTrades=filter(ExecutionsRedis,exitreason=="Open") %>% select(symbol,trade,size,entrytime,entryprice,exittime,mtmprice=exitprice,pnl=pnl)
        if(nrow(openTrades)>0){
          openTrades$entryprice=specify_decimal(openTrades$entryprice,2)
          if(nrow(openTrades)>0){
            openTrades$pnl=formatC(specify_decimal(openTrades$pnl,0),format="d",big.mark = ",")
          }
          body= paste0(body,"Positions In Execution Logs.","</p>", tableHTML(openTrades,border = 1),"<br>")
        }

        # 3.0 Superfluous Trades
        print(paste(i,":","Identifying Superfluos Trades"))
        # Reconcile with Redis - Strategy DB.
        orderPositions.recon=data.frame()
        orderPositions=data.frame(symbol=character(),position.executionlog=numeric())

        if(is.na(externalfile)){
          pattern=paste("*trades*",tolower((strategyname)),":*",sep="")
          OrdersRedis=createPNLSummary(strategydb,trimws(pattern),as.character(backteststart),as.character(backtestend))
          OrdersRedis=OrdersRedis[OrdersRedis$netposition!=0,]
          if(nrow(OrdersRedis)>0){
            orderPositions=aggregate(netposition~symbol,OrdersRedis,FUN=sum)
            strategyPositions=filter(itrades,exitreason=="Open")
            if(nrow(strategyPositions)>0){
              strategyPositions$size=ifelse(strategyPositions$trade=="BUY",strategyPositions$size,-strategyPositions$size)
              strategyPositions=aggregate(size~symbol,strategyPositions,FUN=sum)
              orderPositions.recon=merge(orderPositions,strategyPositions,all.x = TRUE,all.y = TRUE)
              orderPositions.recon$netposition=ifelse(is.na(orderPositions.recon$netposition),0,orderPositions.recon$netposition)
              orderPositions.recon$size=ifelse(is.na(orderPositions.recon$size),0,orderPositions.recon$size)
              names(orderPositions.recon)=c("symbol","RedisPosition","StrategyRequirement")
              excessInRedis=filter(orderPositions.recon,(RedisPosition>0 & RedisPosition > StrategyRequirement)| (RedisPosition<0 & RedisPosition < StrategyRequirement))
              excessInRedis$excess=excessInRedis$RedisPosition-excessInRedis$StrategyRequirement
              colnames(excessInRedis)=c("Symbol","Positions - Order Logs", "Positions - Algorithm", "Excess")
            }else{
              excessInRedis=orderPositions
              names(excessInRedis)=c("symbol","RedisPosition")
              excessInRedis$StrategyRequirement=0
            }
            if(nrow(excessInRedis)>0){
              body= paste0(body,"Superfluous Trades: The following positions in Order Logs are not required by strategy. Please correct manually - ",strategyname,".", tableHTML(excessInRedis,border = 1),"<br>")
            }
          }
        }

        executionPositions.recon=data.frame()
        executionPositions=data.frame(symbol=character(),position.orderlog=numeric())
        ExecutionsRedis=ExecutionsRedis[ExecutionsRedis$netposition!=0,]
        if(nrow(ExecutionsRedis)>0){
          executionPositions=aggregate(netposition~symbol,ExecutionsRedis,FUN=sum)
          strategyPositions=filter(itrades,exitreason=="Open")
          if(nrow(strategyPositions)>0){
            strategyPositions$size=ifelse(strategyPositions$trade=="BUY",strategyPositions$size,-strategyPositions$size)
            strategyPositions=aggregate(size~symbol,strategyPositions,FUN=sum)
            executionPositions.recon=merge(executionPositions,strategyPositions,all.x = TRUE,all.y = TRUE)
            executionPositions.recon$netposition=ifelse(is.na(executionPositions.recon$netposition),0,executionPositions.recon$netposition)
            executionPositions.recon$size=ifelse(is.na(executionPositions.recon$size),0,executionPositions.recon$size)
            names(executionPositions.recon)=c("symbol","RedisPosition","StrategyRequirement")
            excessInRedis=filter(executionPositions.recon,(RedisPosition>0 & RedisPosition > StrategyRequirement)| (RedisPosition<0 & RedisPosition < StrategyRequirement))
            excessInRedis$excess=excessInRedis$RedisPosition-excessInRedis$StrategyRequirement
            colnames(excessInRedis)=c("Symbol","Positions - Execution Logs", "Positions - Algorithm", "Excess")
          }else{
            excessinRedis=executionPositions
            names(excessInRedis)=c("symbol","RedisPosition")
            excessInRedis$StrategyRequirement=0
          }
          if(nrow(excessInRedis)>0){
            body= paste0(body,"Superfluous Trades: The following positions in Execution Logs are not required by strategy. Please correct manually - ",strategyname,".", tableHTML(excessInRedis,border = 1),"<br>")
          }
        }
        # 4.0 Catch Up Trades
        print(paste(i,":","Identifying Catch Up Trades"))

        if(is.na(externalfile)){
          shortInRedis=filter(orderPositions.recon,(StrategyRequirement>0 & RedisPosition < StrategyRequirement)| (StrategyRequirement<0 & RedisPosition > StrategyRequirement))
          if(nrow(shortInRedis)>0){
            shortInRedis$shortfall=shortInRedis$StrategyRequirement-shortInRedis$RedisPosition
            colnames(shortInRedis)=c("Symbol","Positions - Order Logs", "Positions - Algorithm", "Shortfall")
            body= paste0(body," Catch Up: The following positions are required by strategy but not in order logs. Please consider if you would like to manually take these positions - ",strategyname,". </p>", tableHTML(shortInRedis,border=1),"<br>")
          }
        }

        shortInRedis=filter(executionPositions.recon,(StrategyRequirement>0 & RedisPosition < StrategyRequirement)| (StrategyRequirement<0 & RedisPosition > StrategyRequirement))
        if(nrow(shortInRedis)>0){
          shortInRedis$shortfall=shortInRedis$StrategyRequirement-shortInRedis$RedisPosition
          colnames(shortInRedis)=c("Symbol","Positions - Execution Logs", "Positions - Algorithm", "Shortfall")
          body= paste0(body," Catch Up: The following positions are required by strategy but not in execution logs. Please consider if you would like to manually take these positions - ",strategyname,". </p>", tableHTML(shortInRedis,border=1),"<br>")
        }


        # 5.0 Redis Inconsistent
        print(paste(i,":","Identifying Inconsistencies in Redis"))

        if(is.na(externalfile)){
          # Excess Execution
          colnames(executionPositions)=c("symbol","position.executionlog")
          colnames(orderPositions)=c("symbol","position.orderlog")
          redisrecon=merge(orderPositions,executionPositions)
          IncorrectExecutionsInRedis=filter(redisrecon,(position.executionlog>position.orderlog))
          colnames(IncorrectExecutionsInRedis)=c("Symbol","Positions - Order Logs", "Positions - Execution Logs")

          if(nrow(IncorrectExecutionsInRedis)>0){
            body= paste0(body,"Logs Recon Issue: The following positions in Execution logs are higher than Order logs. Please correct manually - ",strategyname,". </p>", tableHTML(IncorrectExecutionsInRedis,border=1),"<br>")
          }

          # Shortfall in Executions
          IncorrectExecutionsInRedis=filter(redisrecon,(position.executionlog<position.orderlog))
          colnames(IncorrectExecutionsInRedis)=c("Symbol","Positions - Order Logs", "Positions - Execution Logs")
          if(nrow(IncorrectExecutionsInRedis)>0){
            body= paste0(body,"Logs Recon Issue: The following positions in Execution logs are lower than order logs - ",strategyname,". </p>", tableHTML(IncorrectExecutionsInRedis,border=1),"<br>")
          }

        }

        mime() %>%
          to(receiver.email) %>%
          from("reporting@incurrency.com") %>%
          subject(paste0("Summary for ",strategyname, " - ",name)) %>%
          html_body(body) %>%
          send_message()
      }
    }
  }
}

calculateBrokerage<-function(tradeRow,kBrokerage,calculation="BOTH"){
  if(calculation=="ENTRY"){
    entrybrokerage=ifelse(tradeRow$entryprice==0,0,ifelse(grepl("BUY",tradeRow$trade),kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTBuy)/100,kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTSell)/100))
    exitbrokerage=0
  }else if(calculation=="EXIT"){
    entrybrokerage=0
    exitbrokerage=ifelse(tradeRow$exitreason=="Open",0,ifelse(grepl("BUY",tradeRow$trade),kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTSell)/100,kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTBuy)/100))
  }else{
    entrybrokerage=ifelse(tradeRow$entryprice==0,0,ifelse(grepl("BUY",tradeRow$trade),
                                                          kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTBuy)/100,
                                                          kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTSell)/100))
    exitbrokerage=ifelse(tradeRow$exitreason=="Open",0,ifelse(grepl("BUY",tradeRow$trade),kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTSell)/100,kBrokerage$PerContractBrokerage+tradeRow$entryprice*tradeRow$size*(kBrokerage$ValueBrokerage+kBrokerage$STTBuy)/100))
  }
  entrybrokerage+exitbrokerage
}

getTrendMetrics<-function(symbol,dest="daily",realtime=FALSE,...){
  md=loadSymbol(symbol,dest = dest,realtime=realtime,...)
  t=Trend(md$date,md$ahigh,md$alow,md$aclose)
  t=t[,c("date","trend","numberhh","numberll","movementhighlow","movementsettle")]
  t=merge(t,md[,c("date","asettle")])
  t$group=c(0, diff(t$trend))
  t$group=ifelse(t$group!=0,1,0)
  t$group=cumsum(t$group)
  summary<-t %>%
    dplyr::group_by(group) %>%
    dplyr::summarize(start=first(date),end=last(date),trend=first(trend),bars=n(),hlmove=round(last(movementhighlow)*100/first(asettle),2),settlemove=round(last(movementsettle)*100/first(asettle),2))
  summary=as.data.frame(summary)
  # analyze uptrend
  up_max_bars=max(filter(summary,trend==1)$bars)
  up_min_bars=min(filter(summary,trend==1)$bars)
  up_avg_bars=mean(filter(summary,trend==1)$bars)
  up_sd_bars=sd(filter(summary,trend==1)$bars)
  up_count=nrow(filter(summary,trend==1))
  up_total=sum(filter(summary,trend==1)$settlemove)
  up_total_move=sum(filter(summary,trend==1)$settlemove)
  up_avg_move=mean(filter(summary,trend==1)$settlemove)
  up_max_move=max(filter(summary,trend==1)$settlemove)
  up_min_move=min(filter(summary,trend==1)$settlemove)
  up_move=c(up_count,up_max_bars,up_min_bars,up_avg_bars,up_sd_bars,up_max_move,up_min_move,up_avg_move)
  # analyze dntrend
  dn_max_bars=max(filter(summary,trend==-1)$bars)
  dn_min_bars=min(filter(summary,trend==-1)$bars)
  dn_avg_bars=mean(filter(summary,trend==-1)$bars)
  dn_sd_bars=sd(filter(summary,trend==-1)$bars)
  dn_count=nrow(filter(summary,trend==-1))
  dn_total_move=sum(filter(summary,trend==-1)$settlemove)
  dn_avg_move=mean(filter(summary,trend==-1)$settlemove)
  dn_max_move=max(filter(summary,trend==-1)$settlemove)
  dn_min_move=min(filter(summary,trend==-1)$settlemove)
  dn_move=c(dn_count,dn_max_bars,dn_min_bars,dn_avg_bars,dn_sd_bars,dn_max_move,dn_min_move,dn_avg_move)
  # analyze neutral trend
  nt_max_bars=max(filter(summary,trend==0)$bars)
  nt_min_bars=min(filter(summary,trend==0)$bars)
  nt_avg_bars=mean(filter(summary,trend==0)$bars)
  nt_sd_bars=sd(filter(summary,trend==0)$bars)
  nt_count=nrow(filter(summary,trend==0))
  nt_total_move=sum(filter(summary,trend==0)$settlemove)
  nt_avg_move=mean(filter(summary,trend==0)$settlemove)
  nt_max_move=max(filter(summary,trend==0)$settlemove)
  nt_min_move=min(filter(summary,trend==0)$settlemove)
  nt_move=c(nt_count,nt_max_bars,nt_min_bars,nt_avg_bars,nt_sd_bars,nt_max_move,nt_min_move,nt_avg_move)
  out=cbind(up_move,dn_move,nt_move)
  rownames(out)=c("Number of Moves","Max Bars in Move","Min Bars in Move","Avg Bars in Move","SD of Bars","Largest Size of Move", "Lowest Size of Move","Average Size of Move")
  out
}

get_os <- function(){
  sysinf <- Sys.info()
  if (!is.null(sysinf)){
    os <- sysinf['sysname']
    if (os == 'Darwin')
      os <- "osx"
  } else { ## mystery machine
    os <- .Platform$OS.type
    if (grepl("^darwin", R.version$os))
      os <- "osx"
    if (grepl("linux-gnu", R.version$os))
      os <- "linux"
  }
  tolower(os)
}

thisFile <- function() {
  #https://stackoverflow.com/questions/1815606/rscript-determine-path-of-the-executing-script
  cmdArgs <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  match <- grep(needle, cmdArgs)
  if (length(match) > 0) {
    # Rscript
    return(normalizePath(sub(needle, "", cmdArgs[match])))
  } else {
    # 'source'd via R console
    return(normalizePath(sys.frames()[[1]]$ofile))
  }
}
