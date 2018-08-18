# Tabulate index changes
library(rredis)
library(RQuantLib)
library(quantmod)
library(zoo)
options(scipen=999)

data.env<- new.env()
if(is.null(getOption("datafolder"))){
  data.env$datafolder="/home/psharma/Dropbox/rfiles/data/daily/"
}else{
  data.env$datafolder=getOption("datafolder")
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

# createTradeSummaryFromRedis<-function(redisdb,pattern,start,end,mdpath,deriv=FALSE){
#   #generates trades dataframe using information in redis
#   redisConnect()
#   redisSelect(redisdb)
#   rediskeys=redisKeys()
#   actualtrades <- data.frame(symbol=character(),trade=character(),entrysize=as.numeric(),entrytime=as.POSIXct(character()),
#                              entryprice=numeric(),exittime=as.POSIXct(character()),exitprice=as.numeric(),
#                              percentprofit=as.numeric(),bars=as.numeric(),brokerage=as.numeric(),
#                              netpercentprofit=as.numeric(),netprofit=as.numeric(),key=character(),stringsAsFactors = FALSE
#   )
#   periodstartdate=as.Date(start,tz="Asia/Kolkata")
#   periodenddate=as.Date(end,tz="Asia/Kolkata")
#
#   rediskeysShortList<-as.character()
#   for(i in 1:length(rediskeys)){
#     l=length(grep(pattern,rediskeys[i]))
#     if(l>0){
#       if(grepl("opentrades",rediskeys[i])||grepl("closedtrades",rediskeys[i])){
#         rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
#       }
#     }
#   }
#   rediskeysShortList<-sort(rediskeysShortList)
#   # loop through keys and generate pnl
#   for(i in 1:length(rediskeysShortList)){
#     data<-unlist(redisHGetAll(rediskeysShortList[i]))
#     exitprice=0
#     entrydate=data["entrytime"]
#     exitdate=data["exittime"]
#     entrydate=as.Date(entrydate,tz="Asia/Kolkata")
#     exitdate=ifelse(is.na(exitdate),Sys.Date(),as.Date(exitdate,tz="Asia/Kolkata"))
#     if(exitdate>=periodstartdate && entrydate<=periodenddate){
#       if(grepl("opentrades",rediskeysShortList[i])){
#         symbol=data["entrysymbol"]
#         symbolsvector = unlist(strsplit(symbol, "_"))
#         symbol<-symbolsvector[1]
#         if (!deriv) {
#           load(paste(mdpath, symbolsvector[1], ".Rdata", sep = ""))
#         } else{
#           load(paste(
#             fnodatafolder,
#             symbolsvector[3],
#             "/",
#             symbol,
#             ".Rdata",
#             sep = ""
#           ))
#         }
#         index=which(as.Date(md$date,tz="Asia/Kolkata")==exitdate)
#         if(length(index)==0){
#           index=nrow(md)
#         }
#         exitprice=md$settle[index]
#       }else{
#         exitprice=as.numeric(data["exitprice"])
#       }
#       percentprofit=(exitprice-as.numeric(data["entryprice"]))/as.numeric(data["entryprice"])
#       percentprofit=ifelse(data["entryside"]=="BUY",percentprofit,-percentprofit)
#       brokerage=as.numeric(data["entrybrokerage"])+as.numeric(data["exitbrokerage"])
#       netpercentprofit=percentprofit-(brokerage/(as.numeric(data["entryprice"])*as.numeric(data["entrysize"])))
#       netprofit=(exitprice-as.numeric(data["entryprice"]))*as.numeric(data["entrysize"])-brokerage
#       netprofit=ifelse(data["entryside"]=="BUY",netprofit,-netprofit)
#       if(!deriv){
#         df=data.frame(symbol=symbol,trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
#                       entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
#                       percentprofit=percentprofit,bars=0,brokerage=brokerage,
#                       netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
#         )
#       }else{
#         df=data.frame(symbol=data["parentsymbol"],trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
#                       entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
#                       percentprofit=percentprofit,bars=0,brokerage=brokerage,
#                       netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
#         )
#       }
#
#       rownames(df)<-NULL
#       actualtrades=rbind(actualtrades,df)
#
#     }
#   }
#   #return(actualtrades[with(trades,order(entrytime)),])
#   return(actualtrades)
# }

createPNLSummary <-
  function(redisdb,
           pattern,
           start,
           end,
           mdpath,
           deriv = FALSE) {
    #pattern = strategy name (Case sensitive)
    #start,end = string as "yyyy-mm-dd"
    #mdpath = path to market data files for valuing open positions
    #realtrades<-createPNLSummary(0,"swing01","2017-01-01","2017-01-31","/home/psharma/Seafile/rfiles/daily-fno/")
    rredis::redisConnect()
    rredis::redisSelect(redisdb)
    rediskeys = redisKeys(paste("*",pattern,"*",sep=""))
    actualtrades <-
      data.frame(
        symbol = character(),
        trade = character(),
        entrysize = as.numeric(),
        entrytime = as.POSIXct(character()),
        entryprice = numeric(),
        exittime = as.POSIXct(character()),
        exitprice = as.numeric(),
        percentprofit = as.numeric(),
        bars = as.numeric(),
        brokerage = as.numeric(),
        netpercentprofit = as.numeric(),
        netprofit = as.numeric(),
        key = character(),
        stringsAsFactors = FALSE
      )
    periodstartdate = as.Date(start, tz = "Asia/Kolkata")
    periodenddate = as.Date(end, tz = "Asia/Kolkata")

    rediskeysShortList <- as.character()
    for (i in 1:length(rediskeys)) {
      l = length(grep(pattern, rediskeys[i]))
      if (l > 0) {
        if (grepl("opentrades", rediskeys[i]) ||
            grepl("closedtrades", rediskeys[i])) {
          rediskeysShortList <- rbind(rediskeysShortList,
                                      rediskeys[i])
        }
      }
    }
    rediskeysShortList <- sort(rediskeysShortList)
    # loop through keys and generate pnl
    if(length(rediskeysShortList)>0){
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
            if (deriv) {
              symbolsvector = unlist(strsplit(symbol, "_"))
              md=loadSymbol(symbol,days=100000)
            } else{
              symbolsvector = unlist(strsplit(symbol, "_"))
              md=loadSymbol(symbol,days=100000)
            }
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
          brokerage = entrybrokerage + exitbrokerage
          netpercentprofit = percentprofit - (brokerage / (
            as.numeric(data["entryprice"]) *
              as.numeric(data["entrysize"])
          ))
          netprofit = (exitprice - as.numeric(data["entryprice"])) * as.numeric(data["entrysize"]) -
            brokerage
          netprofit = ifelse(data["entryside"] == "BUY", netprofit, -netprofit)
          df = data.frame(
            symbol = data["parentsymbol"],
            trade = data["entryside"],
            entrysize = as.numeric(data["entrysize"]),
            entrytime = as.POSIXct(data["entrytime"], tz = "Asia/Kolkata"),
            entryprice = as.numeric(data["entryprice"]),
            exittime = as.POSIXct(data["exittime"], tz = "Asia/Kolkata"),
            exitprice = exitprice,
            percentprofit = percentprofit,
            bars = 0,
            brokerage = brokerage,
            netpercentprofit = netpercentprofit,
            netprofit = netprofit,
            key = rediskeysShortList[i],
            stringsAsFactors = FALSE
          )
          rownames(df) <- NULL
          actualtrades = rbind(actualtrades, df)

        }
      }
    }
    #return(actualtrades[with(trades,order(entrytime)),])
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
      timeformat = as.POSIXct(first(price)$time/1000, origin="1970-01-01")
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
      portfolio <-
        portfolio[portfolio$entrytime <= trades.till, ]
      for (row in 1:nrow(portfolio)) {
        if ((is.na(portfolio[row, 'exittime']) || portfolio[row, 'exittime'] >= position.on ) &&  portfolio[row, 'symbol'] == scrip) {
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
           per.contract.brokerage = TRUE,
           deriv = FALSE) {
    #Returns realized and unrealized pnl for each day
    # Should be called after portfolio is constructed
    # portfolio = df containing columns[symbol,entrytime,exittime,entryprice,exitprice,trade,size]
    # portfolio$exitdate should be NA for rows that still have open positions.
    #pnl = pnl<-data.frame(bizdays=as.Date(subset$date,tz="Asia/Kolkata"),realized=0,unrealized=0,brokerage=0)
    # path = string containing path to market data, ending with "/"
    #brokerage = % brokerage of trade value
    #realized and unrealized profits are cumulative till date
    if (length(brokerage) == 1) {
      brokerage = rep(brokerage, nrow(portfolio))
    }
    pnl$positioncount=0
    pnl$longnpv=0
    pnl$shortnpv=0
    if (nrow(portfolio) > 0) {
      #for (l in 1:193){
      for (l in 1:nrow(portfolio)) {
        name = portfolio[l, 'symbol']
        entrydate = portfolio[l, 'entrytime']
        exitdate = portfolio[l, 'exittime']
        md=loadSymbol(name,days=1000000)
        handlesplits=FALSE
        if("splitadjust" %in% colnames(md)){
            handlesplits=TRUE
          }
        md=unique(md)
        entryindex = which(md$date == entrydate)
        exitindex = last(which(md$date <= exitdate))
        unrealizedpnlexists = FALSE
        if (length(exitindex) == 0) {
          # we do not have an exit date
          exitindex = nrow(md)
          altexitindex = which(md$date == pnl$bizdays[nrow(pnl)])
          exitindex = ifelse(length(altexitindex) > 0, altexitindex, exitindex)
          unrealizedpnlexists = TRUE
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

          for (index in entryindex:(exitindex - 1)) {
            #entryindex,exitindex are indices on md
            #dtindex,dtindexstart,dtindexend are indices on pnl
            dtindex = which(pnl$bizdays == md$date[index])# it is possible that bizdays does not match the md$dates!
            if (length(dtindex) > 0) {
              if(handlesplits){
                newprice=md$asettle[index]
              }else{
                newprice = md$settle[index]
              }
              if(handlesplits){
                if(index==entryindex){
                  newsplitadjustment=1
                }else{
                  newsplitadjustment=md$splitadjust[(index-1)]/md$splitadjust[index]
                }
              }
              newsplitadjustment=1
              pnl$unrealized[dtindex:nrow(pnl)] <-  pnl$unrealized[dtindex:nrow(pnl)] + ifelse(grepl("BUY", side),(newprice*newsplitadjustment - mtm)*size,(mtm - newprice*newsplitadjustment) * size)
              cumunrealized[(dtindex - dtindexstart + 1)] = ifelse(grepl("BUY", side),(newprice*newsplitadjustment - mtm) * size,(mtm - newprice*newsplitadjustment ) * size )
              pnl$longnpv[dtindex]=pnl$longnpv[dtindex]+ifelse(grepl("BUY", side),(newprice*newsplitadjustment)*size,0)
              pnl$shortnpv[dtindex]=pnl$shortnpv[dtindex]+ifelse(grepl("SHORT", side),(newprice*newsplitadjustment)*size,0)
              mtm <- newprice
              if (index == entryindex) {
                if (per.contract.brokerage) {
                  pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + brokerage[l] * size
                } else{
                  pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + brokerage[l] * size * portfolio[l, 'entryprice']
                }
              }
            }
          }
          newsplitadjustment=md$splitadjust[(exitindex)]/md$splitadjust[(exitindex-1)]
          newsplitadjustment=1
          lastdaypnl = ifelse(grepl("BUY", side),(portfolio[l, 'exitprice']*newsplitadjustment - mtm) * size,(mtm - portfolio[l, 'exitprice']*newsplitadjustment) * size)
          if (!unrealizedpnlexists) {
            if (length(dtindex) == 0) {
              dtindex = which(pnl$bizdays ==portfolio[l, "exittime"]) - 1
            }
            pnl$realized[(dtindex + 1):nrow(pnl)] <-pnl$realized[(dtindex + 1):nrow(pnl)] + sum(cumunrealized) + lastdaypnl
            if (per.contract.brokerage) {
              pnl$brokerage[(dtindex + 1):nrow(pnl)] = pnl$brokerage[(dtindex + 1):nrow(pnl)] + brokerage[l] * size
            } else{
              pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + brokerage[l] *
                size * portfolio[l, 'exitprice']*md$splitadjust[(index)]/md$splitadjust[exitindex]
            }
            pnl$unrealized[(dtindex + 1):nrow(pnl)] = pnl$unrealized[(dtindex + 1):nrow(pnl)] - sum(cumunrealized)
          } else{
            pnl$unrealized[(dtindex + 1):nrow(pnl)] = pnl$unrealized[(dtindex + 1):nrow(pnl)] + lastdaypnl
            pnl$longnpv[dtindex+1]=pnl$longnpv[dtindex+1]+ifelse(grepl("BUY", side),(portfolio[l, 'exitprice']*newsplitadjustment) * size,0)
            pnl$shortnpv[dtindex+1]=pnl$shortnpv[dtindex+1]+ifelse(grepl("SHORT", side),(portfolio[l, 'exitprice']*newsplitadjustment) * size,0)
          }
        }
      }
    }
    pnl$cashdeployed=pnl$longnpv+pnl$shortnpv-pnl$unrealized-pnl$realized+pnl$brokerage
    pnl$cashflow=c(NA_real_,diff(pnl$cashdeployed))
    pnl$cashflow[1]=pnl$cashdeployed[1]
    pnl$cashflow=-round(pnl$cashflow,0)
    pnl

  }


MapToFutureTrades<-function(itrades,rollover=FALSE){
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
    itrades$entryprice[i]=futureTradePrice(itrades$symbol[i],itrades$entrytime[i],itrades$entryprice[i])
    itrades$exitprice[i]=futureTradePrice(itrades$symbol[i],itrades$exittime[i],itrades$exitprice[i])
  }
  itrades[order(itrades$entrytime),]
}

futureTradePrice<-function(futureSymbol,tradedate,underlyingtradeprice){
  # Return unadjusted futureprice for the tradedate
  underlying=sapply(strsplit(futureSymbol[1],"_"),"[",1)
  expiry=sapply(strsplit(futureSymbol[1],"_"),"[",3)
  md=loadUnderlyingSymbol(futureSymbol[1],days=1000000)
  if(!("splitadjust" %in% names(md))){
    md$splitadjust=1
  }
  md<-unique(md)
  underlyingprice=md[md$date==tradedate,]
  adjustment=0
  md=loadSymbol(futureSymbol[1],days=1000000)
  md<-unique(md)
  futureprice=md[md$date==tradedate,]
  if(nrow(futureprice)==1 & nrow(underlyingprice)==1){
    if(underlyingtradeprice==underlyingprice$open[1]/underlyingprice$splitadjust[1]){
      adjustment=futureprice$settle-underlyingprice$settle
    }
    if(adjustment[1]>0){
      return (underlyingprice$open+adjustment)
    }else{
      return (futureprice$settle)
    }
  }else{
    print(paste("Future price not found for symbol",futureSymbol, "for date",tradedate,sep=" "))
    return (0)
  }

}

getmtm<-function(symbol,date){
  md=loadSymbol(symbol,cutoff=date,days=5)
  last(md[md$date<=date,c("asettle")])
}

sharpe <- function(returns, risk.free.rate = 0.07) {
  sqrt(252) * (mean(returns) - (risk.free.rate / 365)) / sd(returns)
}

getAllStrikesForExpiry <-
  function(underlyingshortname,
           expiry,
           datafolder="/home/psharma/Dropbox/rfiles/data/") {
    datafolder=paste(datafolder,"daily/opt/",sep="")
    potentialSymbols = list.files(
      paste(datafolder, expiry, sep = ""),
      pattern = paste(underlyingshortname, "_OPT", sep = "")
    )
    b <- lapply(potentialSymbols, strsplit, split = "_")
    c <- sapply(b, tail, n = 1L)
    d <- sapply(c, tail, n = 1L)
    e <- sapply(d, strsplit, "\\.rds")
    #as.numeric(sapply(e,function(x){x[length(x)-1]}))
    e<-as.numeric(e)
    if(underlyingshortname=="NSENIFTY"){
      inclusion=which(e%%100==0)
      e<-e[inclusion]
    }
    e
  }

getClosestStrike <-
  function(dates,
           underlyingshortname,
           expiry) {
    # date is posixct
    # expiry is string %Y%m%d
    # underlyingshortname is just the symbol short name like RELIANCE
    strikes <-
      getAllStrikesForExpiry(underlyingshortname, expiry)
    strikes = strikes[complete.cases(strikes)]
    md=loadSymbol(paste(underlyingshortname,"_FUT_",expiry,"__",sep=""))
    datesubset = md$date %in% dates
    prices = md[datesubset, 'settle']
    #datesubset=md[md$date %in% dates,'date']
    #strikes=strikes[complete.cases(prices)]
    #strikes=strikes[complete.cases(strikes)]
    if (length(prices) > 0) {
      strikeIndex <-
        sapply(prices[1], function(x, s) {
          which.min(abs(s - x))
        }, s = strikes)
      out = strikes[strikeIndex]
    } else{
      longsymbol=paste(underlyingshortname,"_FUT_",expiry,"__",sep="")
      today<-strftime(as.Date(dates[1],format="%Y%m%d",tz=kTimeZone),format="%Y-%m-%d",tz=kTimeZone)
      newrow <- getPriceArrayFromRedis(9,longsymbol,"tick","close",paste(today, " 09:12:00"), paste(today, "15:30:00"))
      if(nrow(newrow)==1){
        prices = newrow$close
        strikeIndex <-
          sapply(prices[1], function(x, s) {
            which.min(abs(s - x))
          }, s = strikes)
        out = strikes[strikeIndex]
      }else{
        out = NA_real_
      }

    }
    out
    #data.frame(date=as.POSIXct(datesubset,tz="Asia/Kolkata"),strike=strikes[strikeIndex])
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

getStrikeByClosestSettlePrice <-
  function(trades,timeZone) {
    trades$strike = NA_real_
    for (i in 1:nrow(trades)) {
      symbolsvector = unlist(strsplit(trades$symbol[i], "_"))
      trades$strike[i] = getClosestStrike(trades$entrytime[i],symbolsvector[1],strftime(trades$entrycontractexpiry[i],"%Y%m%d",tz = timeZone))
    }
    trades
    #dfsignals[!is.na(dfsignals$strike),]

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

chart <-
  function(symbol,...) {
    md<-loadSymbol(symbol,...)
    if(nrow(md)==0){
      type=tolower(strsplit(symbol,"_")[[1]][2])
      path=paste("/home/psharma/Dropbox/rfiles/data/daily/",type,sep="")
      symbols=sapply(strsplit(list.files(path),"\\."),'[',1)
      symbols=sapply(strsplit(symbols,"_"),'[',1)
      shortlist=symbols[grep(substr(symbol,1,5),symbols)]
      if(length(shortlist)>0){
        print(paste("Did you mean..",paste(shortlist,collapse=" or "),sep=" "))
      }
      return()
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
    chartSeries(symbolname,
                subset = paste(start, "::", end, sep = ""),
                theme = customTheme,
                name=name)
    symbolname[paste(start, "::", end, sep = "")]
  }

QuickChart<-function(symbol,...){
  #symbol=deparse(substitute(symbol))
  #type=deparse(substitute(type))
  out<-chart(symbol,...)
  if(!is.null(out)){
    out.md<-convertToDF(out)
    trend.md<-RTrade::Trend(out.md$date,out.md$high,out.md$low,out.md$close)
    swinglevel=xts(trend.md$swinglevel,out.md$date)
    plot(addTA(swinglevel,on=1, type='s',lty=3))
    trend=xts(trend.md$trend,out.md$date)
    plot(addTA(trend,type='s'))
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
          md$date=round(md$date,"mins")
          md$date=as.POSIXct(md$date)
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


loadSymbol<-function(symbol,realtime=FALSE,cutoff=NULL,src="daily",dest="daily",datafolder="/home/psharma/Dropbox/rfiles/data/",days=365,tz="Asia/Kolkata"){
  if(is.null(cutoff)){
    cutoff=Sys.time()
  }else{
    cutoff=as.POSIXct(cutoff,tz=tz)
  }
  if(days==365 && src=="persecond"){
    days=3
    if(dest=="daily"){
      dest="5minutes"
    }
  }
  if(dest=="weekly"){
    starttime=cutoff-7*days*24*60*60
  }else if(dest=="monthly"){
    starttime=cutoff-30*days*24*60*60
  }else{
    starttime=cutoff-days*24*60*60
  }
  md=readRDS3(symbol,datafolder,starttime,cutoff,src) # md in src duration
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
    path=paste("/home/psharma/Dropbox/rfiles/data/daily/",type,sep="")
    symbols=sapply(strsplit(list.files(path),"\\."),'[',1)
    symbols=sapply(strsplit(symbols,"_"),'[',1)
    shortlist=symbols[grep(substr(symbol,1,5),symbols)]
    if(length(shortlist)>0){
      print(paste("Did you mean..",paste(shortlist,collapse=" or "),sep=" "))
    }
    return(md)
  }
  md

}

loadUnderlyingSymbol<-function(symbol,...){
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

readRDS3<-function(symbol,folder,starttime,endtime,duration){
  md=data.frame()
  symbolsvector = unlist(strsplit(symbol, "_"))
  type=tolower(symbolsvector[2])
  folder=paste(folder,duration,"/",type,"/",sep="")
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
  if(symbol==""){
    rredis::redisConnect()
    rredis::redisSelect(2)
    a <-  unlist(rredis::redisSMembers("splits")) # get values from redis in a vector
    rredis::redisClose()
    tmp <- (strsplit(a, split = "_")) # convert vector to list
    k <- lengths(tmp) # expansion size for each list element
    allvalues <-  unlist(tmp) # convert list to vector
    splitinfo <-  data.frame(
      date = 1:length(a),
      symbol = 1:length(a),
      oldshares = 1:length(a),
      newshares = 1:length(a),
      reason = rep("", length(a)),
      stringsAsFactors = FALSE
    )
    for (i in 1:length(a)) {
      for (j in 1:k[i]) {
        runsum = cumsum(k)[i]
        splitinfo[i, j] <- allvalues[runsum - k[i] + j]
      }
    }
    splitinfo$date = as.POSIXct(splitinfo$date, format = "%Y%m%d", tz = "Asia/Kolkata")
    splitinfo$oldshares <- as.numeric(splitinfo$oldshares)
    splitinfo$newshares <- as.numeric(splitinfo$newshares)
  }else{
    # a<-unlist(rredis::redisSMembers("symbolchange")) # get values from redis in a vector
    # origsymbols=sapply(strsplit(a,"_"),"[",2)
    # newsymbols=sapply(strsplit(a,"_"),"[",3)
    symbolchange=getSymbolChange()
    # linkedsymbols=RTrade::linkedsymbols(origsymbols,newsymbols,symbol)
    linkedsymbols=linkedsymbols(symbolchange,symbol,complete)$symbol
    linkedsymbols=paste("^",linkedsymbols,"$",sep="")
    rredis::redisConnect()
    rredis::redisSelect(2)
    a<-unlist(rredis::redisSMembers("splits")) # get values from redis in a vector
    rredis::redisClose()
    date=sapply(strsplit(a,"_"),"[",1)
    date=strptime(date,format="%Y%m%d")
    date=as.POSIXct(date,tz="Asia/Kolkata")
    symbol=sapply(strsplit(a,"_"),"[",2)
    oldshares=sapply(strsplit(a,"_"),"[",3)
    newshares=sapply(strsplit(a,"_"),"[",4)
    reason=sapply(strsplit(a,"_"),"[",5)
    indices=unlist(sapply(linkedsymbols,grep,symbol))
    splitinfo=data.frame()
    splitinfo=data.frame(date=date[indices],symbol=symbol[indices],oldshares=oldshares[indices],newshares=newshares[indices],reason=reason[indices],stringsAsFactors = FALSE)
    splitinfo=splitinfo[order(splitinfo$date),]
    if(nrow(splitinfo)>0){
      splitinfo$oldshares=as.numeric(splitinfo$oldshares)
      splitinfo$newshares=as.numeric(splitinfo$newshares)
    }
  }
  splitinfo
}

getSymbolChange<-function(){
  rredis::redisConnect()
  rredis::redisSelect(2)
  a <-unlist(rredis::redisSMembers("symbolchange")) # get values from redis in a vector
  tmp <-  (strsplit(a, split = "_")) # convert vector to list
  k <-  lengths(tmp) # expansion size for each list element
  allvalues <- unlist(tmp) # convert list to vector
  symbolchange <-
    data.frame(
      date = rep("", length(a)),
      key = rep("", length(a)),
      newsymbol = rep("", length(a)),
      stringsAsFactors = FALSE
    )
  for (i in 1:length(a)) {
    for (j in 1:k[i]) {
      runsum = cumsum(k)[i]
      symbolchange[i, j] <-
        allvalues[runsum - k[i] + j]
    }
  }
  symbolchange$date = as.Date(symbolchange$date, format = "%Y%m%d", tz = "Asia/Kolkata")
  symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
  symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
  rredis::redisClose()
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

xirr <- function(cf, dates,par=c(0,0.1),trace=FALSE) {
  # Secant method.
  secant <-
    function(par,
             fn,
             tol = 1.e-07,
             itmax = 100,
             trace = FALSE,
             ...) {
      # par = a starting vector with 2 starting values
      # fn = a function whose first argument is the variable of interest
      if (length(par) != 2)
        stop("You must specify a starting parameter vector of length 2")
      p.2 <- par[1]
      p.1 <- par[2]
      f <- rep(NA, length(par))
      f[1] <- fn(p.1, ...)
      f[2] <- fn(p.2, ...)
      iter <- 1
      pchg <- abs(p.2 - p.1)
      fval <- f[2]
      if (trace)
        cat("par: ", par, "fval: ", f, "\n")
      while (pchg >= tol &
             abs(fval) > tol & iter <= itmax) {
        p.new <- p.2 - (p.2 - p.1) * f[2] / (f[2] - f[1])
        p.new=max(-0.99,p.new)
        pchg <- abs(p.new - p.2)
        fval <-
          ifelse(is.na(fn(p.new, ...)), 1, fn(p.new, ...))
        p.1 <- p.2
        p.2 <- p.new
        f[1] <- f[2]
        f[2] <- fval
        iter <- iter + 1
        if (trace)
          cat("par: ", p.new, "fval: ", fval, "\n")
      }
      list(par = p.new,
           value = fval,
           iter = iter)
    }

  # Net present value.
  npv <-
    function(irr, cashflow, times)
      sum(cashflow / (1 + irr) ^ times)

  times <-
    as.numeric(difftime(dates, dates[1], units = "days")) / 365.24

  r <- secant(
    par = c(0, 0.1),
    fn = npv,
    cashflow = cf,
    times = times
  )

  return(r$par)
}

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
      endingposition=GetCurrentPosition(out[o, "symbol"], trades,position.on = referenceDate-1)
      change = 0
      side = "UNDEFINED"
      if(grepl("BUY",out[o,"trade"])){
        change=out[o,"size"]
        side="BUY"
      }else{
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

      redisString=toJSON(parameters,dataframe = c("columns"),auto_unbox = TRUE)
      redisString<-gsub("\\[","",redisString)
      redisString<-gsub("\\]","",redisString)
      redisRPush(paste("trades", parameters$OrderReference, sep = ":"),charToRaw(redisString))
    }
  }
}
