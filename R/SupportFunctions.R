# Tabulate index changes
library(rredis)
library(RQuantLib)
library(quantmod)
library(zoo)
options(scipen=999)

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
              load(
                paste(
                  mdpath,
                  symbolsvector[3],
                  "/",
                  symbol,
                  ".Rdata",
                  sep =
                    ""
                )
              )
            } else{
              symbolsvector = unlist(strsplit(symbol, "_"))
              load(paste(
                mdpath,
                symbolsvector[1],
                ".Rdata",
                sep = ""
              ))
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
      high = max(price$value)
      low = min(price$value)
      open = price$value[1]
      close = price$value[nrow(price)]
      out = data.frame(
        date = as.POSIXct(starttime, format = "%Y-%m-%d"),
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
        symbolsvector = unlist(strsplit(name, "_"))
        load(paste(path,symbolsvector[3],"/",name,".Rdata",sep = ""))
      }else{
        load(paste(path,scrip,".Rdata",sep=""))
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
              load(paste(path,symbolsvector[3],"/",name,".Rdata",sep = ""))
            }else{
              load(paste(path,scrip,".Rdata",sep=""))
            }
            buyindex= which(as.Date(md$date, tz = "Asia/Kolkata") == portfolio[row,'entrytime'])
            currentindex= which(as.Date(md$date, tz = "Asia/Kolkata") == position.on)
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
           path,
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
    if (nrow(portfolio) > 0) {
      #for (l in 1:193){
      for (l in 1:nrow(portfolio)) {
        #print(paste("portfolio line:",l,sep=""))
        name = portfolio[l, 'symbol']
        entrydate = as.Date(portfolio[l, 'entrytime'], tz = "Asia/Kolkata")
        exitdate = as.Date(portfolio[l, 'exittime'], tz = "Asia/Kolkata")
        if (!deriv) {
          load(paste(path, name, ".Rdata", sep = ""))
        } else{
          symbolsvector = unlist(strsplit(name, "_"))
          load(paste(path,symbolsvector[3],"/",name,".Rdata",sep = ""))
        }

        handlesplits=FALSE
        if(!is.null(path)){
          if (!deriv) {
            load(paste(path, name, ".Rdata", sep = ""))
          } else{
            symbolsvector = unlist(strsplit(name, "_"))
            load(paste(path,symbolsvector[3],"/",name,".Rdata",sep = ""))
          }

          if("splitadjust" %in% colnames(md)){
            handlesplits=TRUE
          }
        }
        md=unique(md)
        entryindex = which(as.Date(md$date, tz = "Asia/Kolkata") == entrydate)
        exitindex = which(as.Date(md$date, tz = "Asia/Kolkata") == exitdate)
        unrealizedpnlexists = FALSE
        if (length(exitindex) == 0) {
          # we do not have an exit date
          exitindex = nrow(md)
          altexitindex = which(as.Date(md$date, tz = "Asia/Kolkata") == pnl$bizdays[nrow(pnl)])
          exitindex = ifelse(length(altexitindex) > 0, altexitindex, exitindex)
          unrealizedpnlexists = TRUE
        }

        mtm = portfolio[l, "entryprice"]
        #priorsplitadjustment=1
        size = portfolio[l, 'size']
        dtindex = 0
        if (length(entryindex) == 1) {
          dtindexstart = which(
            pnl$bizdays == as.Date(md$date[entryindex], tz = "Asia/Kolkata")
          )
          dtindexend = which(
            pnl$bizdays == as.Date(md$date[exitindex], tz =
                                     "Asia/Kolkata")
          )
          cumunrealized = seq(0,0,length.out = (dtindexend -  dtindexstart + 1))
          side = portfolio[l, 'trade']
          #for (index in entryindex:5269) {
          positionindexstart = which(pnl$bizdays == as.Date(md$date[entryindex], tz = "Asia/Kolkata"))
          positionindexend = which(pnl$bizdays == as.Date(md$date[exitindex], tz = "Asia/Kolkata"))
          pnl$positioncount[positionindexstart:(positionindexend)]<-pnl$positioncount[positionindexstart:(positionindexend)]+1

            for (index in entryindex:(exitindex - 1)) {
            #entryindex,exitindex are indices for portfolio
            #dtindex,dtindexstart,dtindexend are indices for pnl
            #print(index)
            dtindex = which(pnl$bizdays == as.Date(md$date[index], tz = "Asia/Kolkata"))
            if (length(dtindex) > 0) {
              # only proceed if bizdays has the specified md$date[index] value
              if(handlesplits){
                newprice=md$asettle[index]
              }else{
                newprice = md$settle[index]
              }
              newsplitadjustment=1
              if(handlesplits){
                if(index==entryindex){
                  newsplitadjustment=1
                }else{
                  newsplitadjustment=md$splitadjust[(index-1)]/md$splitadjust[index]
                }
              }
              newsplitadjustment=1
              pnl$unrealized[dtindex:nrow(pnl)] <-
                pnl$unrealized[dtindex:nrow(pnl)] + ifelse(grepl("BUY", side),(newprice*newsplitadjustment - mtm)*size,
                                                           (mtm - newprice*newsplitadjustment) * size)
              cumunrealized[(dtindex -
                               dtindexstart + 1)] = ifelse(grepl("BUY", side),(newprice*newsplitadjustment - mtm) * size,
                                                           (mtm - newprice*newsplitadjustment ) * size )
              mtm <- newprice
              #priorsplitadjustment <- newsplitadjustment
              if (index == entryindex) {
                if (per.contract.brokerage) {
                  pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + brokerage[l] * size
                } else{
                  pnl$brokerage[dtindex:nrow(pnl)] = pnl$brokerage[dtindex:nrow(pnl)] + brokerage[l] * size * portfolio[l, 'entryprice']
                }
              }
              # it is possible that bizdays does not match the md$dates!
            }
          }
          newsplitadjustment=md$splitadjust[(exitindex)]/md$splitadjust[(exitindex-1)]
          lastdaypnl = ifelse(grepl("BUY", side),(portfolio[l, 'exitprice']*newsplitadjustment - mtm) * size,(mtm - portfolio[l, 'exitprice']*newsplitadjustment) * size)
          if (!unrealizedpnlexists) {
            if (length(dtindex) == 0) {
              dtindex = which(pnl$bizdays == as.Date(portfolio[l, "exittime"],tz = "Asia/Kolkata")) - 1
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
          }

        }
      }
    }
    pnl

  }

optionTradeSignalsLongOnly <- function(signals,
                                       fnodatafolder,
                                       equitydatafolder,
                                       rollover = FALSE) {
  out = data.frame(
    date = as.POSIXct(character()),
    symbol = character(),
    buy = numeric(),
    sell = numeric(),
    short = numeric(),
    cover = numeric(),
    buyprice = numeric(),
    sellprice = numeric(),
    shortprice = numeric(),
    coverprice = numeric(),
    stringsAsFactors = FALSE
  )

  #### Amend signals for any rollover ####
  signals$inlongtrade <- RTrade::Flip(signals$buy, signals$sell)
  signals$inshorttrade <-
    RTrade::Flip(signals$short, signals$cover)
  if (rollover) {
    signals$rolloverdate <-
      signals$currentmonthexpiry != signals$entrycontractexpiry &
      signals$entrycontractexpiry != Ref(signals$entrycontractexpiry, -1)
    signals$rolloverorders <-
      signals$rolloverdate &
      ((
        signals$inlongtrade &
          Ref(signals$inlongtrade, -1)
      ) |
        (
          signals$inshorttrade & Ref(signals$inshorttrade, -1)
        ))
    for (i in 1:nrow(signals)) {
      if (signals$rolloverorders[i] == TRUE) {
        if (signals$inlongtrade[i] == 1) {
          df.copy = signals[i, ]
          signals$sell[i] = 1
          indexofbuy = getBuyIndices(signals, i)
          #indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
          for (j in 1:length(indexofbuy)) {
            if (j == 1) {
              df.copy$buy[1] = 1
            } else{
              df.copy$buy[1] = 999
            }
            df.copy$strike[1] = signals$strike[indexofbuy[j]]
            signals <- rbind(signals,
                             df.copy)
          }
        } else if (signals$inshorttrade[i] == 1) {
          df.copy = signals[i, ]
          signals$cover[i] = 1
          indexofshort = getShortIndices(signals, i)
          for (j in 1:length(indexofshort)) {
            if (j == 1) {
              df.copy$short[1] = 1
            } else{
              df.copy$short[1] = 999
            }
            df.copy$strike[1] = signals$strike[indexofshort[j]]
            signals <- rbind(signals,
                             df.copy)
          }
        }
      }
    }
  }
  signals <-
    getClosestStrikeUniverse(signals, fnodatafolder, equitydatafolder, kTimeZone)
  signals <- signals[order(signals$date), ]

  #### Entry ####
  expiry = format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
  signals$symbol = ifelse(
    signals$buy > 0,
    paste(
      unlist(strsplit(signals$symbol, "_"))[1],
      "OPT",
      expiry,
      "CALL",
      signals$strike,
      sep = "_"
    ),
    ifelse(
      signals$short > 0,
      paste(
        unlist(strsplit(signals$symbol, "_"))[1],
        "OPT",
        expiry,
        "PUT",
        signals$strike,
        sep = "_"
      ),
      signals$symbol
    )
  )
  for (i in 1:nrow(signals)) {
    if (signals$buy[i] > 0) {
      symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
      load(
        paste(
          fnodatafolder,
          symbolsvector[3],
          "/",
          signals$symbol[i],
          ".Rdata",
          sep = ""
        )
      )
      datarow = md[md$date == signals$date[i],]
      buyprice = datarow$settle[1]
      out = rbind(
        out,
        data.frame(
          date = signals$date[i],
          symbol = signals$symbol[i],
          buy = signals$buy[i],
          sell = 0,
          short = 0,
          cover = 0,
          buyprice = buyprice,
          sellprice = 0,
          shortprice = 0,
          coverprice = 0
        )
      )

    } else if (signals$short[i] > 0) {
      symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
      load(
        paste(
          fnodatafolder,
          symbolsvector[3],
          "/",
          signals$symbol[i],
          ".Rdata",
          sep = ""
        )
      )
      datarow = md[md$date == signals$date[i],]
      shortprice = datarow$settle[1]
      out = rbind(
        out,
        data.frame(
          date = signals$date[i],
          symbol = signals$symbol[i],
          buy = signals$short[i],
          sell = 0,
          short = 0,
          cover = 0,
          buyprice = shortprice,
          sellprice = 0,
          shortprice = 0,
          coverprice = 0
        )
      )
    }
  }

  # Adjust for scenario where there is no rollover and the position(s) is/are open on expiry date
  # indexofbuy = sapply(seq(1:length(signals$buy)), function(x) {
  #   index = tail(which(signals$buy[1:(x - 1)] > 0), 1)
  # })
  # indexofshort = sapply(seq(1:length(signals$short)), function(x) {
  #   index = tail(which(signals$short[1:(x - 1)] > 0), 1)
  # })
  # indexofbuy = unlist(indexofbuy)
  # indexofshort = unlist(indexofshort)
  # indexofshort = c(rep(0, (nrow(signals) - length(indexofshort))), indexofshort)
  # indexofbuy = c(rep(0, (nrow(signals) - length(indexofbuy))), indexofbuy)
  # indexofentry = pmax(indexofbuy, indexofshort)
  # for (i in 1:nrow(signals)) {
  #   symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
  #   if (length(symbolsvector) == 1 && indexofentry[i] > 0) {
  #     indexofbuy = getBuyIndices(signals, i)
  #     if (length(indexofbuy) > 0) {
  #       for (j in 1:length(indexofbuy)) {
  #         symbolsvector = unlist(strsplit(signals$symbol[indexofbuy[j]], "_"))
  #         expiry = as.Date(strptime(symbolsvector[3], "%Y%m%d", tz = "Asia/Kolkata"))
  #         if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
  #           if (signals$cover[i] == 0 && signals$sell[i] == 0)
  #             #continuing trade. close the trade
  #             if (Ref(signals$inlongtrade, -1)[i] > 0) {
  #               signals$sell[i] = 1
  #             } else if (Ref(signals$inshorttrade, -1)[i] > 0) {
  #               signals$cover[i] = 1
  #             }
  #         }
  #       }
  #     }
  #   }
  # }

  for (i in 1:nrow(signals)) {
    if (signals$inlongtrade[i] > 0) {
      indexofbuy = getBuyIndices(signals, i,0)
      if (length(indexofbuy) > 0) {
        for (j in 1:length(indexofbuy)) {
          if(i>indexofbuy[j]){
            symbolsvector = unlist(strsplit(
              signals$symbol[indexofbuy[j]],
              "_"
            ))
            expiry = as.Date(
              strptime(
                symbolsvector[3],
                "%Y%m%d",
                tz = "Asia/Kolkata"
              )
            )
            if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
              signals$sell[i] = 1
            }
          }

        }

      }
    } else if (signals$inshorttrade[i] > 0) {
      indexofshort = getShortIndices(signals, i)
      if (length(indexofshort) > 0) {
        for (j in 1:length(indexofshort)) {
          if(i>indexofshort[j]){
            symbolsvector = unlist(strsplit(
              signals$symbol[indexofshort[j]],
              "_"
            ))
            expiry = as.Date(
              strptime(
                symbolsvector[3],
                "%Y%m%d",
                tz = "Asia/Kolkata"
              )
            )
            if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
              signals$cover[i] = 1
            }
          }

        }
      }

    }
  }

  #### Exit ####
  for (i in 1:nrow(signals)) {
    if (signals$sell[i] > 0) {
      indexofbuy = getBuyIndices(signals, i)
      #      indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
      if (length(indexofbuy) >= 1) {
        uniquesymbols = NULL
        for (j in 1:length(indexofbuy)) {
          if (length(grep(
            signals$symbol[indexofbuy[j]],
            uniquesymbols
          )) == 0) {
            uniquesymbols[length(uniquesymbols) + 1] <-
              signals$symbol[indexofbuy[j]]
            symbolsvector = unlist(strsplit(
              signals$symbol[indexofbuy[j]],
              "_"
            ))
            load(
              paste(
                fnodatafolder,
                symbolsvector[3],
                "/",
                signals$symbol[indexofbuy[j]],
                ".Rdata",
                sep = ""
              )
            )
            datarow = md[md$date == signals$date[i],]
            if (nrow(datarow) == 1) {
              load(
                paste(
                  equitydatafolder,
                  symbolsvector[1],
                  ".Rdata",
                  sep = ""
                )
              )
              udatarow = md[md$date == signals$date[i],]
              sellprice = NA_real_
              if (signals$sell[i] == 1) {
                expiry = as.Date(
                  strptime(
                    symbolsvector[3],
                    "%Y%m%d",
                    tz = "Asia/Kolkata"
                  )
                )
                if (as.Date(
                  datarow$date,
                  tz = "Asia/Kolkata"
                ) ==
                expiry) {
                  if (symbolsvector[4] == "CALL") {
                    sellprice = max(
                      0,
                      udatarow$settle[1] - as.numeric(
                        symbolsvector[5]
                      )
                    )
                  } else{
                    sellprice = max(
                      0,
                      as.numeric(
                        symbolsvector[5]
                      ) - udatarow$settle[1]
                    )
                  }
                } else{
                  sellprice = datarow$settle[1]
                }
              } else if (signals$sell[i] > 1) {
                expiry = as.Date(
                  strptime(
                    symbolsvector[3],
                    "%Y%m%d",
                    tz = "Asia/Kolkata"
                  )
                )
                dte = businessDaysBetween(
                  "India",
                  as.Date(
                    signals$date[i],
                    tz = "Asia/Kolkata"
                  ),
                  expiry
                )
                vol=tryCatch({EuropeanOptionImpliedVolatility(
                  tolower(
                    symbolsvector[4]
                  ),
                  datarow$settle[1],
                  udatarow$settle[1],
                  as.numeric(
                    symbolsvector[5]
                  ),
                  0.015,
                  0.06,
                  dte / 365,
                  0.01
                )},error=function(err){0.01})
                greeks <- EuropeanOption(
                  tolower(
                    symbolsvector[4]
                  ),
                  signals$sellprice[i],
                  as.numeric(
                    symbolsvector[5]
                  ),
                  0.015,
                  0.06,
                  dte / 365,
                  vol
                )
                sellprice = greeks$value
              }

              if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                # add row to signals
                out = rbind(
                  out,
                  data.frame(
                    date = signals$date[i],
                    symbol = signals$symbol[indexofbuy[j]],
                    buy = 0,
                    sell = signals$sell[i],
                    short =
                      0,
                    cover = 0,
                    buyprice = 0,
                    sellprice = sellprice,
                    shortprice = 0,
                    coverprice = 0
                  )
                )
              } else{
                indexofsignal = which(
                  out$date == signals$date[i] &
                    out$symbol == signals$symbol[indexofbuy[j]]
                )
                out$sell[indexofsignal] = signals$sell[i]
                out$sellprice[indexofsignal] = sellprice
              }
            }
            # check if record is for today
            else  if (as.Date(signals$date[i],tz = "Asia/Kolkata") == Sys.Date()) {
              if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                # add row to signals
                out = rbind(
                  out,
                  data.frame(
                    date = signals$date[i],
                    symbol = signals$symbol[indexofbuy[j]],
                    buy = 0,
                    sell = signals$sell[i],
                    short =
                      0,
                    cover = 0,
                    buyprice = 0,
                    sellprice = 0,
                    shortprice = 0,
                    coverprice = 0
                  )
                )

              } else{
                indexofsignal = which(
                  out$date == signals$date[i] &
                    out$symbol == signals$symbol[indexofbuy[j]]
                )
                out$sell[indexofsignal] =
                  signals$sell[i]
                out$sellprice[indexofsignal] =
                  0
              }
            }else{
              print(paste("Missing data for symbol ",signals$symbol[indexofbuy[j]]," for date ",signals$date[i],sep=""))
            }

          }
        }

      }
    } else if (signals$cover[i] > 0) {
      indexofshort = getShortIndices(signals, i)
      #indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
      if (length(indexofshort) >= 1) {
        uniquesymbols = NULL
        for (j in 1:length(indexofshort)) {
          if (length(grep(
            signals$symbol[indexofshort[j]],
            uniquesymbols
          )) == 0) {
            uniquesymbols[length(uniquesymbols) + 1] <-
              signals$symbol[indexofshort[j]]
            symbolsvector = unlist(strsplit(
              signals$symbol[indexofshort[j]],
              "_"
            ))
            load(
              paste(
                fnodatafolder,
                symbolsvector[3],
                "/",
                signals$symbol[indexofshort[j]],
                ".Rdata",
                sep = ""
              )
            )
            datarow = md[md$date == signals$date[i],]
            if (nrow(datarow) == 1) {
              load(
                paste(
                  equitydatafolder,
                  symbolsvector[1],
                  ".Rdata",
                  sep = ""
                )
              )
              udatarow = md[md$date == signals$date[i],]
              coverprice = NA_real_
              if (signals$cover[i] == 1) {
                coverprice = datarow$settle[1]
              } else if (signals$cover[i] > 1) {
                expiry = as.Date(
                  strptime(
                    symbolsvector[3],
                    "%Y%m%d",
                    tz = "Asia/Kolkata"
                  )
                )
                dte = businessDaysBetween(
                  "India",
                  as.Date(
                    signals$date[i],
                    tz = "Asia/Kolkata"
                  ),
                  expiry
                )
                vol = tryCatch({EuropeanOptionImpliedVolatility(
                  tolower(
                    symbolsvector[4]
                  ),
                  datarow$settle[1],
                  udatarow$settle[1],
                  as.numeric(
                    symbolsvector[5]
                  ),
                  0.015,
                  0.06,
                  dte / 365,
                  0.1
                )},error=function(err){0.01})

                greeks <- EuropeanOption(
                  tolower(
                    symbolsvector[4]
                  ),
                  signals$coverprice[i],
                  as.numeric(
                    symbolsvector[5]
                  ),
                  0.015,
                  0.06,
                  dte / 365,
                  vol
                )
                coverprice = greeks$value
              }

              if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                # add row to signals
                out = rbind(
                  out,
                  data.frame(
                    date = signals$date[i],
                    symbol = signals$symbol[indexofshort[j]],
                    buy = 0,
                    sell = signals$cover[i],
                    short = 0,
                    cover = 0,
                    buyprice = 0,
                    sellprice = coverprice,
                    shortprice = 0,
                    coverprice = 0
                  )
                )
              } else{
                indexofsignal = which(
                  out$date == signals$date[i] &
                    out$symbol == signals$symbol[indexofshort[j]]
                )
                out$sell[indexofsignal] = signals$cover[i]
                out$sellprice[indexofsignal] = coverprice
              }
            }
            # check if record is for today
            else if (as.Date(signals$date[i],
                             tz = "Asia/Kolkata") == Sys.Date()) {
              if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                # add row to signals
                out = rbind(
                  out,
                  data.frame(
                    date = signals$date[i],
                    symbol = signals$symbol[indexofshort[j]],
                    buy = 0,
                    sell = signals$cover[i],
                    short = 0,
                    cover = 0,
                    buyprice = 0,
                    sellprice = 0,
                    shortprice = 0,
                    coverprice = 0
                  )
                )

              } else{
                indexofsignal = which(
                  out$date == signals$date[i] &
                    out$symbol == signals$symbol[indexofshort[j]]
                )
                out$sell[indexofsignal] = signals$cover[i]
                out$sellprice[indexofsignal] = 0
              }
            }else{
              print(paste("Missing data for symbol ",signals$symbol[indexofshort[j]]," for date ",signals$date[i],sep=""))
            }

          }

        }
      }

    }
  }
  out[order(out$date), ]
}

optionTradeSignalsShortOnly <-
  function(signals,
           fnodatafolder,
           equitydatafolder,
           rollover = FALSE) {
    out = data.frame(
      date = as.POSIXct(character()),
      symbol = character(),
      buy = numeric(),
      sell = numeric(),
      short = numeric(),
      cover = numeric(),
      buyprice = numeric(),
      sellprice = numeric(),
      shortprice = numeric(),
      coverprice = numeric(),
      stringsAsFactors = FALSE
    )

    # amend signals for any rollover
    signals$inlongtrade <- RTrade::Flip(signals$buy, signals$sell)
    signals$inshorttrade <-
      RTrade::Flip(signals$short, signals$cover)

    if (rollover) {
      signals$rolloverdate <-
        signals$currentmonthexpiry != signals$entrycontractexpiry &
        signals$entrycontractexpiry != Ref(signals$entrycontractexpiry, -1)
      signals$rolloverorders <-
        signals$rolloverdate &
        ((
          signals$inlongtrade &
            Ref(signals$inlongtrade, -1)
        ) |
          (
            signals$inshorttrade & Ref(signals$inshorttrade, -1)
          ))
      for (i in 1:nrow(signals)) {
        if (signals$rolloverorders[i] == TRUE) {
          if (signals$inlongtrade[i] == 1) {
            df.copy = signals[i, ]
            signals$sell[i] = 1
            indexofbuy = getBuyIndices(signals, i)
            #indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
            for (j in 1:length(indexofbuy)) {
              if (j == 1) {
                df.copy$buy[1] = 1
              } else{
                df.copy$buy[1] = 999
              }
              df.copy$strike[1] = signals$strike[indexofbuy[j]]
              signals <- rbind(signals,
                               df.copy)
            }
          } else if (signals$inshorttrade[i] == 1) {
            df.copy = signals[i, ]
            signals$cover[i] = 1
            indexofshort = getShortIndices(signals, i)
            for (j in 1:length(indexofshort)) {
              if (j == 1) {
                df.copy$short[1] = 1
              } else{
                df.copy$short[1] = 999
              }
              df.copy$strike[1] = signals$strike[indexofshort[j]]
              signals <- rbind(signals,
                               df.copy)
            }
          }
        }
      }
    }
    signals <-
      getClosestStrikeUniverse(signals, fnodatafolder, equitydatafolder, kTimeZone)
    signals <- signals[order(signals$date), ]

    #Entry
    expiry = format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
    signals$symbol = ifelse(
      signals$buy > 0,
      paste(
        unlist(strsplit(signals$symbol, "_"))[1],
        "OPT",
        expiry,
        "PUT",
        signals$strike,
        sep = "_"
      ),
      ifelse(
        signals$short > 0,
        paste(
          unlist(strsplit(signals$symbol, "_"))[1],
          "OPT",
          expiry,
          "CALL",
          signals$strike,
          sep = "_"
        ),
        signals$symbol
      )
    )

    for (i in 1:nrow(signals)) {
      if (signals$buy[i] > 0) {
        symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
        load(
          paste(
            fnodatafolder,
            symbolsvector[3],
            "/",
            signals$symbol[i],
            ".Rdata",
            sep = ""
          )
        )
        datarow = md[md$date == signals$date[i],]
        buyprice = datarow$settle[1]
        out = rbind(
          out,
          data.frame(
            date = signals$date[i],
            symbol = signals$symbol[i],
            buy = 0,
            sell = 0,
            short = signals$buy[i],
            cover = 0,
            buyprice = 0,
            sellprice = 0,
            shortprice = buyprice,
            coverprice = 0
          )
        )

      } else if (signals$short[i] > 0) {
        symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
        load(
          paste(
            fnodatafolder,
            symbolsvector[3],
            "/",
            signals$symbol[i],
            ".Rdata",
            sep = ""
          )
        )
        datarow = md[md$date == signals$date[i],]
        shortprice = datarow$settle[1]
        out = rbind(
          out,
          data.frame(
            date = signals$date[i],
            symbol = signals$symbol[i],
            buy = 0,
            sell = 0,
            short = signals$short[i],
            cover = 0,
            buyprice = 0,
            sellprice = 0,
            shortprice = shortprice,
            coverprice = 0
          )
        )
      }
    }

    # Adjust for scenario where there is no rollover and the position is open on expiry date
    # indexofbuy = sapply(seq(1:length(signals$buy)), function(x) {
    #   index = tail(which(signals$buy[1:(x - 1)] > 0), 1)
    # })
    # indexofshort = sapply(seq(1:length(signals$short)), function(x) {
    #   index = tail(which(signals$short[1:(x - 1)] > 0), 1)
    # })
    # indexofbuy = unlist(indexofbuy)
    # indexofshort = unlist(indexofshort)
    # indexofshort = c(rep(0, (nrow(signals) - length(indexofshort))), indexofshort)
    # indexofbuy = c(rep(0, (nrow(signals) - length(indexofbuy))), indexofbuy)
    # indexofentry = pmax(indexofbuy, indexofshort)
    # for (i in 1:nrow(signals)) {
    #   symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
    #   if (length(symbolsvector) == 1 && indexofentry[i] > 0) {
    #     symbolsvector = unlist(strsplit(signals$symbol[indexofentry[i]], "_"))
    #     expiry = as.Date(strptime(symbolsvector[3], "%Y%m%d", tz = "Asia/Kolkata"))
    #     if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
    #       if (signals$cover[i] == 0 && signals$sell[i] == 0)
    #         #continuing trade. close the trade
    #         if (Ref(signals$inlongtrade, -1)[i] > 0) {
    #           signals$sell[i] = 1
    #         } else if (Ref(signals$inshorttrade, -1)[i] > 0) {
    #           signals$cover[i] = 1
    #         }
    #     }
    #   }
    # }

    for (i in 1:nrow(signals)) {
      if (signals$inlongtrade[i] > 0) {
        indexofbuy = getBuyIndices(signals, i,0)
        if (length(indexofbuy) > 0) {
          for (j in 1:length(indexofbuy)) {
            if(i>indexofbuy[j]){
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofbuy[j]],
                "_"
              ))
              expiry = as.Date(
                strptime(
                  symbolsvector[3],
                  "%Y%m%d",
                  tz = "Asia/Kolkata"
                )
              )
              if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
                signals$sell[i] = 1
              }
            }

          }

        }
      } else if (signals$inshorttrade[i] > 0) {
        indexofshort = getShortIndices(signals, i,0)
        if (length(indexofshort) > 0) {
          for (j in 1:length(indexofshort)) {
            if(i>indexofshort[j]){
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofshort[j]],
                "_"
              ))
              expiry = as.Date(
                strptime(
                  symbolsvector[3],
                  "%Y%m%d",
                  tz = "Asia/Kolkata"
                )
              )
              if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
                signals$cover[i] = 1
              }
            }

          }
        }

      }
    }

    #Exit
    for (i in 1:nrow(signals)) {
      if (signals$sell[i] > 0) {
        indexofbuy = getBuyIndices(signals, i)
        #     indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
        if (length(indexofbuy) >= 1) {
          uniquesymbols = NULL
          for (j in 1:length(indexofbuy)) {
            if (length(grep(
              signals$symbol[indexofbuy[j]],
              uniquesymbols
            )) == 0) {
              uniquesymbols[length(uniquesymbols) + 1] <-
                signals$symbol[indexofbuy[j]]
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofbuy[j]],
                "_"
              ))
              load(
                paste(
                  fnodatafolder,
                  symbolsvector[3],
                  "/",
                  signals$symbol[indexofbuy[j]],
                  ".Rdata",
                  sep = ""
                )
              )
              datarow = md[md$date == signals$date[i],]
              if (nrow(datarow) == 1) {
                load(
                  paste(
                    equitydatafolder,
                    symbolsvector[1],
                    ".Rdata",
                    sep = ""
                  )
                )
                udatarow = md[md$date == signals$date[i],]
                sellprice = NA_real_
                if (signals$sell[i] == 1) {
                  sellprice = datarow$settle[1]
                } else if (signals$sell[i] > 1) {
                  expiry = as.Date(
                    strptime(
                      symbolsvector[3],
                      "%Y%m%d",
                      tz = "Asia/Kolkata"
                    )
                  )
                  dte = businessDaysBetween(
                    "India",
                    as.Date(
                      signals$date[i],
                      tz = "Asia/Kolkata"
                    ),
                    expiry
                  )
                  vol = EuropeanOptionImpliedVolatility(
                    tolower(
                      symbolsvector[4]
                    ),
                    datarow$settle[1],
                    udatarow$settle[1],
                    as.numeric(
                      symbolsvector[5]
                    ),
                    0.015,
                    0.06,
                    dte / 365,
                    0.1
                  )
                  greeks <- EuropeanOption(
                    tolower(
                      symbolsvector[4]
                    ),
                    signals$sellprice[i],
                    as.numeric(
                      symbolsvector[5]
                    ),
                    0.015,
                    0.06,
                    dte / 365,
                    vol
                  )
                  sellprice = greeks$value
                }

                if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofbuy[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$sell[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = sellprice
                    )
                  )
                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofbuy[j]]
                  )
                  out$cover[indexofsignal] = signals$sell[i]
                  out$coverprice[indexofsignal] = sellprice
                }
              }
              # check if record is for today
              else  if (as.Date(signals$date[i],
                                tz = "Asia/Kolkata") == Sys.Date()) {
                if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofbuy[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$sell[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = 0
                    )
                  )

                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofbuy[j]]
                  )
                  out$cover[indexofsignal] = signals$sell[i]
                  out$coverprice[indexofsignal] = 0
                }
              }else{
                print(paste("Missing data for symbol ",signals$symbol[indexofbuy[j]]," for date ",signals$date[i],sep=""))
              }

            }


          }

        }
      } else if (signals$cover[i] > 0) {
        indexofshort = getShortIndices(signals, i)
        if (length(indexofshort) >= 1) {
          uniquesymbols = NULL
          for (j in 1:length(indexofshort)) {
            if (length(grep(
              signals$symbol[indexofshort[j]],
              uniquesymbols
            )) == 0) {
              uniquesymbols[length(uniquesymbols) + 1] <-
                signals$symbol[indexofshort[j]]
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofshort[j]],
                "_"
              ))
              load(
                paste(
                  fnodatafolder,
                  symbolsvector[3],
                  "/",
                  signals$symbol[indexofshort[j]],
                  ".Rdata",
                  sep = ""
                )
              )
              datarow = md[md$date == signals$date[i],]
              if (nrow(datarow) == 1) {
                load(
                  paste(
                    equitydatafolder,
                    symbolsvector[1],
                    ".Rdata",
                    sep = ""
                  )
                )
                udatarow = md[md$date == signals$date[i],]
                coverprice = NA_real_
                if (signals$cover[i] == 1) {
                  coverprice = datarow$settle[1]
                } else if (signals$cover[i] > 1) {
                  expiry = as.Date(
                    strptime(
                      symbolsvector[3],
                      "%Y%m%d",
                      tz = "Asia/Kolkata"
                    )
                  )
                  dte = businessDaysBetween(
                    "India",
                    as.Date(
                      signals$date[i],
                      tz = "Asia/Kolkata"
                    ),
                    expiry
                  )
                  vol = EuropeanOptionImpliedVolatility(
                    tolower(
                      symbolsvector[4]
                    ),
                    datarow$settle[1],
                    udatarow$settle[1],
                    as.numeric(
                      symbolsvector[5]
                    ),
                    0.015,
                    0.06,
                    dte / 365,
                    0.1
                  )
                  greeks <- EuropeanOption(
                    tolower(
                      symbolsvector[4]
                    ),
                    signals$coverprice[i],
                    as.numeric(
                      symbolsvector[5]
                    ),
                    0.015,
                    0.06,
                    dte / 365,
                    vol
                  )
                  coverprice = greeks$value
                }

                if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofshort[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$cover[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = coverprice
                    )
                  )
                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofshort[j]]
                  )
                  out$cover[indexofsignal] = signals$cover[i]
                  out$coverprice[indexofsignal] = coverprice
                }
              }     # check if record is for today
              else if (as.Date(signals$date[i],
                               tz = "Asia/Kolkata") == Sys.Date()) {
                if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofshort[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$cover[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = 0
                    )
                  )

                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofshort[j]]
                  )
                  out$cover[indexofsignal] = signals$cover[i]
                  out$coverprice[indexofsignal] = 0
                }
              }else{
                print(paste("Missing data for symbol ",signals$symbol[indexofshort[j]]," for date ",signals$date[i],sep=""))
              }

            }

          }

        }
      }
    }
    out[order(out$date), ]
  }

futureTradeSignals <-
  function(signals,
           fnodatafolder,
           equitydatafolder,
           rollover = FALSE) {
    out = data.frame(
      date = as.POSIXct(character()),
      symbol = character(),
      buy = numeric(),
      sell = numeric(),
      short = numeric(),
      cover = numeric(),
      buyprice = numeric(),
      sellprice = numeric(),
      shortprice = numeric(),
      coverprice = numeric(),
      stringsAsFactors = FALSE
    )
    signals$inlongtrade <- RTrade::Flip(signals$buy, signals$sell)
    signals$inshorttrade <-
      RTrade::Flip(signals$short, signals$cover)

    # amend signals for any rollover
    signals$inlongtrade <- RTrade::Flip(signals$buy, signals$sell)
    signals$inshorttrade <-
      RTrade::Flip(signals$short, signals$cover)

    if (rollover) {
      # signals$rolloverdate <-
      #         signals$currentmonthexpiry != signals$entrycontractexpiry &
      #         signals$entrycontractexpiry != Ref(signals$entrycontractexpiry, -1)
      signals$rolloverdate <-
        signals$entrycontractexpiry != Ref(signals$entrycontractexpiry, -1)

      signals$rolloverorders <-  signals$rolloverdate &
        ((
          signals$inlongtrade & Ref(signals$inlongtrade, -1)
        ) |
          (
            signals$inshorttrade & Ref(signals$inshorttrade, -1)
          ))
      for (i in 1:nrow(signals)) {
        if (signals$rolloverorders[i] == TRUE) {
          if (signals$inlongtrade[i] == 1) {
            df.copy = signals[i, ]
            signals$sell[i] = 9
            indexofbuy = getBuyIndices(signals, i)
            #indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
            for (j in 1:length(indexofbuy)) {
              df.copy$buy[1] = signals$buy[indexofbuy[j]]
              # if (j == 1) {
              #               df.copy$buy[1] = signals[indexofbuy[j],"buy"]
              #       } else{
              #               df.copy$buy[1] = 999
              #       }
              df.copy$strike[1] = signals$strike[indexofbuy[j]]
              signals <- rbind(signals, df.copy)
            }
          } else if (signals$inshorttrade[i] == 1) {
            df.copy = signals[i, ]
            signals$cover[i] = 9
            indexofshort = getShortIndices(signals, i)
            for (j in 1:length(indexofshort)) {
              df.copy$short[1] = signals$short[indexofshort[j]]
              # if (j == 1) {
              #         df.copy$short[1] = 1
              # } else{
              #         df.copy$short[1] = 999
              # }
              df.copy$strike[1] = signals$strike[indexofshort[j]]
              signals <- rbind(signals, df.copy)
            }
          }
        }
      }
    }
    signals <- signals[order(signals$date), ]
    expiry = format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
    signals$symbol = ifelse(
      signals$buy > 0,
      paste(unlist(strsplit(
        signals$symbol, "_"
      ))[1], "FUT", expiry, "", "", sep = "_"),
      ifelse(
        signals$short > 0,
        paste(unlist(strsplit(
          signals$symbol, "_"
        ))[1], "FUT", expiry, "", "", sep = "_"),
        signals$symbol
      )
    )
    #Entry
    for (i in 1:nrow(signals)) {
      if (signals$buy[i] > 0) {
        symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
        load(
          paste(
            fnodatafolder,
            symbolsvector[3],
            "/",
            signals$symbol[i],
            ".Rdata",
            sep = ""
          )
        )
        datarow = md[md$date == signals$date[i],]
        buyprice = datarow$settle[1]
        out = rbind(
          out,
          data.frame(
            date = signals$date[i],
            symbol = signals$symbol[i],
            buy = signals$buy[i],
            sell = 0,
            short = 0,
            cover = 0,
            buyprice = buyprice,
            sellprice = 0,
            shortprice = 0,
            coverprice = 0
          )
        )

      } else if (signals$short[i] > 0) {
        symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
        load(
          paste(
            fnodatafolder,
            symbolsvector[3],
            "/",
            signals$symbol[i],
            ".Rdata",
            sep = ""
          )
        )
        datarow = md[md$date == signals$date[i],]
        shortprice = datarow$settle[1]
        out = rbind(
          out,
          data.frame(
            date = signals$date[i],
            symbol = signals$symbol[i],
            buy = 0,
            sell = 0,
            short = signals$short[i],
            cover = 0,
            buyprice = 0,
            sellprice = 0,
            shortprice = shortprice,
            coverprice = 0
          )
        )
      }
    }

    # Adjust for scenario where there is no rollover and the position is open on expiry date
    # indexofbuy = sapply(seq(1:length(signals$buy)), function(x) {
    #   index = tail(which(signals$buy[1:(x - 1)] > 0), 1)
    # })
    # indexofshort = sapply(seq(1:length(signals$short)), function(x) {
    #   index = tail(which(signals$short[1:(x - 1)] > 0), 1)
    # })
    # indexofbuy = unlist(indexofbuy)
    # indexofshort = unlist(indexofshort)
    # indexofshort = c(rep(0, (nrow(signals) - length(indexofshort))), indexofshort)
    # indexofbuy = c(rep(0, (nrow(signals) - length(indexofbuy))), indexofbuy)
    # indexofentry = pmax(indexofbuy, indexofshort)
    # for (i in 1:nrow(signals)) {
    #   symbolsvector = unlist(strsplit(signals$symbol[i], "_"))
    #   if (length(symbolsvector) == 1 && indexofentry[i] > 0) {
    #     symbolsvector = unlist(strsplit(signals$symbol[indexofentry[i]], "_"))
    #     expiry = as.Date(strptime(symbolsvector[3], "%Y%m%d", tz = "Asia/Kolkata"))
    #     if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
    #       if (signals$cover[i] == 0 && signals$sell[i] == 0)
    #         #continuing trade. close the trade
    #         if (Ref(signals$inlongtrade, -1)[i] > 0) {
    #           signals$sell[i] = 1
    #         } else if (Ref(signals$inshorttrade, -1)[i] > 0) {
    #           signals$cover[i] = 1
    #         }
    #     }
    #   }
    # }
    if(!rollover){
      for (i in 1:nrow(signals)) {
        if (signals$inlongtrade[i] > 0) {
          indexofbuy = getBuyIndices(signals, i,0)
          if (length(indexofbuy) > 0) {
            for (j in 1:length(indexofbuy)) {
              if(i>indexofbuy[j]){
                symbolsvector = unlist(strsplit(
                  signals$symbol[indexofbuy[j]],
                  "_"
                ))
                expiry = as.Date(
                  strptime(
                    symbolsvector[3],
                    "%Y%m%d",
                    tz = "Asia/Kolkata"
                  )
                )
                if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
                  signals$sell[i] = 1
                }
              }

            }

          }
        } else if (signals$inshorttrade[i] > 0) {
          indexofshort = getShortIndices(signals, i,0)
          if (length(indexofshort) > 0) {
            for (j in 1:length(indexofshort)) {
              if(i>indexofshort[j]){
                symbolsvector = unlist(strsplit(
                  signals$symbol[indexofshort[j]],
                  "_"
                ))
                expiry = as.Date(
                  strptime(
                    symbolsvector[3],
                    "%Y%m%d",
                    tz = "Asia/Kolkata"
                  )
                )
                if (expiry == as.Date(signals$date[i], tz = "Asia/Kolkata")) {
                  signals$cover[i] = 1
                }
              }
            }
          }
        }
      }
    }


    #Exit
    for (i in 1:nrow(signals)) {
      if (signals$sell[i] > 0) {
        indexofbuy = getBuyIndices(signals, i)
        #     indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
        if (length(indexofbuy) >= 1) {
          uniquesymbols = NULL
          for (j in 1:length(indexofbuy)) {
            if (length(grep(
              signals$symbol[indexofbuy[j]],
              uniquesymbols
            )) == 0) {
              uniquesymbols[length(uniquesymbols) + 1] <-
                signals$symbol[indexofbuy[j]]
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofbuy[j]],
                "_"
              ))
              load(
                paste(
                  fnodatafolder,
                  symbolsvector[3],
                  "/",
                  signals$symbol[indexofbuy[j]],
                  ".Rdata",
                  sep = ""
                )
              )
              md<-unique(md)
              datarow = md[md$date == signals$date[i],]
              if (nrow(datarow) == 1) {
                load(
                  paste(
                    equitydatafolder,
                    symbolsvector[1],
                    ".Rdata",
                    sep = ""
                  )
                )
                md<-unique(md)
                udatarow = md[md$date == signals$date[i],]
                spread = NA_real_
                if (datarow$open[1] != datarow$high[1] ||
                    datarow$open[1] != datarow$low[1]) {
                  spread = (
                    datarow$high[1] + datarow$low[1] + datarow$settle[1] - (
                      udatarow$high[1] +
                        udatarow$low[1] + udatarow$settle[1]
                    )
                  ) / 3
                } else if (datarow$open[1] == datarow$high[1]) {
                  spread = (
                    datarow$low[1] + datarow$settle[1] - (
                      udatarow$low[1] + udatarow$settle[1]
                    )
                  ) /
                    2
                } else{
                  spread = (
                    datarow$high[1] + datarow$settle[1] - (
                      udatarow$high[1] + udatarow$settle[1]
                    )
                  ) /
                    2
                }
                sellprice = NA_real_
                if (signals$sell[i] == 1||signals$sell[i] == 9) {
                  sellprice = datarow$settle[1]
                } else if (signals$sell[i] > 1) {
                  splitratio=udatarow$splitadjust[1]
                  sellprice = signals$sellprice[i]*splitratio + spread
                }
                if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofbuy[j]],
                      buy = 0,
                      sell = signals$sell[i],
                      short = 0,
                      cover = 0,
                      buyprice = 0,
                      sellprice = sellprice,
                      shortprice = 0,
                      coverprice = 0
                    )
                  )
                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofbuy[j]]
                  )
                  out$sell[indexofsignal] = signals$sell[i]
                  out$sellprice[indexofsignal] = sellprice
                }
              }       # check if record is for today
              else if (as.Date(signals$date[i],
                               tz = "Asia/Kolkata") == Sys.Date()) {
                if (signals$symbol[i] != signals$symbol[indexofbuy[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofbuy[j]],
                      buy = 0,
                      sell = signals$sell[i],
                      short = 0,
                      cover = 0,
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = 0
                    )
                  )

                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofbuy[j]]
                  )
                  out$sell[indexofsignal] = signals$sell[i]
                  out$sellprice[indexofsignal] = 0
                }
              }else{
                print(paste("Missing data for symbol ",signals$symbol[indexofbuy[j]]," for date ",signals$date[i],sep=""))
              }

            }
          }

        }
      } else if (signals$cover[i] > 0) {
        indexofshort = getShortIndices(signals, i)
        if (length(indexofshort) >= 1) {
          uniquesymbols = NULL
          for (j in 1:length(indexofshort)) {
            if (length(grep(
              signals$symbol[indexofshort[j]],
              uniquesymbols
            )) == 0) {
              uniquesymbols[length(uniquesymbols) + 1] <-
                signals$symbol[indexofshort[j]]
              symbolsvector = unlist(strsplit(
                signals$symbol[indexofshort[j]],
                "_"
              ))
              load(
                paste(
                  fnodatafolder,
                  symbolsvector[3],
                  "/",
                  signals$symbol[indexofshort[j]],
                  ".Rdata",
                  sep = ""
                )
              )
              md<-unique(md)
              datarow = md[md$date == signals$date[i],]
              if (nrow(datarow) == 1) {
                load(
                  paste(
                    equitydatafolder,
                    symbolsvector[1],
                    ".Rdata",
                    sep = ""
                  )
                )
                md<-unique(md)
                udatarow = md[md$date == signals$date[i],]
                spread = NA_real_
                if (datarow$open[1] != datarow$high[1] ||
                    datarow$open[1] != datarow$low[1]) {
                  spread = (
                    datarow$high[1] + datarow$low[1] + datarow$settle[1] - (
                      udatarow$high[1] +
                        udatarow$low[1] + udatarow$settle[1]
                    )
                  ) / 3
                } else if (datarow$open[1] == datarow$high[1]) {
                  spread = (
                    datarow$low[1] + datarow$settle[1] - (
                      udatarow$low[1] + udatarow$settle[1]
                    )
                  ) /
                    2
                } else{
                  spread = (
                    datarow$high[1] + datarow$settle[1] - (
                      udatarow$high[1] + udatarow$settle[1]
                    )
                  ) /
                    2
                }
                coverprice = NA_real_
                if (signals$cover[i] == 1|signals$cover[i] == 9) {
                  coverprice = datarow$settle[1]
                } else if (signals$cover[i] > 1) {
                  splitratio=udatarow$splitadjust[1]
                  coverprice = signals$coverprice[i]*splitratio + spread
                  #print(paste("symbol:",udatarow$symbol[1],", date:",udatarow$date[1],", coverprice:",signals$coverprice[i], ", splitratio:",splitratio,",spread:",spread))
                }

                if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofshort[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$cover[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = coverprice
                    )
                  )
                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofshort[j]]
                  )
                  out$cover[indexofsignal] = signals$cover[i]
                  out$coverprice[indexofsignal] = coverprice
                }
              }       # check if record is for today
              else if (as.Date(signals$date[i],
                               tz = "Asia/Kolkata") == Sys.Date()) {
                if (signals$symbol[i] != signals$symbol[indexofshort[j]]) {
                  # add row to signals
                  out = rbind(
                    out,
                    data.frame(
                      date = signals$date[i],
                      symbol = signals$symbol[indexofshort[j]],
                      buy = 0,
                      sell = 0,
                      short = 0,
                      cover = signals$cover[i],
                      buyprice = 0,
                      sellprice = 0,
                      shortprice = 0,
                      coverprice = 0
                    )
                  )

                } else{
                  indexofsignal = which(
                    out$date == signals$date[i] &
                      out$symbol == signals$symbol[indexofshort[j]]
                  )
                  out$cover[indexofsignal] = signals$cover[i]
                  out$coverprice[indexofsignal] = 0
                }
              }else{
                print(paste("Missing data for symbol ",signals$symbol[indexofshort[j]]," for date ",signals$date[i],sep=""))
              }

            }
          }
        }
      }
    }
    out[order(out$date), ]
  }

MapToFutureTrades<-function(itrades,fnodatafolder,equitydatafolder,rollover=FALSE){
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
  itrades$symbol = paste(itrades$symbol, "FUT", expiry, "", "", sep = "_")

  # Substitute Price Array
  for(i in 1:nrow(itrades)){
    itrades$entryprice[i]=futureTradePrice(itrades$symbol[i],itrades$entrytime[i],itrades$entryprice[i],kFNODataFolder,kNiftyDataFolder)
    itrades$exitprice[i]=futureTradePrice(itrades$symbol[i],itrades$exittime[i],itrades$exitprice[i],kFNODataFolder,kNiftyDataFolder)
  }
  itrades[order(itrades$entrytime),]
}

futureTradePrice<-function(futureSymbol,tradedate,underlyingtradeprice,fnodatafolder,equitydatafolder){
  # Return unadjusted futureprice for the tradedate
  underlying=sapply(strsplit(futureSymbol[1],"_"),"[",1)
  expiry=sapply(strsplit(futureSymbol[1],"_"),"[",3)
  load(paste(equitydatafolder,underlying[1],".Rdata",sep=""))
  md<-unique(md)
  underlyingprice=md[md$date==tradedate,]
  adjustment=0
  load(paste(fnodatafolder,expiry,"/",futureSymbol[1],".Rdata",sep=""))
  md<-unique(md)
  futureprice=md[md$date==tradedate,]
  if(nrow(futureprice)==1 & nrow(underlyingprice)==1){
    if(underlyingtradeprice==underlyingprice$aopen[1]){
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

sharpe <- function(returns, risk.free.rate = 0.07) {
  sqrt(252) * (mean(returns) - (risk.free.rate / 365)) / sd(returns)
}

getAllStrikesForExpiry <-
  function(underlyingshortname,
           expiry,
           fnodatafolder) {
    potentialSymbols = list.files(
      paste(fnodatafolder, expiry, sep = ""),
      pattern = paste(underlyingshortname, "_", sep = "")
    )
    b <- lapply(potentialSymbols, strsplit, split = "_")
    c <- sapply(b, tail, n = 1L)
    d <- sapply(c, tail, n = 1L)
    e <- sapply(d, strsplit, "\\.Rdata")
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
           expiry,
           fnodatafolder,
           equitydatafolder) {
    # date is posixct
    # expiry is string %Y%m%d
    # underlyingshortname is just the symbol short name like RELIANCE
    strikes <-
      getAllStrikesForExpiry(underlyingshortname, expiry, fnodatafolder)
    strikes = strikes[complete.cases(strikes)]
    load(
      paste(
        fnodatafolder,
        expiry,
        "/",
        underlyingshortname,
        "_FUT_",
        expiry,
        "__",
        ".Rdata",
        sep = ""
      )
    )
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
      newrow <- getPriceArrayFromRedis(9,longsymbol,"tick","close",paste(today, " 09:12:00"), paste(today, " 15:30:00"))
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
  function(dfsignals,
           fnodatafolder,
           equitydatafolder,
           timeZone) {
    dfsignals$strike = NA_real_
    for (i in 1:nrow(dfsignals)) {
      if (dfsignals$buy[i] > 0 ||
          dfsignals$sell[i] > 0 || dfsignals$short[i] > 0 ||
          dfsignals$cover[i] > 0) {
        symbolsvector = unlist(strsplit(dfsignals$symbol[i], "_"))
        dfsignals$strike[i] = getClosestStrike(dfsignals$date[i],symbolsvector[1],strftime(dfsignals$entrycontractexpiry[i],"%Y%m%d",tz = timeZone),fnodatafolder,equitydatafolder)
      }
    }
    dfsignals
    #dfsignals[!is.na(dfsignals$strike),]

  }

getStrikeByClosestSettlePrice <-
  function(trades,
           fnodatafolder,
           equitydatafolder,
           timeZone) {
    trades$strike = NA_real_
    for (i in 1:nrow(trades)) {
      symbolsvector = unlist(strsplit(trades$symbol[i], "_"))
      trades$strike[i] = getClosestStrike(trades$entrytime[i],symbolsvector[1],strftime(trades$entrycontractexpiry[i],"%Y%m%d",tz = timeZone),fnodatafolder,equitydatafolder)
    }
    trades
    #dfsignals[!is.na(dfsignals$strike),]

  }

getMaxOIStrike <-
  function(dates,
           underlyingshortname,
           expiry,
           fnodatafolder,
           equitydatafolder,
           right) {
    strikesUniverse <-
      getAllStrikesForExpiry(underlyingshortname, expiry, fnodatafolder)
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
        load(paste(
          fnodatafolder,
          expiry,
          "/",
          symbols[s],
          sep = ""
        ))
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
  function(symbol,
           start=NULL,
           end=NULL,
           realtime = FALSE,
           type = "STK",
           fnodatafolder = "/home/psharma/Dropbox/rfiles/dailyfno/",
           equitydatafolder = "/home/psharma/Dropbox/rfiles/daily/",...) {
      md<-loadSymbol(symbol,realtime,type,...)
    if (symbol == "NSENIFTY") {
      md$aclose = md$asettle
    }
    if(is.null(nrow(md))){
      symbols=sapply(strsplit(list.files(equitydatafolder),"\\."),'[',1)
      shortlist=symbols[grep(substr(symbol,1,5),symbols)]
      if(length(shortlist)>0){
        print(paste("Did you mean..",paste(shortlist,collapse=" or "),sep=" "))
      }
      return()
    }
    symbolname=NULL
    if(length(grep("aopen",names(md)))>0){
      symbolname = convertToXTS(md, c("aopen", "ahigh", "alow", "asettle", "avolume"))
    }else{
      symbolname = convertToXTS(md, c("open", "high", "low", "settle", "volume"))
    }
    #      symbolname = convertToXTS(md, c("aopen", "ahigh", "alow", "aclose", "avolume"))
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
    if(is.null(start)){
      start=''
    }
    if(is.null(end)){
      end=''
    }

    chartSeries(symbolname,
                subset = paste(start, "::", end, sep = ""),
                theme = customTheme,
                name=name)
    symbolname[paste(start, "::", end, sep = "")]
  }

QuickChart<-function(symbol,startdate=NULL,enddate=NULL,realtime=FALSE,type="STK",...){
  #symbol=deparse(substitute(symbol))
  #type=deparse(substitute(type))
  out<-chart(symbol,startdate,enddate,realtime,type,...)
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



changeTimeFrame<-function(md,sourceDuration=NULL, destDuration=NULL){
  names<-as.character()
  data<-xts()
  index<-as.integer()
  out<-data.frame()
  if(!is.null(nrow(md)) & !is.null(sourceDuration) & !is.null(destDuration)){
    if(sourceDuration=="DAILY"){
      if(destDuration=="WEEKLY"){
        names<-names(md)
        names<-names[!names %in% c("symbol","date")]
        data<-convertToXTS(md,names)
        fridays = as.POSIXlt(time(data))$wday == 5
        indx <- c(0,which(fridays),nrow(data))
      }else if(destDuration=="MONTHLY"){
        names<-names(md)
        names<-names[!names %in% c("symbol","date")]
        data<-convertToXTS(md,names)
        months = as.POSIXlt(time(data))$mon
        indx <- c(0,which(diff(months)!=0),nrow(data))
      }
    }

    if(length(names)>0)
      for(i in seq_along(names)){
        if(grepl("aopen",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=first))
        }
        if(grepl("ahigh",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=max))
        }
        if(grepl("alow",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=min))
        }
        if(grepl("aclose",names[i])|grepl("asettle",names[i])||grepl("split",names[i])|grepl("interest",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=last))
        }
        if(grepl("avolume",names[i])|grepl("adelivered",names[i])){
          assign(names[i],period.apply(data[,names[i]], INDEX=indx, FUN=sum))
        }
      }
    if(length(which(names=="aopen"))==1){
      assign("open",get("aopen")*get("splitadjust"))
      colnames(open)<-"open"
    }
    if(length(which(names=="ahigh"))==1){
      assign("high",get("ahigh")*get("splitadjust"))
      colnames(high)<-"high"
    }
    if(length(which(names=="alow"))==1){
      assign("low",get("alow")*get("splitadjust"))
      colnames(low)<-"low"
    }
    if(length(which(names=="asettle"))==1){
      assign("settle",get("asettle")*get("splitadjust"))
      colnames(settle)<-"settle"
    }
    if(length(which(names=="aclose"))==1){
      assign("close",get("aclose")*get("splitadjust"))
      colnames(close)<-"close"
    }
    if(length(which(names=="avolume"))==1){
      assign("volume",get("avolume")/get("splitadjust"))
      colnames(volume)<-"volume"
    }
    if(length(which(names=="adelivered"))==1){
      assign("delivered",get("adelivered")/get("splitadjust"))
      colnames(delivered)<-"delivered"
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

loadSymbol<-function(symbol,realtime=FALSE,type="STK",sourceDuration=NULL,destDuration=NULL,fnodatafolder = "/home/psharma/Dropbox/rfiles/dailyfno/",
                     equitydatafolder = "/home/psharma/Dropbox/rfiles/daily/"){
  symbolsvector = unlist(strsplit(symbol, "_"))
  filefound=FALSE
  if(length(symbolsvector)==1){
    fileName=paste(equitydatafolder,symbol,".Rdata",sep="")
    if(file.exists(fileName)){
      load(fileName)
      filefound=TRUE
    }
  }else{
    fileName=paste(fnodatafolder,symbolsvector[3],"/",symbol,".Rdata",sep ="")
    if(file.exists(fileName)){
      load(fileName)
      filefound=TRUE
    }
  }
  if(filefound && realtime && !is.na(type)){
    today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
    newrow=RTrade::getPriceArrayFromRedis(9,paste(symbol,"_",type,"___",sep=""),"tick","close",paste(today, " 09:12:00"),paste(today, " 15:30:00"))
    if(nrow(newrow)==1){
      if(!is.na(type)){
        if(type=="STK"){
          newrow <-
            data.frame(
              "symbol" = symbol,
              "date" = newrow$date[1],
              "open" = newrow$open[1],
              "high" = newrow$high[1],
              "low" = newrow$low[1],
              "close" = newrow$close[1],
              "settle" = newrow$close[1],
              "delivered"=0,
              "volume" = 0,
              "aopen" = newrow$open[1],
              "ahigh" = newrow$high[1],
              "alow" = newrow$low[1],
              "aclose" = newrow$close[1],
              "asettle" = newrow$close[1],
              "adelivered"=0,
              "avolume" = 0,
              "splitadjust" = 1
            )
        }else if(type=="IND"){
          newrow <-
            data.frame(
              "symbol" = symbol,
              "date" = newrow$date[1],
              "open" = newrow$open[1],
              "high" = newrow$high[1],
              "low" = newrow$low[1],
              "close" = newrow$close[1],
              "settle" = newrow$close[1],
              "volume" = 0,
              "aopen" = newrow$open[1],
              "ahigh" = newrow$high[1],
              "alow" = newrow$low[1],
              "aclose" = newrow$close[1],
              "asettle" = newrow$close[1],
              "avolume" = 0,
              "splitadjust" = 1
            )
        }

      }else{
        newrow <-
          data.frame(
            "symbol" = symbol,
            "date" = newrow$date[1],
            "open" = newrow$open[1],
            "high" = newrow$high[1],
            "low" = newrow$low[1],
            "close" = newrow$close[1],
            "settle" = newrow$close[1],
            "volume" = 0,
            "oi"=0
          )
      }
    }
    md <- rbind(md, newrow)
  }
  if(filefound){
    md<-unique(md)
  }else{
    md<-NA_character_
  }
  md<-changeTimeFrame(md,sourceDuration,destDuration)
  md
}

getSplitInfo<-function(symbol="",complete=FALSE){
  rredis::redisConnect()
  rredis::redisSelect(2)
  if(symbol==""){
    a <-  unlist(rredis::redisSMembers("splits")) # get values from redis in a vector
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

xirr <- function(cf, dates) {
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
