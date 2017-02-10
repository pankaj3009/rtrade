# Tabulate index changes
library(rredis)
library(RQuantLib)

createIndexConstituents<-function(redisdb,pattern,threshold="2000-01-01"){
  redisConnect()
  redisSelect(redisdb)
  rediskeys=redisKeys()
  dfstage1 <- data.frame(symbol=character(),
                         startdate=as.Date(character()),
                         stringsAsFactors=FALSE)
  dfstage2 <- data.frame(symbol=character(),
                         startdate=as.Date(character()),
                         enddate=as.Date(character()),
                         stringsAsFactors=FALSE)
  symbols <- data.frame(symbol=character(),
                        startdate=as.Date(character()),
                        enddate=as.Date(character()),
                        stringsAsFactors=FALSE)
  rediskeysShortList<-as.character()
  thresholdDate=as.Date(threshold,format="%Y-%m-%d",tz="Asia/Kolkata")
  for(i in 1:length(rediskeys)){
    l=length(grep(pattern,rediskeys[i]))
    if(l>0){
      if(thresholdDate<as.Date(substring(rediskeys[i],nchar(pattern)+2),format="%Y%m%d",tz="Asia/Kolkata")){
        rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
      }
    }
  }
  rediskeysShortList<-sort(rediskeysShortList)
  for(i in 1:length(rediskeysShortList)){
    seriesstartdate=substring(rediskeysShortList[i],nchar(pattern)+2)
    seriesenddate=ifelse(i==length(rediskeysShortList),Sys.Date(),as.Date(substring(rediskeysShortList[i+1],nchar(pattern)+2),format="%Y%m%d",tz="Asia/Kolkata")-1)
    symbols<-data.frame(symbol=unlist(redisSMembers(rediskeysShortList[i])),startdate=as.Date(seriesstartdate,format="%Y%m%d",tz="Asia/Kolkata"),enddate=as.Date(seriesenddate,origin="1970-01-01"),stringsAsFactors = FALSE)
    for(j in 1:nrow(symbols)){
      existingindex=which(dfstage2$symbol==symbols$symbol[j] & dfstage2$enddate==symbols$startdate[j]-1)
      if(length(existingindex)>0){
        #replace row in dfstage2
        dfstage2[existingindex,]<-data.frame(symbol=symbols$symbol[j],startdate=dfstage2[existingindex,c("startdate")],enddate=symbols$enddate[j],stringsAsFactors=FALSE)
      }else{
        dfstage2=rbind(dfstage2,data.frame(symbol=symbols$symbol[j],startdate=symbols$startdate[j],enddate=symbols$enddate[j],stringsAsFactors=FALSE))
      }
    }
  }
  redisClose()
  dfstage2

}

createFNOSize<-function(redisdb,pattern,threshold="2000-01-01"){
  redisConnect()
  redisSelect(redisdb)
  rediskeys=redisKeys()
  dfstage2 <- data.frame(symbol=character(),
                         contractsize=numeric(),
                         startdate=as.Date(character()),
                         enddate=as.Date(character()),
                         stringsAsFactors=FALSE)
  symbols <- data.frame(symbol=character(),
                        contractsize=numeric(),
                        startdate=as.Date(character()),
                        enddate=as.Date(character()),
                        stringsAsFactors=FALSE)
  rediskeysShortList<-character()
  thresholdDate=as.Date(threshold,format="%Y-%m-%d",tz="Asia/Kolkata")
  for(i in 1:length(rediskeys)){
    l=length(grep(pattern,rediskeys[i]))
    if(l>0){
      if(thresholdDate<as.Date(substring(rediskeys[i],nchar(pattern)+2),format="%Y%m%d",tz="Asia/Kolkata")){
        rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
      }
    }
  }
  rediskeysShortList<-sort(rediskeysShortList)

  for(i in 1:length(rediskeysShortList)){
    seriesenddate=substring(rediskeysShortList[i],nchar(pattern)+2)
    seriesstartdate=ifelse(i==1,as.Date("2000-01-01"),as.Date(substring(rediskeysShortList[i-1],nchar(pattern)+2),format="%Y%m%d",tz="Asia/Kolkata")+1)
    symbolsunformatted<-unlist(redisHGetAll(rediskeysShortList[i]))
    symbols<-data.frame(symbol=names(symbolsunformatted),contractsize=as.numeric(symbolsunformatted),enddate=as.Date(seriesenddate,format="%Y%m%d",tz="Asia/Kolkata"),startdate=as.Date(seriesstartdate,origin="1970-01-01"),stringsAsFactors = FALSE)
    for(j in 1:nrow(symbols)){
      existingindex=which(dfstage2$symbol==symbols$symbol[j] &dfstage2$contractsize==symbols$contractsize[j] & dfstage2$enddate==symbols$startdate[j]-1)
      if(length(existingindex)>0){
        #replace row in dfstage2
        dfstage2[existingindex,]<-data.frame(symbol=symbols$symbol[j],contractsize=symbols$contractsize[j],startdate=dfstage2[existingindex,c("startdate")],enddate=symbols$enddate[j],stringsAsFactors=FALSE)
      }else{
        dfstage2=rbind(dfstage2,data.frame(symbol=symbols$symbol[j],contractsize=symbols$contractsize[j],startdate=symbols$startdate[j],enddate=symbols$enddate[j],stringsAsFactors=FALSE))
      }
    }
  }
  redisClose()
  dfstage2

}

getMostRecentSymbol<-function(symbol,original,final){
  out<-linkedsymbols(final,original,symbol)
  out<-out[length(out)]
  out
}

readAllSymbols<-function(redisdb,pattern,threshold="2000-01-01"){
  redisConnect()
  redisSelect(redisdb)
  rediskeys=redisKeys()
  symbols <- data.frame(brokersymbol=character(),
                        exchangesymbol=character(),
                        stringsAsFactors=FALSE)
  rediskeysShortList<-as.character()
  for(i in 1:length(rediskeys)){
    l=length(grep(pattern,rediskeys[i]))
    if(l>0){
      rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
    }
  }
  rediskeysShortList<-sort(rediskeysShortList)
  rediskeysShortList<-rediskeysShortList[length(rediskeysShortList)]
  symbols<-unlist(redisHGetAll(rediskeysShortList))
  symbols<-data.frame(exchangesymbol=names(symbols),brokersymbol=symbols,stringsAsFactors = FALSE)
  return(symbols)
}

createPNLSummary<-function(redisdb,pattern,start,end,mdpath){
  #pattern = strategy name (Case sensitive)
  #start,end = string as "yyyy-mm-dd"
  #mdpath = path to market data files for valuing open positions
  #realtrades<-createPNLSummary(0,"swing01","2017-01-01","2017-01-31","/home/psharma/Seafile/rfiles/daily-fno/")
  redisConnect()
  redisSelect(redisdb)
  rediskeys=redisKeys()
  actualtrades <- data.frame(symbol=character(),trade=character(),entrysize=as.numeric(),entrytime=as.POSIXct(character()),
                             entryprice=numeric(),exittime=as.POSIXct(character()),exitprice=as.numeric(),
                             percentprofit=as.numeric(),bars=as.numeric(),brokerage=as.numeric(),
                             netpercentprofit=as.numeric(),netprofit=as.numeric(),key=character(),stringsAsFactors = FALSE
  )
  periodstartdate=as.Date(start,tz="Asia/Kolkata")
  periodenddate=as.Date(end,tz="Asia/Kolkata")

  rediskeysShortList<-as.character()
  for(i in 1:length(rediskeys)){
    l=length(grep(pattern,rediskeys[i]))
    if(l>0){
      if(grepl("opentrades",rediskeys[i])||grepl("closedtrades",rediskeys[i])){
        rediskeysShortList<-rbind(rediskeysShortList,rediskeys[i])
      }
    }
  }
  rediskeysShortList<-sort(rediskeysShortList)
  # loop through keys and generate pnl
  for(i in 1:length(rediskeysShortList)){
    data<-unlist(redisHGetAll(rediskeysShortList[i]))
    exitprice=0
    entrydate=data["entrytime"]
    exitdate=data["exittime"]
    entrydate=as.Date(entrydate,tz="Asia/Kolkata")
    exitdate=as.Date(ifelse(is.na(exitdate),Sys.Date(),as.Date(exitdate,tz="Asia/Kolkata")))
    if(exitdate>=periodstartdate && entrydate<=periodenddate){
      if(grepl("opentrades",rediskeysShortList[i])){
        symbol=data["entrysymbol"]
        load(paste(mdpath,symbol,".Rdata",sep=""))
        index=which(as.Date(md$date,tz="Asia/Kolkata")==exitdate)
        if(length(index)==0){
          index=nrow(md)
        }
        exitprice=md$settle[index]
      }else{
        exitprice=as.numeric(data["exitprice"])
      }
      percentprofit=(exitprice-as.numeric(data["entryprice"]))/as.numeric(data["entryprice"])
      percentprofit=ifelse(data["entryside"]=="BUY",percentprofit,-percentprofit)
      brokerage=as.numeric(data["entrybrokerage"])+as.numeric(data["exitbrokerage"])
      netpercentprofit=percentprofit-(brokerage/(as.numeric(data["entryprice"])*as.numeric(data["entrysize"])))
      netprofit=(exitprice-as.numeric(data["entryprice"]))*as.numeric(data["entrysize"])-brokerage
      netprofit=ifelse(data["entryside"]=="BUY",netprofit,-netprofit)
      df=data.frame(symbol=data["parentsymbol"],trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
                    entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
                    percentprofit=percentprofit,bars=0,brokerage=brokerage,
                    netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
      )
      rownames(df)<-NULL
      actualtrades=rbind(actualtrades,df)

    }
  }
  #return(actualtrades[with(trades,order(entrytime)),])
  return(actualtrades)
}

getExpiryDate <- function(mydate) {
        #mydate = Date object
        eom = RQuantLib::getEndOfMonth(calendar = "India", as.Date(mydate, tz = "Asia/Kolkata"))
        weekday = as.POSIXlt(eom, tz = "Asia/Kolkata")$wday + 1
        adjust = weekday - 5
        if (weekday > 5) {
                #expirydate=as.Date(modAdvance(-adjust,"India",as.Date(eom),0,2))
                expirydate = eom - (weekday - 5)
        } else if (weekday == 5) {
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
        function(redisdb, symbol, duration, type, todaydate) {
                # Retrieves OHLCS prices from redisdb (currently no 9) starting from todaydate till end date.
                #symbol = redis symbol name in db=9
                #duration = [tick,daily]
                # type = [OPT,STK,FUT]
                # todaydate = starting timestamp for retrieving prices formatted as "YYYY-mm-dd HH:mm:ss"
                redisConnect()
                redisSelect(as.numeric(redisdb))
                start = as.numeric(as.POSIXct(todaydate, format = "%Y-%m-%d %H:%M:%S", tz =
                                                      "Asia/Kolkata")) * 1000
                a <-
                        redisZRangeByScore(paste(symbol, duration, type, sep = ":"), min = start, "+inf")
                redisClose()
                price = jsonlite::stream_in(textConnection(gsub("\\n", "", unlist(a))))
                if (nrow(price) > 0) {
                        price$value = as.numeric(price$value)
                        high = max(price$value)
                        low = min(price$value)
                        open = price$value[1]
                        close = price$value[nrow(price)]
                        out = data.frame(
                                date = as.POSIXct(todaydate, format = "%Y-%m-%d"),
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

GetCurrentPosition <- function(scrip, portfolio) {
        # Returns the current position for a scrip after calculating all rows in portfolio.
        #scrip = String
        #Portfolio = df containing columns[symbol,exittime,trade,size]
        position <- 0
        if (nrow(portfolio) > 0) {
                for (row in 1:nrow(portfolio)) {
                        if (is.na(portfolio[row, 'exittime']) &&
                            portfolio[row, 'symbol'] == scrip) {
                                position = position + ifelse(grepl("BUY", portfolio[row, 'trade']),
                                                             portfolio[row, 'size'],
                                                             -portfolio[row, 'size'])
                        }
                }
        }
        position
}

CalculateDailyPNL <- function(portfolio, pnl, path,brokerage) {
        #Returns realized and unrealized pnl for each day
        # Should be called after portfolio is constructed
        # portfolio = df containing columns[symbol,entrytime,exittime,entryprice,exitprice,trade,size]
        # portfolio$exitdate should be NA for rows that still have open positions.
        #pnl = pnl<-data.frame(bizdays=as.Date(subset$date,tz="Asia/Kolkata"),realized=0,unrealized=0,brokerage=0)
        # path = string containing path to market data, ending with "/"
        #brokerage = % brokerage of trade value
        #realized and unrealized profits are cumulative till date
        if (nrow(portfolio) > 0) {
                for (l in 1:nrow(portfolio)) {
                        #print(paste("portfolio line:",l,sep=""))
                        name = portfolio[l, 'symbol']
                        entrydate=as.Date(portfolio[l,'entrytime'],tz="Asia/Kolkata")
                        exitdate=as.Date(portfolio[l,'exittime'],tz="Asia/Kolkata")
                        load(paste(path, name, ".Rdata", sep = ""))
                        entryindex=which(as.Date(md$date,tz="Asia/Kolkata")==entrydate)
                        exitindex=which(as.Date(md$date,tz="Asia/Kolkata")==exitdate)
                        unrealizedpnlexists=FALSE
                        if(length(exitindex)==0){
                                # we do not have an exit date
                                exitindex=nrow(md)
                                unrealizedpnlexists=TRUE
                        }
                        mtm=portfolio[l,"entryprice"]
                        size=portfolio[l,'size']
                        dtindex=0
                        dtindexstart=which(pnl$bizdays==as.Date(md$date[entryindex],tz="Asia/Kolkata"))
                        dtindexend=which(pnl$bizdays==as.Date(md$date[exitindex],tz="Asia/Kolkata"))
                        cumunrealized=seq(0,0,length.out = (dtindexend-dtindexstart+1))
                        side=portfolio[l,'trade']
                        for(index in entryindex:(exitindex-1)){
                                #entryindex,exitindex are indices for portfolio
                                #dtindex,dtindexstart,dtindexend are indices for pnl
                                #print(index)
                                dtindex=which(pnl$bizdays==as.Date(md$date[index],tz="Asia/Kolkata"))
                                if(length(dtindex)>0){
                                  # only proceed if bizdays has the specified md$date[index] value
                                  newprice=md$settle[index]
                                  pnl$unrealized[dtindex:nrow(pnl)]<-pnl$unrealized[dtindex:nrow(pnl)]+ifelse(side=="BUY",(newprice-mtm)*size,(mtm-newprice)*size)
                                  cumunrealized[(dtindex-dtindexstart+1)]=ifelse(side=="BUY",(newprice-mtm)*size,(mtm-newprice)*size)
                                  mtm<-newprice
                                  if(index==entryindex){
                                    #pnl$unrealized[dtindex]=pnl$unrealized[dtindex]-brokerage*size
                                    #cumunrealized[(dtindex-dtindexstart+1)]=cumunrealized[(dtindex-dtindexstart+1)]-brokerage*size
                                    pnl$brokerage[dtindex:nrow(pnl)]=pnl$brokerage[dtindex:nrow(pnl)]+brokerage*size
                                  }
                                  # it is possible that bizdays does not match the md$dates!
                                }
                        }
                        lastdaypnl=ifelse(side=="BUY",(portfolio[l,'exitprice']-mtm)*size,(mtm-portfolio[l,'exitprice'])*size)
                        if(!unrealizedpnlexists){
                                if(length(dtindex)==0){
                                  dtindex=which(pnl$bizdays==as.Date(portfolio[l,"exittime"],tz="Asia/Kolkata"))-1

                                }
                                pnl$realized[(dtindex+1):nrow(pnl)]<-pnl$realized[(dtindex+1):nrow(pnl)]+sum(cumunrealized)+lastdaypnl
                                pnl$brokerage[(dtindex+1):nrow(pnl)]=pnl$brokerage[(dtindex+1):nrow(pnl)]+brokerage*size
                                #pnl$unrealized[dtindexstart:dtindexend]=pnl$unrealized[dtindexstart:dtindexend]-cumunrealized
                                pnl$unrealized[(dtindex+1):nrow(pnl)]=pnl$unrealized[(dtindex+1):nrow(pnl)]-sum(cumunrealized)
                        }else{
                                pnl$unrealized[(dtindex+1):nrow(pnl)]=pnl$unrealized[(dtindex+1):nrow(pnl)]+lastdaypnl
                        }

                }

        }
        #pnl$realized=cumsum(pnl$realized)
        #pnl$unrealized=cumsum(pnl$unrealized)
        #pnl$brokerage=cumsum(pnl$brokerage)
        pnl

}

optionTradeSignalsLongOnly<-function(signals,fnodatafolder,equitydatafolder,rollover=FALSE){
  out=data.frame(date=as.POSIXct(character()),
                 symbol=character(),
                 buy=numeric(),
                 sell=numeric(),
                 short=numeric(),
                 cover=numeric(),
                 buyprice=numeric(),
                 sellprice=numeric(),
                 shortprice=numeric(),
                 coverprice=numeric(),
                 stringsAsFactors = FALSE)

  # amend signals for any rollover
  signals$inlongtrade<-RTrade::Flip(signals$buy,signals$sell)
  signals$inshorttrade<-RTrade::Flip(signals$short,signals$cover)
  if(rollover){
    signals$rolloverdate<-signals$currentmonthexpiry!=signals$entrycontractexpiry & signals$entrycontractexpiry!=Ref(signals$entrycontractexpiry,-1)
    signals$rolloverorders<-signals$rolloverdate & ((signals$inlongtrade & Ref(signals$inlongtrade,-1))|(signals$inshorttrade & Ref(signals$inshorttrade,-1)))
    for(i in 2:nrow(signals)){
      if(signals$rolloverorders[i]==TRUE){
        if(signals$inlongtrade[i]==1){
          df.copy=signals[i,]
          signals$sell[i]=1
          indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
          df.copy$buy[1]=1
          df.copy$strike[1]=signals$strike[indexofbuy]
          signals<-rbind(signals,df.copy)
        }else if(signals$inshorttrade[i]==1){
          df.copy=signals[i,]
          signals$cover[i]=1
          indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
          df.copy$short[1]=1
          df.copy$strike[1]=signals$strike[indexofshort]
          signals<-rbind(signals,df.copy)

        }
      }
    }
  }

  signals<-signals[order(signals$date),]

  #Entry
  expiry=format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
  signals$symbol=ifelse(signals$buy>0,
                        paste(unlist(strsplit(signals$symbol,"_"))[1],"OPT",expiry,"CALL",signals$strike,sep="_"),
                        ifelse(signals$short>0,
                               paste(unlist(strsplit(signals$symbol,"_"))[1],"OPT",expiry,"PUT",signals$strike,sep="_"),
                               signals$symbol
                        )
  )
  for(i in 1:nrow(signals)){
    if(signals$buy[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      buyprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=signals$buy[i],sell=0,
                               short=0,cover=0,buyprice=buyprice,sellprice=0,shortprice=0,coverprice=0))

    }else if(signals$short[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      shortprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=signals$short[i],sell=0,
                               short=0,cover=0,buyprice=shortprice,sellprice=0,shortprice=0,coverprice=0))
    }
  }

  # Adjust for scenario where there is no rollover and the position is open on expiry date
  indexofbuy=sapply(seq(1:length(signals$buy)),function(x){index=tail(which(signals$buy[1:(x-1)]>0),1)})
  indexofshort=sapply(seq(1:length(signals$short)),function(x){index=tail(which(signals$short[1:(x-1)]>0),1)})
  indexofbuy=unlist(indexofbuy)
  indexofshort=unlist(indexofshort)
  indexofshort=c(rep(0,(nrow(signals)-length(indexofshort))),indexofshort)
  indexofbuy=c(rep(0,(nrow(signals)-length(indexofbuy))),indexofbuy)
  indexofentry=pmax(indexofbuy,indexofshort)
  for(i in 1:nrow(signals)){
    symbolsvector=unlist(strsplit(signals$symbol[i],"_"))
    if(length(symbolsvector)==1 && indexofentry[i]>0){
      symbolsvector=unlist(strsplit(signals$symbol[indexofentry[i]],"_"))
      expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
      if(expiry==as.Date(signals$date[i],tz="Asia/Kolkata")){
        if(signals$cover[i]==0 && signals$sell[i]==0)
          #continuing trade. close the trade
          if(Ref(signals$inlongtrade,-1)[i]>0){
            signals$sell[i]=1
          }else if(Ref(signals$inshorttrade,-1)[i]>0){
            signals$cover[i]=1
          }
      }
    }
  }

  #Exit
  for(i in 1:nrow(signals)){
    if(signals$sell[i]>0){
      indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
      if(length(indexofbuy)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofbuy],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofbuy], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder, symbolsvector[1], ".Rdata", sep = ""))
          udatarow = md[md$date == signals$date[i], ]
          sellprice=NA_real_
          if(signals$sell[i]==1){
            sellprice=datarow$settle[1]
          }else if (signals$sell[i]>1){
            expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
            dte=businessDaysBetween("India",as.Date(signals$date[i],tz="Asia/Kolkata"),expiry)
            vol=EuropeanOptionImpliedVolatility(
              tolower(symbolsvector[4]),
              datarow$settle[1],
              udatarow$settle[1],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              0.1
            )
            greeks<-EuropeanOption(
              tolower(symbolsvector[4]),
              signals$sellprice[i],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              vol
            )
            sellprice=greeks$value
          }

          if(signals$symbol[i]!=signals$symbol[indexofbuy]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=signals$sell[i],
                                     short=0,cover=0,buyprice=0,sellprice=sellprice,shortprice=0,coverprice=0))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
            out$sell[indexofsignal]=signals$sell[i]
            out$sellprice[indexofsignal]=sellprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofbuy]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=signals$sell[i],
                                       short=0,cover=0,buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
              out$sell[indexofsignal]=signals$sell[i]
              out$sellprice[indexofsignal]=0
            }
          }
        }
      }
    }else if(signals$cover[i]>0){
      indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
      if(length(indexofshort)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofshort],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofshort], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder, symbolsvector[1], ".Rdata", sep = ""))
          udatarow = md[md$date == signals$date[i], ]
          coverprice=NA_real_
          if(signals$cover[i]==1){
            coverprice=datarow$settle[1]
          }else if (signals$cover[i]>1){
            expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
            dte=businessDaysBetween("India",as.Date(signals$date[i],tz="Asia/Kolkata"),expiry)
            vol=EuropeanOptionImpliedVolatility(
              tolower(symbolsvector[4]),
              datarow$settle[1],
              udatarow$settle[1],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              0.1
            )
            greeks<-EuropeanOption(
              tolower(symbolsvector[4]),
              signals$coverprice[i],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              vol
            )
            coverprice=greeks$value
          }

          if(signals$symbol[i]!=signals$symbol[indexofshort]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=signals$cover[i],
                                     short=0,cover=0,buyprice=0,sellprice=coverprice,shortprice=0,coverprice=0))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
            out$sell[indexofsignal]=signals$cover[i]
            out$sellprice[indexofsignal]=coverprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofshort]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=signals$cover[i],
                                       short=0,cover=0,buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
              out$sell[indexofsignal]=signals$cover[i]
              out$sellprice[indexofsignal]=0
            }
          }
        }
      }
    }
  }
  out[order(out$date),]
}

optionTradeSignalsShortOnly<-function(signals,fnodatafolder,equitydatafolder,rollover=FALSE){
  out=data.frame(date=as.POSIXct(character()),
                 symbol=character(),
                 buy=numeric(),
                 sell=numeric(),
                 short=numeric(),
                 cover=numeric(),
                 buyprice=numeric(),
                 sellprice=numeric(),
                 shortprice=numeric(),
                 coverprice=numeric(),
                 stringsAsFactors = FALSE)

  # amend signals for any rollover
  signals$inlongtrade<-RTrade::Flip(signals$buy,signals$sell)
  signals$inshorttrade<-RTrade::Flip(signals$short,signals$cover)
  if(rollover){
    signals$rolloverdate<-signals$currentmonthexpiry!=signals$entrycontractexpiry & signals$entrycontractexpiry!=Ref(signals$entrycontractexpiry,-1)
    signals$rolloverorders<-signals$rolloverdate & ((signals$inlongtrade & Ref(signals$inlongtrade,-1))|(signals$inshorttrade & Ref(signals$inshorttrade,-1)))
    for(i in 2:nrow(signals)){
      if(signals$rolloverorders[i]==TRUE){
        if(signals$inlongtrade[i]==1){
          df.copy=signals[i,]
          signals$sell[i]=1
          indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
          df.copy$buy[1]=1
          df.copy$strike[1]=signals$strike[indexofbuy]
          signals<-rbind(signals,df.copy)
        }else if(signals$inshorttrade[i]==1){
          df.copy=signals[i,]
          signals$cover[i]=1
          indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
          df.copy$short[1]=1
          df.copy$strike[1]=signals$strike[indexofshort]
          signals<-rbind(signals,df.copy)

        }
      }
    }
  }

  signals<-signals[order(signals$date),]

  expiry=format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
  signals$symbol=ifelse(signals$buy>0,
                        paste(unlist(strsplit(signals$symbol,"_"))[1],"OPT",expiry,"PUT",signals$strike,sep="_"),
                        ifelse(signals$short>0,
                               paste(unlist(strsplit(signals$symbol,"_"))[1],"OPT",expiry,"CALL",signals$strike,sep="_"),
                               signals$symbol
                        )
  )

  #Entry
  for(i in 1:nrow(signals)){
    if(signals$buy[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      buyprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=0,sell=0,
                               short=signals$buy[i],cover=0,buyprice=0,sellprice=0,shortprice=buyprice,coverprice=0))

    }else if(signals$short[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      shortprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=0,sell=0,
                               short=signals$short[i],cover=0,buyprice=0,sellprice=0,shortprice=shortprice,coverprice=0))
    }
  }

  # Adjust for scenario where there is no rollover and the position is open on expiry date
  indexofbuy=sapply(seq(1:length(signals$buy)),function(x){index=tail(which(signals$buy[1:(x-1)]>0),1)})
  indexofshort=sapply(seq(1:length(signals$short)),function(x){index=tail(which(signals$short[1:(x-1)]>0),1)})
  indexofbuy=unlist(indexofbuy)
  indexofshort=unlist(indexofshort)
  indexofshort=c(rep(0,(nrow(signals)-length(indexofshort))),indexofshort)
  indexofbuy=c(rep(0,(nrow(signals)-length(indexofbuy))),indexofbuy)
  indexofentry=pmax(indexofbuy,indexofshort)
  for(i in 1:nrow(signals)){
    symbolsvector=unlist(strsplit(signals$symbol[i],"_"))
    if(length(symbolsvector)==1){
      symbolsvector=unlist(strsplit(signals$symbol[indexofentry[i]],"_"))
      expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
      if(expiry==as.Date(signals$date[i],tz="Asia/Kolkata")){
        if(signals$cover[i]==0 && signals$sell[i]==0)
          #continuing trade. close the trade
          if(Ref(signals$inlongtrade,-1)[i]>0){
            signals$sell[i]=1
          }else if(Ref(signals$inshorttrade,-1)[i]>0){
            signals$cover[i]=1
          }
      }
    }
  }

  #Exit
  for(i in 1:nrow(signals)){
    if(signals$sell[i]>0){
      indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
      if(length(indexofbuy)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofbuy],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofbuy], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder, symbolsvector[1], ".Rdata", sep = ""))
          udatarow = md[md$date == signals$date[i], ]
          sellprice=NA_real_
          if(signals$sell[i]==1){
            sellprice=datarow$settle[1]
          }else if (signals$sell[i]>1){
            expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
            dte=businessDaysBetween("India",as.Date(signals$date[i],tz="Asia/Kolkata"),expiry)
            vol=EuropeanOptionImpliedVolatility(
              tolower(symbolsvector[4]),
              datarow$settle[1],
              udatarow$settle[1],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              0.1
            )
            greeks<-EuropeanOption(
              tolower(symbolsvector[4]),
              signals$sellprice[i],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              vol
            )
            sellprice=greeks$value
          }

          if(signals$symbol[i]!=signals$symbol[indexofbuy]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=0,
                                     short=0,cover=signals$sell[i],buyprice=0,sellprice=0,shortprice=0,coverprice=sellprice))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
            out$cover[indexofsignal]=signals$sell[i]
            out$coverprice[indexofsignal]=sellprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofbuy]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=0,
                                       short=0,cover=signals$sell[i],buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
              out$cover[indexofsignal]=signals$sell[i]
              out$coverprice[indexofsignal]=0
            }
          }
        }
      }
    }else if(signals$cover[i]>0){
      indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
      if(length(indexofshort)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofshort],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofshort], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder, symbolsvector[1], ".Rdata", sep = ""))
          udatarow = md[md$date == signals$date[i], ]
          coverprice=NA_real_
          if(signals$cover[i]==1){
            coverprice=datarow$settle[1]
          }else if (signals$cover[i]>1){
            expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
            dte=businessDaysBetween("India",as.Date(signals$date[i],tz="Asia/Kolkata"),expiry)
            vol=EuropeanOptionImpliedVolatility(
              tolower(symbolsvector[4]),
              datarow$settle[1],
              udatarow$settle[1],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              0.1
            )
            greeks<-EuropeanOption(
              tolower(symbolsvector[4]),
              signals$coverprice[i],
              as.numeric(symbolsvector[5]),
              0.015,
              0.06,
              dte / 365,
              vol
            )
            coverprice=greeks$value
          }

          if(signals$symbol[i]!=signals$symbol[indexofshort]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=0,
                                     short=0,cover=signals$cover[i],buyprice=0,sellprice=0,shortprice=0,coverprice=coverprice))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
            out$cover[indexofsignal]=signals$cover[i]
            out$coverprice[indexofsignal]=coverprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofshort]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=0,
                                       short=0,cover=signals$cover[i],buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
              out$cover[indexofsignal]=signals$cover[i]
              out$coverprice[indexofsignal]=0
            }
          }
        }
      }
    }
  }
  out[order(out$date),]
}

futureTradeSignals<-function(signals,fnodatafolder,equitydatafolder,rollover=FALSE){
  out=data.frame(date=as.POSIXct(character()),
                 symbol=character(),
                 buy=numeric(),
                 sell=numeric(),
                 short=numeric(),
                 cover=numeric(),
                 buyprice=numeric(),
                 sellprice=numeric(),
                 shortprice=numeric(),
                 coverprice=numeric(),
                 stringsAsFactors = FALSE)
  signals$inlongtrade<-RTrade::Flip(signals$buy,signals$sell)
  signals$inshorttrade<-RTrade::Flip(signals$short,signals$cover)

  # amend signals for any rollover
  if(rollover){
     signals$rolloverdate<-signals$currentmonthexpiry!=signals$entrycontractexpiry & signals$entrycontractexpiry!=Ref(signals$entrycontractexpiry,-1)
    signals$rolloverorders<-signals$rolloverdate & ((signals$inlongtrade & Ref(signals$inlongtrade,-1))|(signals$inshorttrade & Ref(signals$inshorttrade,-1)))
    for(i in 2:nrow(signals)){
      if(signals$rolloverorders[i]==TRUE){
        if(signals$inlongtrade[i]==1){
          df.copy=signals[i,]
          signals$sell[i]=1
          indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
          df.copy$buy[1]=1
          df.copy$strike[1]=signals$strike[indexofbuy]
          signals<-rbind(signals,df.copy)
        }else if(signals$inshorttrade[i]==1){
          df.copy=signals[i,]
          signals$cover[i]=1
          indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
          df.copy$short[1]=1
          df.copy$strike[1]=signals$strike[indexofshort]
          signals<-rbind(signals,df.copy)

        }
      }
    }
  }

  signals<-signals[order(signals$date),]

  expiry=format(signals[, c("entrycontractexpiry")], format = "%Y%m%d")
  signals$symbol=ifelse(signals$buy>0,
                        paste(unlist(strsplit(signals$symbol,"_"))[1],"FUT",expiry,"","",sep="_"),
                        ifelse(signals$short>0,
                               paste(unlist(strsplit(signals$symbol,"_"))[1],"FUT",expiry,"","",sep="_"),
                               signals$symbol
                        )
  )
  #Entry
  for(i in 1:nrow(signals)){
    if(signals$buy[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      buyprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=signals$buy[i],sell=0,
                               short=0,cover=0,buyprice=buyprice,sellprice=0,shortprice=0,coverprice=0))

    }else if(signals$short[i]>0){
      load(paste(fnodatafolder, signals$symbol[i], ".Rdata", sep = ""))
      datarow = md[md$date == signals$date[i], ]
      shortprice=datarow$settle[1]
      out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[i],buy=0,sell=0,
                               short=signals$short[i],cover=0,buyprice=0,sellprice=0,shortprice=shortprice,coverprice=0))
    }
  }

  # Adjust for scenario where there is no rollover and the position is open on expiry date
  indexofbuy=sapply(seq(1:length(signals$buy)),function(x){index=tail(which(signals$buy[1:(x-1)]>0),1)})
  indexofshort=sapply(seq(1:length(signals$short)),function(x){index=tail(which(signals$short[1:(x-1)]>0),1)})
  indexofbuy=unlist(indexofbuy)
  indexofshort=unlist(indexofshort)
  indexofshort=c(rep(0,(nrow(signals)-length(indexofshort))),indexofshort)
  indexofbuy=c(rep(0,(nrow(signals)-length(indexofbuy))),indexofbuy)
  indexofentry=pmax(indexofbuy,indexofshort)
  for(i in 1:nrow(signals)){
    symbolsvector=unlist(strsplit(signals$symbol[i],"_"))
    if(length(symbolsvector)==1){
      symbolsvector=unlist(strsplit(signals$symbol[indexofentry[i]],"_"))
      expiry=as.Date(strptime(symbolsvector[3],"%Y%m%d",tz="Asia/Kolkata"))
      if(expiry==as.Date(signals$date[i],tz="Asia/Kolkata")){
          if(signals$cover[i]==0 && signals$sell[i]==0)
            #continuing trade. close the trade
            if(Ref(signals$inlongtrade,-1)[i]>0){
              signals$sell[i]=1
            }else if(Ref(signals$inshorttrade,-1)[i]>0){
              signals$cover[i]=1
            }
      }
    }
  }


  #Exit
  for(i in 1:nrow(signals)){
    if(signals$sell[i]>0){
      indexofbuy=tail(which(signals$buy[1:(i-1)]>0),1)
      if(length(indexofbuy)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofbuy],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofbuy], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder,symbolsvector[1],".Rdata",sep=""))
          udatarow=md[md$date == signals$date[i], ]
          spread=NA_real_
          if(datarow$open[1]!=datarow$high[1] ||datarow$open[1]!=datarow$low[1]){
            spread=(datarow$high[1]+datarow$low[1]+datarow$settle[1]-(udatarow$high[1]+udatarow$low[1]+udatarow$settle[1]))/3
          }else if(datarow$open[1]==datarow$high[1] ){
            spread=(datarow$low[1]+datarow$settle[1]-(udatarow$low[1]+udatarow$settle[1]))/2
          }else{
            spread=(datarow$high[1]+datarow$settle[1]-(udatarow$high[1]+udatarow$settle[1]))/2
          }
          sellprice=NA_real_
          if(signals$sell[i]==1){
            sellprice=datarow$settle[1]
          }else if (signals$sell[i]>1){
            sellprice=signals$sellprice[i]+spread
          }
          if(signals$symbol[i]!=signals$symbol[indexofbuy]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=signals$sell[i],
                                     short=0,cover=0,buyprice=0,sellprice=sellprice,shortprice=0,coverprice=0))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
            out$sell[indexofsignal]=signals$sell[i]
            out$sellprice[indexofsignal]=sellprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofbuy]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofbuy],buy=0,sell=signals$sell[i],
                                       short=0,cover=0,buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofbuy])
              out$sell[indexofsignal]=signals$sell[i]
              out$sellprice[indexofsignal]=0
            }
          }
        }
      }
    }else if(signals$cover[i]>0){
      indexofshort=tail(which(signals$short[1:(i-1)]>0),1)
      if(length(indexofshort)==1){
        symbolsvector=unlist(strsplit(signals$symbol[indexofshort],"_"))
        load(paste(fnodatafolder, signals$symbol[indexofshort], ".Rdata", sep = ""))
        datarow = md[md$date == signals$date[i], ]
        if(nrow(datarow)==1){
          load(paste(equitydatafolder,symbolsvector[1],".Rdata",sep=""))
          udatarow=md[md$date == signals$date[i], ]
          spread=NA_real_
          if(datarow$open[1]!=datarow$high[1] ||datarow$open[1]!=datarow$low[1]){
            spread=(datarow$high[1]+datarow$low[1]+datarow$settle[1]-(udatarow$high[1]+udatarow$low[1]+udatarow$settle[1]))/3
          }else if(datarow$open[1]==datarow$high[1] ){
            spread=(datarow$low[1]+datarow$settle[1]-(udatarow$low[1]+udatarow$settle[1]))/2
          }else{
            spread=(datarow$high[1]+datarow$settle[1]-(udatarow$high[1]+udatarow$settle[1]))/2
          }
          coverprice=NA_real_
          if(signals$cover[i]==1){
            coverprice=datarow$settle[1]
          }else if (signals$cover[i]>1){
            coverprice=signals$coverprice[i]+spread
          }

          if(signals$symbol[i]!=signals$symbol[indexofshort]){
            # add row to signals
            out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=0,
                                     short=0,cover=signals$cover[i],buyprice=0,sellprice=0,shortprice=0,coverprice=coverprice))
          }else{
            indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
            out$cover[indexofsignal]=signals$cover[i]
            out$coverprice[indexofsignal]=coverprice
          }
        }else{
          # check if record is for today
          if(as.Date(signals$date[i],tz="Asia/Kolkata")==Sys.Date()){
            if(signals$symbol[i]!=signals$symbol[indexofshort]){
              # add row to signals
              out=rbind(out,data.frame(date=signals$date[i],symbol=signals$symbol[indexofshort],buy=0,sell=0,
                                       short=0,cover=signals$cover[i],buyprice=0,sellprice=0,shortprice=0,coverprice=0))

            }else{
              indexofsignal=which(out$date==signals$date[i] & out$symbol==signals$symbol[indexofshort])
              out$cover[indexofsignal]=signals$cover[i]
              out$coverprice[indexofsignal]=0
            }
          }
        }
      }
    }
  }
  out[order(out$date),]
}
