# Tabulate index changes
library(rredis)
library(RQuantLib)
library(quantmod)

createIndexConstituents <-
        function(redisdb, pattern, threshold = "2000-01-01") {
                redisConnect()
                redisSelect(redisdb)
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
                                        symbol = unlist(redisSMembers(rediskeysShortList[i])),
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
                redisClose()
                dfstage2

        }

createFNOConstituents <-
        function(redisdb, pattern, threshold = "2000-01-01") {
                redisConnect()
                redisSelect(redisdb)
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
                redisClose()
                dfstage2

        }

createFNOSize <-
        function(redisdb, pattern, threshold = "2000-01-01") {
                redisConnect()
                redisSelect(redisdb)
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
                redisClose()
                dfstage2

        }

getMostRecentSymbol <- function(symbol, original, final) {
        out <- linkedsymbols(final, original, symbol)
        out <- out[length(out)]
        out
}

readAllSymbols <-
        function(redisdb, pattern, threshold = "2000-01-01") {
                redisConnect()
                redisSelect(redisdb)
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
                redisConnect()
                redisSelect(redisdb)
                rediskeys = redisKeys()
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
                                                load(paste(
                                                        mdpath,
                                                        symbol,
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
                                brokerage = as.numeric(data["entrybrokerage"]) + as.numeric(data["exitbrokerage"])
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
                #return(actualtrades[with(trades,order(entrytime)),])
                actualtrades
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
        function(redisdb, symbol, duration, type, starttime,endtime) {
                # Retrieves OHLCS prices from redisdb (currently no 9) starting from todaydate till end date.
                #symbol = redis symbol name in db=9
                #duration = [tick,daily]
                # type = [OPT,STK,FUT]
                # todaydate = starting timestamp for retrieving prices formatted as "YYYY-mm-dd HH:mm:ss"
                redisConnect()
                redisSelect(as.numeric(redisdb))
                start = as.numeric(as.POSIXct(starttime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
                end = as.numeric(as.POSIXct(endtime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Kolkata")) * 1000
                a <-
                        redisZRangeByScore(paste(symbol, duration, type, sep = ":"), min = start, max=end)
                redisClose()
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

GetCurrentPosition <-
        function(scrip,
                 portfolio,
                 path = NULL,
                 deriv=FALSE,
                 trades.till = Sys.Date(),
                 position.on = Sys.Date()) {
                # Returns the current position for a scrip after calculating all rows in portfolio.
                #scrip = String
                #Portfolio = df containing columns[symbol,exittime,trade,size]
                # path of market data file that holds split information, if any
                # optional startdate is compared to the entrytime to filter records used for position calculation. All records equal or before startdate are included for position calc
                # optional enddate is compared to the exittime to filter records used for position calculation. All records AFTER enddate are included for position calc.
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
                                portfolio[as.Date(portfolio$entrytime, tz = "Asia/Kolkata") <= trades.till, ]
                        for (row in 1:nrow(portfolio)) {
                                if ((
                                        is.na(portfolio[row, 'exittime']) ||
                                        as.Date(portfolio[row, 'exittime'], tz = "Asia/Kolkata") > position.on
                                )
                                &&  portfolio[row, 'symbol'] == scrip) {
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
                                          splitadjustment=md$splitadjust[buyindex]/md$splitadjust[currentindex]
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
                if (nrow(portfolio) > 0) {
                        #for (l in 1:5){
                        for (l in 1:nrow(portfolio)) {
                                #print(paste("portfolio line:",l,sep=""))
                                name = portfolio[l, 'symbol']
                                entrydate = as.Date(portfolio[l, 'entrytime'], tz =
                                                            "Asia/Kolkata")
                                exitdate = as.Date(portfolio[l, 'exittime'], tz =
                                                           "Asia/Kolkata")
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
                                entryindex = which(as.Date(md$date, tz = "Asia/Kolkata") ==
                                                           entrydate)
                                exitindex = which(as.Date(md$date, tz = "Asia/Kolkata") == exitdate)
                                unrealizedpnlexists = FALSE
                                if (length(exitindex) == 0) {
                                        # we do not have an exit date
                                        exitindex = nrow(md)
                                        altexitindex = which(
                                                as.Date(md$date, tz = "Asia/Kolkata") == pnl$bizdays[nrow(pnl)]
                                        )
                                        exitindex = ifelse(length(altexitindex) > 0,
                                                           altexitindex,
                                                           exitindex)
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
                                        cumunrealized = seq(0,
                                                            0,
                                                            length.out = (dtindexend -
                                                                                  dtindexstart + 1))
                                        side = portfolio[l, 'trade']
                                        #for (index in entryindex:1338) {
                                        for (index in entryindex:(exitindex - 1)) {
                                                #entryindex,exitindex are indices for portfolio
                                                #dtindex,dtindexstart,dtindexend are indices for pnl
                                                #print(index)
                                                dtindex = which(
                                                        pnl$bizdays == as.Date(md$date[index], tz =
                                                                                       "Asia/Kolkata")
                                                )
                                                if (length(dtindex) > 0) {
                                                        # only proceed if bizdays has the specified md$date[index] value
                                                        newprice = md$settle[index]
                                                        newsplitadjustment=1
                                                        if(handlesplits){
                                                          if(index==entryindex){
                                                            newsplitadjustment=1
                                                          }else{
                                                            newsplitadjustment=md$splitadjust[(index-1)]/md$splitadjust[index]
                                                        }
                                                        }
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
                        indexofbuy = getBuyIndices(signals, i)
                        if (length(indexofbuy) > 0) {
                                for (j in 1:length(indexofbuy)) {
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
                } else if (signals$inshorttrade[i] > 0) {
                        indexofshort = getShortIndices(signals, i)
                        if (length(indexofshort) > 0) {
                                for (j in 1:length(indexofshort)) {
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

        #Exit
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
                                                } else{
                                                        # check if record is for today
                                                        if (as.Date(signals$date[i],
                                                                    tz = "Asia/Kolkata") ==
                                                            Sys.Date()) {
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
                                                        }
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
                                                } else{
                                                        # check if record is for today
                                                        if (as.Date(signals$date[i],
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
                                                        }
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
                                indexofbuy = getBuyIndices(signals, i)
                                if (length(indexofbuy) > 0) {
                                        for (j in 1:length(indexofbuy)) {
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
                        } else if (signals$inshorttrade[i] > 0) {
                                indexofshort = getShortIndices(signals, i)
                                if (length(indexofshort) > 0) {
                                        for (j in 1:length(indexofshort)) {
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
                                                        } else{
                                                                # check if record is for today
                                                                if (as.Date(signals$date[i],
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
                                                                }
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
                                                        } else{
                                                                # check if record is for today
                                                                if (as.Date(signals$date[i],
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
                                                                }
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

                for (i in 1:nrow(signals)) {
                        if (signals$inlongtrade[i] > 0) {
                                indexofbuy = getBuyIndices(signals, i)
                                if (length(indexofbuy) > 0) {
                                        for (j in 1:length(indexofbuy)) {
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
                        } else if (signals$inshorttrade[i] > 0) {
                                indexofshort = getShortIndices(signals, i)
                                if (length(indexofshort) > 0) {
                                        for (j in 1:length(indexofshort)) {
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
                                                                if (signals$sell[i] == 1) {
                                                                        sellprice = datarow$settle[1]
                                                                } else if (signals$sell[i] > 1) {
                                                                        sellprice = signals$sellprice[i] + spread
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
                                                        } else{
                                                                # check if record is for today
                                                                if (as.Date(signals$date[i],
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
                                                                }
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
                                                                if (signals$cover[i] == 1) {
                                                                        coverprice = datarow$settle[1]
                                                                } else if (signals$cover[i] > 1) {
                                                                        coverprice = signals$coverprice[i] + spread
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
                                                        } else{
                                                                # check if record is for today
                                                                if (as.Date(signals$date[i],
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
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
                out[order(out$date), ]
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
                as.numeric(e)

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
                        out = NA_real_
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
                                if (symbolsvector[1] == "NSENIFTY") {
                                        dfsignals$strike[i] = round((
                                                signals$buyprice[i] + signals$shortprice[i]
                                        ) / 100) * 100
                                } else{
                                        dfsignals$strike[i] = getClosestStrike(
                                                dfsignals$date[i],
                                                symbolsvector[1],
                                                strftime(
                                                        dfsignals$entrycontractexpiry[i],
                                                        "%Y%m%d",
                                                        tz =
                                                                timeZone
                                                ),
                                                fnodatafolder,
                                                equitydatafolder
                                        )
                                }
                        }
                }
                dfsignals
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
                 start,
                 end,
                 deriv = FALSE,
                 fnodatafolder = "/home/psharma/Dropbox/rfiles/dailyfno/",
                 equitydatafolder = "/home/psharma/Dropbox/rfiles/daily/") {
                if (!deriv) {
                        load(paste(equitydatafolder, symbol, ".Rdata", sep = ""))
                } else{
                        symbolsvector = unlist(strsplit(name, "_"))
                        load(paste(
                                fnodatafolder,
                                symbolsvector[3],
                                "/",
                                symbol,
                                ".Rdata",
                                sep = ""
                        ))
                }
                if (symbol == "NSENIFTY") {
                        md$aclose = md$asettle
                }
                symbolname = convertToXTS(md, c("aopen", "ahigh", "alow", "aclose", "avolume"))
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
                chartSeries(symbolname,
                            subset = paste(start, "::", end, sep = ""),
                            theme = customTheme)
        }

createTradeSummaryFromRedis<-function(redisdb,pattern,start,end,mdpath,deriv=FALSE){
  #generates trades dataframe using information in redis
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
    exitdate=ifelse(is.na(exitdate),Sys.Date(),as.Date(exitdate,tz="Asia/Kolkata"))
    if(exitdate>=periodstartdate && entrydate<=periodenddate){
      if(grepl("opentrades",rediskeysShortList[i])){
        symbol=data["entrysymbol"]
        symbolsvector = unlist(strsplit(symbol, "_"))
        symbol<-symbolsvector[1]
        if (!deriv) {
                load(paste(mdpath, symbolsvector[1], ".Rdata", sep = ""))
        } else{
                load(paste(
                        fnodatafolder,
                        symbolsvector[3],
                        "/",
                        symbol,
                        ".Rdata",
                        sep = ""
                ))
        }
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
      if(!deriv){
              df=data.frame(symbol=symbol,trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
                            entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
                            percentprofit=percentprofit,bars=0,brokerage=brokerage,
                            netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
              )
      }else{
              df=data.frame(symbol=data["parentsymbol"],trade=data["entryside"],entrysize=as.numeric(data["entrysize"]),entrytime=data["entrytime"],
                            entryprice=as.numeric(data["entryprice"]),exittime=data["exittime"],exitprice=exitprice,
                            percentprofit=percentprofit,bars=0,brokerage=brokerage,
                            netpercentprofit=netpercentprofit,netprofit=netprofit,key=rediskeysShortList[i],stringsAsFactors = FALSE
              )
      }

      rownames(df)<-NULL
      actualtrades=rbind(actualtrades,df)

    }
  }
  #return(actualtrades[with(trades,order(entrytime)),])
  return(actualtrades)
}


