library(httr)
library(jsonlite)
library(xts)

# ref <- function(x, shift) {
#   # x has to be a vector. Not a matrix or dataframe!!
#   if (shift < 0) {
#     addition <- rep(NA, abs(shift))
#     r1 <- seq(length(x) - abs(shift) + 1, length(x), 1)
#     out <- x[-r1]
#     out <- append(out, addition, 0)
#
#   } else if (shift > 0) {
#     addition <- rep(NA, abs(shift))
#     r1 <- seq(1, abs(shift), 1)
#     out <- x[-r1]
#     out <- append(out, addition, length(x))
#   } else{
#     x
#   }
# }

kQueryBody <- function(start, end, metrics, add.to = NULL) {
  if (is.null(add.to)) {
    result <- list(start, end, list(metrics))
    if (is(start, "list") && is(end, "list")) {
      names(result) <- c("start_relative", "end_relative", "metrics")
    } else if (is(start, "list")) {
      names(result) <- c("start_relative", "end_absolute", "metrics")
    } else if (is(end, "list")) {
      names(result) <- c("start_absolute", "end_relative", "metrics")
    } else{
      names(result) <- c("start_absolute", "end_absolute", "metrics")
    }
  }
  else{
    result <- append(add.to$metrics, metrics)
  }


  result


}

kDate <- function(x, y = NULL) {
  if (is.null(y)) {
    result <- jsonlite::unbox(x)
  } else{
    x <- list(jsonlite::unbox(x), jsonlite::unbox(y))
    names(x) <- c("value", "unit")
    result <- x
  }
  result
}

kMetrics <- function(tags, name, aggregators) {
  if (!is.null(aggregators)) {
    result <- list(tags, jsonlite::unbox(name), list(aggregators))
    names(result) <- c("tags", "name", "aggregators")
    #print(result)


  } else{
    result <- list(tags, jsonlite::unbox(name))
    names(result) <- c("tags", "name")
    #print("wrong loop")
    #print(result)
  }
  result
}

kAggregators <- function(x, y, z) {
  sampling <- list(jsonlite::unbox(y), jsonlite::unbox(z))
  names(sampling) <- c("value", "unit")
  # result <- list(jsonlite::unbox(x), jsonlite::unbox(c("true")),sampling)
  # names(result) <- c("name", "align_start_time","sampling")
  result <- list(jsonlite::unbox(x), sampling)
  names(result) <- c("name", "sampling")
  return(result)
}

kTags <- function(value) {
  input=as.list(strsplit(value,",")[[1]])
  #input <- list(...)[-length(list(...))]
  #print("input")
  #print(input)
  #print("length input")
  #print(length(input))
  tags <- list()
  names <- vector()
  for (i in 1:length(input)) {
    #print("loop #")
    #print(i)
    namevalue <- input[[i]]
    #print("namevalue")
    #print(namevalue)
    name <- strsplit(as.character(input[[i]]), "=")[[1]][[1]]
    #print("name")
    #print(name)
    value <- strsplit(as.character(input[[i]]), "=")[[1]][[2]]
    #value<-strsplit(as.character(value),split=",")
    #print("value")
    #print(value)
    tags <- c(tags, list(value))
    names <- c(names, name)
  }
  names(tags) <- names


  #print("tags")
  #print(tags)
  tags
}

kgetTags<-function(...,start,end,timezone = "Asia/Kolkata",name,ts,tagname){



  input = strsplit(list(...)[[1]], "=")[[1]][2]

  md <- data.frame(symbol = character(),tagname=character(),stringsAsFactors = FALSE)
  if (!is.null(symbolchange)) {
    symbollist = c(toupper(input))

  }
  for (j in length(symbollist):1) {
    a <- list(...)
    a[[1]] = paste("symbol", tolower(symbollist[j]), sep = "=")
    newargs = paste(a, collapse = ",")
    startUnix <-
      as.numeric(as.POSIXct(paste(start, timezone))) * 1000
    endUnix <- as.numeric(as.POSIXct(paste(end, timezone))) * 1000
    startLong <- kDate(startUnix)
    endLong <- kDate(endUnix)
    tags <- kTags(newargs)
    out <- matrix()
    aggr = list()
    for (i in 1:length(ts)) {
      tempname = paste(name, ts[i], sep = ".")
      metrics <- kMetrics(tags, tempname, NULL)


      query <- kQueryBody(startLong, endLong, metrics)
      myjson <- toJSON(query, pretty = TRUE)
      r <-
        POST(
          "http://91.121.165.108:8085/api/v1/datapoints/query/tags",
          body = myjson,
          encode = "json"
        )
      if (length(content(r)$queries[[1]]$results[[1]]$tags[tagname]) > 0) {
        print(content(r)$queries[[1]]$results[[1]]$tags["strike"])
        df<-data.frame(tagname=unlist(content(r)$queries[[1]]$results[[1]]$tags["strike"]),symbol=symbollist[j],stringsAsFactors = FALSE)
        md=rbind(md,df)
      }
    }




  }
  rownames(md)<-NULL
  colnames(md)<-c(tagname,"symbol")
  md
}

kGetOHLCV <-
  function(...,
           df=NULL,
           start = "",
           end = "",
           timezone = "Asia/Kolkata",
           name = "symbol",
           ts = c("open", "high", "low", "close", "volume")
           ,
           aggregators = c("first", "max", "min", "last", "sum"),
           aValue = NULL,
           aUnit = NULL,
           symbolchange = NULL,
           splits = NULL,
           filepath = NULL) {
    input = strsplit(list(...)[[1]], "=")[[1]][2]
    md <- data.frame()
    if (!is.null(symbolchange)) {
      symbollist <-
        linkedsymbols(symbolchange[, 2], #key
                      symbolchange[, 3], #newsymbol
                      toupper(input))
    } else{
      symbollist = c(toupper(input))
    }


    for (j in length(symbollist):1) {
      a <- list(...)
      a[[1]] = paste("symbol", tolower(symbollist[j]), sep = "=")
      newargs = paste(a, collapse = ",")
      startUnix <-
        as.numeric(as.POSIXct(paste(start, timezone))) * 1000
      endUnix <- as.numeric(as.POSIXct(paste(end, timezone))) * 1000
      startLong <- kDate(startUnix)
      endLong <- kDate(endUnix)
      tags <- kTags(newargs)
      out <- matrix()
      aggr = list()
      for (i in 1:length(ts)) {
        tempname = paste(name, ts[i], sep = ".")
        aggr <- kAggregators(aggregators[i], aValue, aUnit)


        if (!is.null(aggr)) {
          metrics <- kMetrics(tags, tempname, aggr)
        } else{
          metrics <- kMetrics(tags, tempname, NULL)
        }
        query <- kQueryBody(startLong, endLong, metrics)
        myjson <- toJSON(query, pretty = TRUE)
        print(paste("retrieving", ts[i], "for", symbollist[j], sep = " "))
        r <-
          POST(
            "http://91.121.165.108:8085/api/v1/datapoints/query",
            body = myjson,
            encode = "json"
          )
        if (content(r)$queries[[1]]$sample_size > 0) {
          m <-
            matrix(
              unlist(content(r)$queries[[1]]$results[[1]]$values),
              ncol = 2,
              byrow = TRUE
            )
          colnames(m) <- c("date", ts[i])
          out <- merge(out, m, by = 1, all = TRUE)


        }
      }
      if (dim(out)[2] == length(ts) + 1) {
        # generate quotes only if the #columns = # elements in ts
        colnames(out) <- c("date", ts)
        out <- out[rowSums(is.na(out)) != length(ts) + 1,]
        d <- data.frame(out)
        md <- rbind(md, d)
      }


    }
    if (nrow(md) > 0) {
      md[, 1] <-
        as.POSIXct(md[, 1] / 1000, origin = "1970-01-01", tz = "Asia/Kolkata")
      md$symbol <- symbollist[1]
    }
    #handle splits
    if (!is.null(splits)) {
      md <- processSplits(md, splits, symbollist,df,end)
    }else{
      md<-rbind(df,md)
    }
    if (is.character(filepath)) {
      save(md, file = paste(filepath, symbollist[1], ".Rdata", sep = ""))
    }
    md
  }

processSplits <- function(md, splits, symbollist,origmd,end) {
  if (nrow(md) > 0) {
    if (is.character(splits$date)) {
      splits[, 1] <-
        as.POSIXct(splits[, 1], format = "%Y-%m-%d") # col 1 is date
    }
  }
  if(nrow(origmd)>0){
    if(is.na(match("splitadjust", names(origmd)))){
      md<-rbind(origmd,md)
    }else{
      superset=c("date","open","high","low","settle","close","volume","delivered","symbol")
      superset<-superset[superset %in% names(origmd)]
      md<-rbind(origmd[,superset],md)
    }
  }


  if(!is.null(md) && nrow(md)>0){
    endreference=as.POSIXct(end)
    md$splitadjust=1
    for (i in 1:length(symbollist)) {
      print(paste("Processing Split for symbol", symbollist[i], sep = " "))
      subset <-
        splits[splits[, 2] == symbollist[i],] # col 2 is symbols
      if (nrow(subset) > 0) {
        for (j in 1:nrow(subset)) {
          #print(paste(endreference,subset[,1][j],subset[,1][j]<=endreference))
          md$splitadjust <-
            ifelse(md$date < subset[, 1][j] & subset[,1][j]<=endreference,
                   md$splitadjust*subset[, 4][j]/subset[, 3][j],
                   #row 3 is oldshare, row 4 is newshares
                   md$splitadjust)
          if ("open" %in% colnames(md)) {
            md$aopen <-md$open/md$splitadjust
          }
          if ("high" %in% colnames(md)) {
            md$ahigh <- md$high/md$splitadjust
          }
          if ("low" %in% colnames(md)) {
            md$alow <- md$low/md$splitadjust
          }
          if ("close" %in% colnames(md)) {
            md$aclose <- md$close/md$splitadjust
          }
          if ("settle" %in% colnames(md)) {
            md$asettle <-md$settle/md$splitadjust
          }
          if ("volume" %in% colnames(md)) {
            md$avolume <-md$volume*md$splitadjust
          }
          if ("delivered" %in% colnames(md)) {
            md$adelivered <-md$delivered*md$splitadjust
          }
        }
      }else{
        # no splits. add columns for adjusted values
        if ("open" %in% colnames(md)) {
          md$aopen <-md$open/md$splitadjust
        }
        if ("high" %in% colnames(md)) {
          md$ahigh <- md$high/md$splitadjust
        }
        if ("low" %in% colnames(md)) {
          md$alow <- md$low/md$splitadjust
        }
        if ("close" %in% colnames(md)) {
          md$aclose <- md$close/md$splitadjust
        }
        if ("settle" %in% colnames(md)) {
          md$asettle <-md$settle/md$splitadjust
        }
        if ("volume" %in% colnames(md)) {
          md$avolume <-md$volume*md$splitadjust
        }
        if ("delivered" %in% colnames(md)) {
          md$adelivered <-md$delivered*md$splitadjust
        }
      }
    }
  }
  md
}

convertToXTS<-function(md,columns=c("aopen","ahigh","alow","aclose","avolume"),dateIndex=FALSE,tz="Asia/Kolkata"){
  if(!is.null(md) && nrow(md)>0){
    #symbol=md[1,c("symbol")]
    if(dateIndex){
      out<-as.xts(md[,columns],as.Date(md[,1],tz=tz))
    }else{
      out<-xts(md[,columns],md[,1])

    }
    #assign(paste(symbol,sep=""),out)
  }
  #return(paste(symbol,sep=""))
  return(out)
}
#a <- merge(Matrix1, Matrix2, by = c("Col1", "Col3"), all = TRUE)
#r<-kGetOHLCV(start="2014-10-30 00:00:00",end="2014-11-05 00:00:00",timezone="Asia/Kolkata",name="india.nse.future.s1.1min",aName=NULL,aValue=NULL,aUnit=NULL,"symbol=nsenifty","expiry=20141127")
#result<-content(r)$queries[[1]]$results[[1]]$values
#r<-kGetOHLCV(start="2014-10-21 09:15:00",end="2014-10-21 15:30:00",timezone="Asia/Kolkata",name="india.nse.equity.s1.1sec",aName=NULL,aValue=NULL,aUnit=NULL,"symbol=vakrangee")
#r<-kGetOHLCV(start="2014-11-26 00:13:00",end="2014-11-26 15:30:00",timezone="Asia/Kolkata",name="india.nse.future.s1.1min",aName=NULL,aValue=NULL,aUnit=NULL,"symbol=nsenifty","expiry=20141127")
#r<-kGetOHLCV(start="2015-01-08 09:15:00",end="2015-01-08 15:30:00",timezone="Asia/Kolkata",name="india.nse.future.s1.1sec",aName=NULL,aValue=NULL,aUnit=NULL,"symbol=nsenifty","expiry=20150129")
#r<-kGetOHLCV(start="1990-05-21 09:15:00",end="2014-10-21 15:30:00",timezone="Asia/Kolkata",name="india.nse.index.s4.daily",aName=NULL,aValue=NULL,aUnit=NULL,"symbol=nsenifty")
#sum(abs(diff(r$close)))
#which(is.na(r$close))

b<-paste("symbol=nsenifty","expiry=20170125","option=CALL",sep=",")
start="2016-12-29 00:00:00"
end= "2016-12-29 00:00:00"
symbol="nsenifty"
