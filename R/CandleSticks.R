candleStickPattern<-function(s,trendBeginning=FALSE,conservative=FALSE,type="STK",realtime=FALSE,maxWait=1){
  if(is.data.frame(s)){
    md=s
  }else{
    md<-loadSymbol(s,realtime)
  }
  t<-Trend(md$date,md$ahigh,md$alow,md$asettle)
  md<-merge(md,t,by="date")
  #atr<-ATR(md[,c("ahigh","alow","asettle")],n=23)
  #md$atr<-atr[,"atr"]
  # md$sma<-SMA(md$asettle,7)
  #md$lowrange=(md$ahigh-md$alow)<md$atr
  md$body=abs(md$asettle-md$aopen)
  md$lowershadow=abs(pmin(md$asettle,md$aopen)-md$alow)
  md$uppershadow=abs(md$ahigh-pmax(md$asettle,md$aopen))
  md$uppershadowavg=SMA(md$uppershadow,23)
  md$lowershadowavg=SMA(md$lowershadow,23)
  md$bodyavg=SMA(md$body,23)
  md$doji=md$body<0.1*md$bodyavg & md$uppershadow+md$lowershadow>2*md$body
  md$blackcandle=md$asettle<md$aopen
  md$whitecandle=md$asettle>md$aopen
  md$shortwhite=md$whitecandle  & md$body<0.6*md$bodyavg
  md$shortblack=md$blackcandle  & md$body<0.6*md$bodyavg
  md$normalwhite=md$whitecandle  & md$body>0.6*md$bodyavg & md$body<1.25*md$bodyavg
  md$normalblack=md$blackcandle  & md$body>0.6*md$bodyavg & md$body<1.25*md$bodyavg
  md$longwhite=md$whitecandle  & md$body>1.25*md$bodyavg
  md$longblack=md$blackcandle  & md$body>1.25*md$bodyavg
  md$whitemarubozu= md$longwhite & md$lowershadow==0 & md$uppershadow==0
  md$whiteopeningmarubozu=md$longwhite & md$lowershadow==0
  md$whiteclosingmarubozu=md$longwhite & md$uppershadow==0
  md$blackmarubozu= md$longblack & md$lowershadow==0 & md$uppershadow==0
  md$blackopeningmarubozu=md$longblack & md$uppershadow==0
  md$blackclosingmarubozu=md$longblack & md$lowershadow==0

  md$gapdown=md$aopen<Ref(md$alow,-1)
  md$gapup=md$aopen>Ref(md$ahigh,-1)
  md$updowncount=sequence(rle(md$updownbar)$lengths)
  md$sma.s<-SMA(md$asettle,5)
  md$sma.h<-SMA(md$ahigh,5)
  md$sma.l<-SMA(md$alow,5)
  md$rsi.23.h<-TTR::RSI(md$ahigh,23)
  md$rsi.23.l<-TTR::RSI(md$alow,23)
  md$rsi.5<-TTR::RSI(md$asettle,5)
  md$daysinuptrend=BarsSince(md$trend!=1)
  md$daysindntrend=BarsSince(md$trend!= -1)
  md$inlongtrade=ContinuingLong("ICICIBANK",as.numeric(md$trend==1),as.numeric(md$trend<1),as.numeric(md$trend==-1))
  md$daysintrend=ave(as.numeric(md$trend==1),cumsum(md$inlongtrade==0),FUN=cumsum)
  #ave(as.numeric(md$trend==1),cumsum(md$daysindntrend==0),FUN=cumsum)
  #with(md, ave(md$buy, cumsum(md$inlongtrade == 0), FUN = cumsum))
  #md$candlesticktrend=ifelse(md$trend!=0 & md$daysinuptrend+md$daysindntrend>=5 & md$updowncount>=2,md$trend,100)
  md$candlesticktrend=ifelse(rollmean(md$updownbar,3,fill=NA,align="right")>0 & md$asettle-Ref(md$asettle,-3)>0,1,
                             ifelse(rollmean(md$updownbar,3,fill=NA,align="right")<0 & md$asettle-Ref(md$asettle,-3)<0,-1,100))
  #md$candlesticktrend=ifelse(md$trend==1|md$trend==-1,ifelse(md$swinglevel>Ref(md$swinglevel,-1),1,-1),0) # good result with ONGC
  md$candlesticktrend=0
  signals=data.frame()

  #1 BULLISH HAMMER
  duration=2
  pattern= Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & md$alow<Ref(md$alow,-2) & # Day 3
    md$alow<Ref(md$alow,-1) & # Day 2
    md$body<0.4*md$bodyavg & md$lowershadow>2*md$body & md$lowershadow>md$lowershadowavg & md$uppershadow<md$bodyavg*0.2 # Day 1
  confirmationprice=pmax(md$asettle,md$aopen)
  stoploss=md$alow
  ##entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH HAMMER",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #1 BEARISH HANGING MAN
  duration=2
  pattern= Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & md$ahigh>Ref(md$ahigh,-2) & # Day 3
    md$ahigh>Ref(md$ahigh,-1) & # Day 2
    md$body<0.4*md$bodyavg & md$lowershadow>2*md$body & md$lowershadow>md$lowershadowavg & md$uppershadow<md$bodyavg*0.2 # Day 1
  confirmationprice=pmin(md$asettle,md$aopen)
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH HANGING MAN",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #2 BULLISH BELT HOLD
  duration=0
  pattern=(md$candlesticktrend==-1|md$candlesticktrend==0) & md$gapdown & md$whiteopeningmarubozu & md$longwhite
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH BELT HOLD",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #2 BEARISH BELT HOLD #VERIFIED
  duration=0
  pattern=(md$candlesticktrend==1|md$candlesticktrend==0) & md$gapup & md$blackopeningmarubozu & md$longblack
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH BELT HOLD",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #3 BULLISH ENGULFING #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & md$whitecandle & !md$shortwhite & md$asettle>Ref(md$aopen,-1) & md$aopen<Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH ENGULFING",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #3 BEARISH ENGULFING #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & md$blackcandle & !md$shortblack & md$asettle<Ref(md$aopen,-1) & md$aopen>Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH ENGULFING",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #4 BULLISH HARAMI #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$whitecandle & md$asettle<Ref(md$aopen,-1) & md$aopen>Ref(md$asettle,-1)
  confirmationprice=pmax(md$asettle,Ref((md$aopen+md$asettle)/2,-1))
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH HARAMI",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #4 BEARISH HARAMI #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$blackcandle & md$asettle>Ref(md$aopen,-1) & md$aopen<Ref(md$asettle,-1)
  confirmationprice=pmin(md$asettle,Ref((md$aopen+md$asettle)/2,-1))
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH HARAMI",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #5 BULLISH HARAMI CROSS #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & md$doji & pmax(md$asettle,md$aopen)<Ref(md$aopen,-1) & pmin(md$asettle,md$aopen)>Ref(md$asettle,-1)
  confirmationprice=ifelse(Ref(md$shortblack,-1),Ref(md$aopen,-1),pmax(md$asettle,Ref((md$aopen+md$asettle)/2,-1)))
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH HARAMI CROSS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #5 BEARISH HARAMI CROSS #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & md$doji & pmax(md$asettle,md$aopen)<Ref(md$asettle,-1) & pmin(md$asettle,md$aopen)>Ref(md$aopen,-1)
  confirmationprice=ifelse(Ref(md$shortwhite,-1),Ref(md$aopen,-1),pmin(md$asettle,Ref((md$aopen+md$asettle)/2,-1)))
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH HARAMI CROSS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #6 BULLISH INVERTED HAMMER #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & (md$shortblack|md$shortwhite) & Ref(md$asettle,-1)>pmax(md$asettle,md$aopen) & md$uppershadow>2*md$body & md$lowershadow<0.1*md$body
  confirmationprice=pmax(md$aopen,md$asettle)+md$uppershadow/2
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH INVERTED HAMMER",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #6 BEARISH SHOOTING STAR
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & (md$shortblack|md$shortwhite) & Ref(md$asettle,-1)<pmin(md$asettle,md$aopen) & md$uppershadow>2*md$body & md$lowershadow<0.1*md$body
  confirmationprice=pmin(md$aopen,md$asettle)
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH SHOOTING STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #7 BULLISH PIERCING LINE #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$gapdown & md$whitecandle & md$asettle>Ref(md$asettle+md$aopen,-1)/2 & md$asettle<Ref(md$aopen,-1)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH PIERCING LINE",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #7 BEARISH DARK CLOUD COVER
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$gapup & md$blackcandle & md$asettle<Ref(md$asettle+md$aopen,-1)/2 & md$asettle>Ref(md$aopen,-1)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH DARK CLOUD COVER",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #8 Bullish Doji Star #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalblack|md$longblack,-1) & md$gapdown & md$doji
  confirmationprice=ifelse(pattern,md$asettle+(Ref(md$asettle,-1)-md$asettle)/2,0)
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH DOJI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #8 Bearish Doji Star #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalwhite|md$longwhite,-1) & md$gapup & md$doji
  confirmationprice=ifelse(pattern,md$asettle-(md$asettle-Ref(md$asettle,-1))/2,0)
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH DOJI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #9 BULLISH MEETING LINE #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$gapdown & md$whitecandle & !md$shortwhite & abs(md$asettle-Ref(md$asettle,-1))/md$asettle<0.002
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH MEETING LINE",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #9 BEARISH MEETING LINE #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$gapup & md$blackcandle & !md$shortblack & abs(md$asettle-Ref(md$asettle,-1))/md$asettle<0.002
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH MEETING LINE",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #10 BULLISH HOMING PIGEON #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$blackcandle & md$aopen<=Ref(md$aopen,-1) & md$asettle>=Ref(md$asettle,-1)
  confirmationprice=pmax(md$asettle,Ref((md$aopen+md$asettle)/2,-1))
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH HOMING PIGEON",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #10 BEARISH DESCENDING HAWK # VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$whitecandle & md$aopen>=Ref(md$aopen,-1) & md$asettle<=Ref(md$asettle,-1)
  confirmationprice=pmin(md$asettle,Ref((md$aopen+md$asettle)/2,-1))
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH DESCENDING HAWK",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #11 BULLISH MATCHING LOW #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$aopen>Ref(md$asettle,-1) & md$blackcandle & md$asettle==Ref(md$asettle,-1)
  confirmationprice=Ref((md$aopen+md$asettle)/2,-1)
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH MATCHING LOW",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #11 BEARISH MATCHING HIGH #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$aopen<Ref(md$asettle,-1) & md$whitecandle & md$asettle==Ref(md$asettle,-1)
  confirmationprice=Ref((md$aopen+md$asettle)/2,-1)
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH MATCHING HIGH",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #12 BULLISH KICKING #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalblack|md$longblack,-1) & Ref(md$aopen,-1)<=md$aopen & (md$normalwhite | md$longwhite)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH KICKING",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #12 BEARISH KICKING
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalwhite|md$longwhite,-1) & Ref(md$aopen,-1)>=md$aopen & (md$normalblack | md$longblack)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH KICKING",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #13 BULLISH ONE WHITE SOLDIER #VERIFIED
  duration=1
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & md$whitecandle & !md$shortwhite & md$aopen>Ref(md$asettle,-1) & md$asettle>Ref(md$aopen,-1)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH ONE WHITE SOLDIER",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #13 BEARISH ONE BLACK CROW
  duration=1
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & md$blackcandle & !md$shortblack & md$aopen<Ref(md$asettle,-1) & md$asettle<Ref(md$aopen,-1)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH ONE BLACK CROW",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #14 BULLISH MORNING STAR
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & Ref(md$gapdown,-1) & (Ref(md$shortblack,-1)|Ref(md$shortwhite,-1)) & md$whitecandle & md$aopen>pmin(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle<Ref(md$aopen,-2) & md$asettle>Ref(md$body/2+md$asettle,-2)
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH MORNING STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #14 BEARISH EVENING STAR
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & Ref(md$gapup,-1) & (Ref(md$shortblack,-1)|Ref(md$shortwhite,-1)) & md$blackcandle & md$aopen<pmax(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle>Ref(md$aopen,-2) & md$asettle<Ref(md$aopen-(md$body/2),-2)
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH EVENING STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #15 BULLISH MORNING DOJI STAR
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & Ref(md$gapdown,-1) & Ref(md$doji,-1) & md$whitecandle & md$aopen>pmin(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle>(Ref(md$aopen,-2)+Ref(md$alow,-1)) & md$asettle<Ref(md$aopen,-2) & md$asettle>Ref(md$body+md$asettle,-2)/2
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH MORNING DOJI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #15 BEARISH EVENING DOJI STAR
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & Ref(md$gapup,-1) & Ref(md$doji,-1) & md$blackcandle & md$aopen<pmax(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle<(Ref(md$aopen,-2)+Ref(md$ahigh,-1)) & md$asettle>Ref(md$aopen,-2) & md$asettle<Ref(md$body+md$aopen,-2)/2
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH EVENING DOJI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #16 BULLISH ABANDONED BABY
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & Ref(md$gapdown,-1) & Ref(md$doji,-1) & Ref(md$alow,-2)>Ref(md$ahigh,-1) & Ref(md$ahigh,-1) < md$alow & md$whitecandle & md$aopen>pmin(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle>(Ref(md$aopen,-2)+Ref(md$alow,-1)) & md$asettle<Ref(md$aopen,-2) & md$asettle>Ref(md$body+md$asettle,-2)/2
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH ABANDONED BABY",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #16 BEARISH ABANDONED BABY
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & Ref(md$gapup,-1) & Ref(md$doji,-1) & Ref(md$ahigh,-2)<Ref(md$alow,-1)& Ref(md$alow,-1)>md$ahigh & md$blackcandle & md$aopen<pmax(Ref(md$aopen,-1),Ref(md$asettle,-1)) & md$asettle<(Ref(md$aopen,-2)+Ref(md$ahigh,-1)) & md$asettle>Ref(md$aopen,-2) & md$asettle<Ref(md$body+md$aopen,-2)/2
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH ABANDONED BABY",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #17 BULLISH TRI STAR #VERIFIED
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$doji,-2) & Ref(md$gapdown,-1) & Ref(md$doji,-1) & md$gapup & md$doji
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH TRI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #17 BEARISH TRI STAR
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$doji,-2) & Ref(md$gapup,-1) & Ref(md$doji,-1) & md$gapdown & md$doji
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH TRI STAR",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #18 BULLISH DOWNSIDE GAP TWO RABBITS
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalblack|md$longblack,-2) & Ref(md$gapdown,-1) & Ref(md$shortwhite,-1) & md$aopen<=Ref(md$aopen,-1) & md$asettle>Ref(md$asettle,-1) & md$asettle<Ref(md$asettle,-2)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH DOWNSIDE GAP TWO RABBITS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #18 BEARISH UPSIDE GAP TWO CROWS
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$normalwhite|md$longwhite,-2) & Ref(md$gapup,-1) & Ref(md$shortblack,-1) & md$aopen>=Ref(md$aopen,-1) & md$asettle<Ref(md$asettle,-1) & md$asettle>Ref(md$asettle,-2)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH UPSIDE GAP TWO CROWS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #19 BULLISH UNIQUE THREE RIVER BOTTOM
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-2) & Ref(md$shortblack,-1) & Ref(md$aopen,-1)>Ref(md$asettle,-2) & Ref(md$alow,-1)<Ref(md$alow,-2) & Ref(md$aopen,-1)<Ref(md$aopen,-2) & Ref(md$asettle,-1)>Ref(md$asettle,-2) & md$gapdown & md$shortwhite & md$asettle<Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH UNIQUE THREE RIVER BOTTOM",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #19 BEARISH UNIQUE THREE MOUNTAIN TOP
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite,-2) & Ref(md$shortwhite,-1) & Ref(md$aopen,-1)<Ref(md$asettle,-2) & Ref(md$ahigh,-1)>Ref(md$ahigh,-2) & Ref(md$aopen,-1)>Ref(md$aopen,-2) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & md$gapup & md$shortblack & md$asettle>Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH UNIQUE THREE MOUNTAIN TOP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #20 BULLISH THREE WHITE SOLDIERS
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite|md$normalwhite,-2) & Ref(md$longwhite|md$normalwhite,-1) & (md$longwhite|md$normalwhite) & Ref(md$aopen,-1)>Ref(md$aopen,-2) & !Ref(md$gapup,-1) & md$aopen>Ref(md$aopen,-1) & !md$gapup
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH THREE WHITE SOLDIERS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #20 BEARISH THREE BLACK CROWS
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack|md$normalblack,-2) & Ref(md$longblack|md$normalblack,-1) & (md$longblack|md$normalblack) & Ref(md$aopen,-1)<Ref(md$aopen,-2) & !Ref(md$gapdown,-1) & md$aopen<Ref(md$aopen,-1) & !md$gapdown
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH THREE BLACK CROWS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #21 BULLISH DESCENT BLOCK
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack|md$normalblack,-2) & Ref(md$blackcandle,-1) & md$blackcandle & Ref(md$aopen,-1)<Ref(md$aopen,-2) & !Ref(md$gapdown,-1) & md$aopen<Ref(md$aopen,-1) & !md$gapdown & Ref(md$aopen-md$asettle,-1)<Ref(md$aopen-md$asettle,-2) & md$aopen-md$asettle<Ref(md$aopen-md$asettle,-1) & Ref(md$asettle-md$alow,-1)>Ref(md$asettle-md$alow,-2) & md$asettle-md$alow>Ref(md$asettle-md$alow,-1) & Ref(md$alow,-1)<Ref(md$alow,-2) & md$alow<Ref(md$alow,-1) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & md$asettle<Ref(md$asettle,-1)
  confirmationprice=(md$asettle+md$aopen)/2
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH DESCENT BLOCK",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #21 BEARISH ADVANCE BLOCK
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite|md$normalwhite,-2) & Ref(md$whitecandle,-1) & md$whitecandle & Ref(md$aopen,-1)>Ref(md$aopen,-2) & !Ref(md$gapup,-1) & md$aopen>Ref(md$aopen,-1) & !md$gapup & Ref(md$asettle-md$aopen,-1)<Ref(md$asettle-md$aopen,-2) & md$asettle-md$aopen<Ref(md$asettle-md$aopen,-1) & Ref(md$ahigh-md$asettle,-1)>Ref(md$ahigh-md$asettle,-2) & md$ahigh-md$asettle>Ref(md$ahigh-md$asettle,-1) & Ref(md$ahigh,-1)<Ref(md$ahigh,-2) & md$ahigh>Ref(md$ahigh,-1)   & Ref(md$asettle,-1)>Ref(md$asettle,-2) & md$asettle>Ref(md$asettle,-1)
  confirmationprice=(md$asettle+md$aopen)/2
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH ADVANCE BLOCK",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #22 BULLISH DELIBERATION BLOCK #VERIFIED
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack|md$normalblack,-2) & #Candle 3
    Ref(md$longblack|md$normalblack,-1) & (Ref(md$aopen,-1)<Ref(md$aopen,-2) & Ref(md$aopen,-1)>Ref(md$asettle,-2)) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & Ref(md$body,-1)<Ref(md$body,-2) &#candle 2
    md$gapdown & (md$shortblack|md$doji)
  confirmationprice=(Ref(md$alow,-1)+md$asettle)/2
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH DELIBERATION BLOCK",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #22 BEARISH DELIBERATION BLOCK #VERIFIED
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite|md$normalwhite,-2) & #Candle 3
    Ref(md$longwhite|md$normalwhite,-1) & (Ref(md$aopen,-1)>Ref(md$aopen,-2) & Ref(md$aopen,-1)<Ref(md$asettle,-2)) & Ref(md$asettle,-1)>Ref(md$asettle,-2) & Ref(md$body,-1)<Ref(md$body,-2) &#candle 2
    md$gapup & (md$shortwhite|md$doji)
  confirmationprice=(Ref(md$ahigh,-1)+md$asettle)/2
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH DELIBERATION BLOCK",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #23 BULLISH TWO RABBITS
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-2) & Ref(md$gapdown,-1) & Ref(md$shortwhite,-1) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & md$aopen>Ref(md$aopen,-1) & md$aopen<Ref(md$asettle,-1) & md$asettle>Ref(md$asettle,-2)+Ref(md$body,-2)*0.3
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH TWO RABBITS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #23 BEARISH TWO CROWS
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite,-2) & Ref(md$gapup,-1) & Ref(md$shortblack,-1) & Ref(md$asettle,-1)>Ref(md$asettle,-2) & md$aopen<Ref(md$aopen,-1) & md$aopen>Ref(md$asettle,-1) & md$asettle<Ref(md$asettle,-2)-Ref(md$body,-2)*0.3
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH TWO CROWS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #24 BULLISH THREE INSIDE UP
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & !Ref(md$shortblack,-2) & Ref(md$whitecandle,-1) & Ref(md$asettle,-1)<Ref(md$aopen,-2) & Ref(md$aopen,-1)>Ref(md$asettle,-2) & md$whitecandle & md$asettle>Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH THREE INSIDE UP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #24 BEARISH THREE INSIDE DOWN
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & !Ref(md$shortwhite,-2) & Ref(md$blackcandle,-1) & Ref(md$asettle,-1)>Ref(md$aopen,-2) & Ref(md$aopen,-1)<Ref(md$asettle,-2) & md$blackcandle & md$asettle<Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH THREE INSIDE DOWN",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #25 BULLISH THREE OUTSIDE UP #VERIFIED
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & Ref(md$whitecandle,-1) & !Ref(md$shortwhite,-1) & Ref(md$asettle,-1)>Ref(md$aopen,-2) & Ref(md$aopen,-1)<Ref(md$asettle,-2) & md$whitecandle & md$asettle>Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH THREE OUTSIDE UP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #25 BEARISH THREE OUTSIDE DOWN
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & Ref(md$blackcandle,-1) & !Ref(md$shortblack,-1) & Ref(md$asettle,-1)<Ref(md$aopen,-2) & Ref(md$aopen,-1)>Ref(md$asettle,-2) & md$blackcandle & md$asettle<Ref(md$asettle,-1)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH THREE OUTSIDE DOWN",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #26 BULLISH THREE STARS IN THE SOUTH
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & Ref(md$lowershadow>1.25*md$lowershadowavg,-2) & Ref(md$uppershadow<0.1*md$lowershadowavg,-2) &
    Ref(md$blackcandle,-1) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & Ref(md$aopen,-1)<Ref(md$aopen,-2) & Ref(md$aopen,-1)>Ref(md$asettle,-2) & Ref(md$alow,-1)>Ref(md$alow,-2) & Ref(md$body,-1)<Ref(md$body,-2) & Ref(md$lowershadow,-1)<Ref(md$lowershadow,-2) &
    md$shortblack & md$blackmarubozu & md$alow>Ref(md$alow,-1) & md$body<Ref(md$body,-1)
  confirmationprice=(md$asettle+md$aopen)/2
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH THREE STARS IN THE SOUTH",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #26 BULLISH SQUEEZE ALERT # VERIFIED
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$blackcandle,-2) & !Ref(md$shortblack,-2) &
    Ref(md$ahigh,-1)<Ref(md$ahigh,-2) & Ref(md$alow,-1)>Ref(md$alow,-2) &
    md$ahigh<Ref(md$ahigh,-1) & md$alow>Ref(md$alow,-1)
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH SQUEEZE ALERT",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #26 BEARISH SQUEEZE ALERT # CANNOT RECONCILE, MADE FIRST CANDLE NORMAL OR LARGE SIZE TO CHECK
  duration=2
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$whitecandle,-2) & !Ref(md$shortwhite,-2) &
    Ref(md$ahigh,-1)< Ref(md$ahigh,-2) & Ref(md$alow,-1)>Ref(md$alow,-2) &
    md$ahigh<Ref(md$ahigh,-1) & md$alow>Ref(md$alow,-1)
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH SQUEEZE ALERT",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #27 BULLISH STICK SANDWICH
  duration=2
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-2) &
    Ref(md$whitecandle,-1) & Ref(md$asettle,-1)>Ref(md$aopen,-2) & Ref(md$aopen,-1)>Ref(md$asettle,-2) &
    md$gapup & md$blackcandle & md$asettle==Ref(md$asettle,-1)
  confirmationprice=(md$asettle+Ref(md$asettle,-1))/2
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH STICK SANDWICH",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #27 BULLISH THREE GAP DOWNS #VERIFIED
  duration=2
  pattern= Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$gapdown,-2) & Ref(md$asettle,-2)<Ref(pmin(md$aopen,md$asettle),-3) & # candle 3
    Ref(md$blackcandle,-1) & Ref(md$gapdown,-1) & Ref(md$asettle,-1)<Ref(pmin(md$aopen,md$asettle),-2) & # candle 2
    md$blackcandle & md$gapdown & md$asettle<Ref(md$asettle,-1) # candle 1
  confirmationprice=(md$asettle+md$aopen)/2
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH THREE GAP DOWNS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #27 BEARISH THREE GAP UPS # VERIFIED
  duration=2
  pattern=  Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$gapup,-2) & Ref(md$asettle,-2)>Ref(pmax(md$aopen,md$asettle),-3) & # candle 3
    Ref(md$whitecandle,-1) & Ref(md$gapup,-1) & Ref(md$asettle,-1)>Ref(pmax(md$aopen,md$asettle),-2) & # candle 2
    md$whitecandle & md$gapup & md$asettle>Ref(md$asettle,-1) # candle 1
  confirmationprice=(md$asettle+md$aopen)/2
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH THREE GAP UPS",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #28 BULLISH CONCEALING BABY SWALLOW
  duration=3
  pattern= Ref(md$blackmarubozu,-3) & Ref(md$blackmarubozu,-2) & # candle 3 & 4
    Ref(md$shortblack,-1) & Ref(md$gapdown,-1) & Ref(md$ahigh,-1)>Ref(md$asettle,-2) & Ref(md$uppershadow>md$uppershadowavg,-1) # candle 2
  md$blackcandle & md$aopen>Ref(md$ahigh,-1) & md$asettle<Ref(md$alow,-1) # candle 1
  confirmationprice=Ref(md$asettle+md$aopen,-1)/2
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH CONCEALING BABY SWALLOW",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #29 BULLISH BREAKAWAY
  duration=4
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-4) & # day 5
    Ref(md$shortblack,-3) & md$gapdown & # day 4
    Ref(md$shortwhite|md$shortblack,-2) & Ref(md$ahigh,-2)<Ref(md$ahigh,-3) & Ref(md$alow,-2)<Ref(md$alow,-3) & # day 3
    Ref(md$shortwhite|md$shortblack,-1) & Ref(md$ahigh,-1)<Ref(md$ahigh,-2) & Ref(md$alow,-1)<Ref(md$alow,-2) & # day 2
    md$whitecandle & md$asettle<Ref(md$asettle,-4) & md$asettle>Ref(md$aopen,-3)
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH BREAKAWAY",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #29 BEARISH BREAKAWAY
  duration=4
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite,-4) & # day 5
    Ref(md$shortwhite,-3) & md$gapup & # day 4
    Ref(md$shortwhite|md$shortblack,-2) & Ref(md$ahigh,-2)>Ref(md$ahigh,-3) & Ref(md$alow,-2)>Ref(md$alow,-3) & # day 3
    Ref(md$shortwhite|md$shortblack,-1) & Ref(md$ahigh,-1)>Ref(md$ahigh,-2) & Ref(md$alow,-1)>Ref(md$alow,-2) & # day 2
    md$blackcandle & md$asettle>Ref(md$asettle,-4) & md$asettle<Ref(md$aopen,-3)
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH BREAKAWAY",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #30 BULLISH LADDER BOTTOM
  duration=4
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-4) & # day 5
    Ref(md$longblack,-3) & Ref(md$aopen,-3)<Ref(md$aopen,-4) & !Ref(md$gapdown,-3) & # day 4
    Ref(md$longblack,-2) & Ref(md$aopen,-2)<Ref(md$aopen,-3) & !Ref(md$gapdown,-2) & # day 3
    Ref(md$shortblack,-1) & Ref(md$asettle,-1)<Ref(md$asettle,-2) & Ref(md$uppershadow>1.25*md$uppershadowavg,-1) & Ref(md$ahigh,-1)>Ref(md$asettle+md$body*0.5,-2) & # day 2
    md$longwhite & md$gapup
  confirmationprice=md$asettle
  stoploss=md$alow
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH LADDER BOTTOM",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #30 BEARISH LADDER TOP
  duration=4
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite,-4) & # day 5
    Ref(md$longwhite,-3) & Ref(md$aopen,-3)>Ref(md$aopen,-4) & !Ref(md$gapup,-3) & # day 4
    Ref(md$longblack,-2) & Ref(md$aopen,-2)>Ref(md$aopen,-3) & !Ref(md$gapup,-2) & # day 3
    Ref(md$shortwhite,-1) & Ref(md$asettle,-1)>Ref(md$asettle,-2) & Ref(md$lowershadow>1.25*md$lowershadowavg,-1) & Ref(md$alow,-1)<Ref(md$asettle-md$body*0.5,-2) & # day 2
    md$longblack & md$gapdown
  confirmationprice=md$asettle
  stoploss=md$ahigh
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH LADDER TOP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #31 BULLISH AFTER BOTTOM GAP UP
  duration=4
  pattern=Ref((md$candlesticktrend==-1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longblack,-4) & # day 5
    Ref(md$longblack,-3) & Ref(md$aopen,-3)<Ref(md$aopen,-4) & !Ref(md$gapdown,-3) & # day 4
    Ref(md$longblack,-2) & Ref(md$gapdown,-2) & # day 3
    Ref(md$longwhite,-1) & Ref(md$aopen,-1)>Ref(md$aopen,-2) & Ref(md$asettle,-1)>Ref(md$asettle,-3) & # day 2
    md$longwhite & md$gapup # day 1
  confirmationprice=md$asettle
  stoploss=pmin(md$alow,Ref(md$alow,-1))
  #entryprice=ifelse(Ref(md$whitecandle,1) & pattern & conservative & Ref(md$asettle,1)>confirmationprice & Ref(md$alow,1)>stoploss,Ref(md$asettle,1),ifelse(Ref(md$whitecandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss<Ref(md$alow,1))|Ref(md$alow,1)>=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BULLISH AFTER BOTTOM GAP UP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }

  #31 BEARISH AFTER TOP GAP DOWN
  duration=4
  pattern=Ref((md$candlesticktrend==1|md$candlesticktrend==0),-trendBeginning*duration) & Ref(md$longwhite,-4) & # day 5
    Ref(md$longwhite,-3) & Ref(md$aopen,-3)>Ref(md$aopen,-4) & !Ref(md$gapup,-3) & # day 4
    Ref(md$longwhite,-2) & Ref(md$gapup,-2) & # day 3
    Ref(md$longblack,-1) & Ref(md$aopen,-1)<Ref(md$aopen,-2) & Ref(md$asettle,-1)<Ref(md$asettle,-3) & # day 2
    md$longblack & md$gapdown # day 1
  confirmationprice=md$asettle
  stoploss=pmax(md$ahigh,Ref(md$ahigh,-1))
  #entryprice=ifelse(Ref(md$blackcandle,1) & pattern & conservative & Ref(md$asettle,1)<confirmationprice & Ref(md$ahigh,1)<stoploss,Ref(md$asettle,1),ifelse(Ref(md$blackcandle,1) & pattern & !conservative & ((confirmationprice<Ref(md$ahigh,1) & confirmationprice>Ref(md$alow,1) & stoploss>Ref(md$ahigh,1))|Ref(md$ahigh,1)<=confirmationprice),Ref(md$asettle,1),0))
  #confirmed=entryprice>0
  #confirmationdate=ifelse(confirmed,Ref(md$date,1),NA)
  indices=which(pattern==TRUE)
  if(length(indices)>0){
    out=data.frame(symbol=md$symbol[indices],date=md$date[indices],pattern="BEARISH LADDER TOP",confirmationprice=confirmationprice[indices],stoploss=stoploss[indices],duration=duration,stringsAsFactors = FALSE)
    signals=rbind(signals,out)
  }
  if(nrow(signals)>0){
    signals=signals[order(signals$date),]
  }
  signals.confirmed=signals[signals$confirmed,]
  md$candlesticksignals=NA_character_
  if(nrow(signals.confirmed)>0){
    string=paste(signals.confirmed$pattern,"_",signals.confirmed$confirmationprice,sep="")
    index=match(signals.confirmed$confirmationdate,md$date)
    for(i in 1:nrow(signals.confirmed)){
      if(is.na(md$candlesticksignals[index[i]])){
        md$candlesticksignals[index[i]]=paste(signals.confirmed$pattern[i],"_",signals.confirmed$confirmationprice[i],sep="")
      }else{
        md$candlesticksignals[index[i]]=paste(md$candlesticksignals[index[i]],";",signals.confirmed$pattern[i],"_",signals.confirmed$confirmationprice[i],sep="")

      }
    }

  }
  signals1=getCandleStickConfirmation(md,signals$pattern,signals$date,signals$confirmationprice,signals$stoploss,maxWait=maxWait)
  signals1$symbol=signals$symbol
  signals1$duration=signals$duration
  signals1$confirmed=ifelse(!is.na(signals1$confirmationdate),TRUE,FALSE)
  list("marketdata"=md,"pattern"=signals1)
}
