/*

## FUNCTIONS IN CPP
#### Indicators ####
# Trend()

#### BackTest Functions #####
# GenerateSignals aka ApplyStop in Amibroker
# GenerateTrades

#### Utility Functions#####
# Ref
# ExRem
# Flip
# BarsSince
# Cross

 */

#include <Rcpp.h>
#include <cmath>
#include <boost/date_time.hpp>
#include <boost/lexical_cast.hpp>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>


using namespace Rcpp;
using namespace std;

// [[Rcpp::export]]

DatetimeVector UniqueDateTime(DatetimeVector label){
  vector<Datetime> out;
  for(int i=0;i<label.size();i++){
    if(std::find(out.begin(), out.end(), label[i]) == out.end()){
      out.push_back(label[i]);
    }
  }
  return wrap(out);
}

// [[Rcpp::depends(BH)]]

namespace bt = boost::posix_time;

const std::locale formats[] = {    // this shows a subset only, see the source file for full list
  std::locale(std::locale::classic(), new bt::time_input_facet("%Y-%m-%d %H:%M:%S%f")),
  std::locale(std::locale::classic(), new bt::time_input_facet("%Y/%m/%d %H:%M:%S%f")),

  std::locale(std::locale::classic(), new bt::time_input_facet("%Y-%m-%d")),
  std::locale(std::locale::classic(), new bt::time_input_facet("%b/%d/%Y")),
};
const size_t nformats = sizeof(formats)/sizeof(formats[0]);

double stringToTime(const std::string s) {
  bt::ptime pt,ptbase;
  // loop over formats and try them til one fits
  for (size_t i=0; pt == ptbase && i < nformats; ++i) {
    std::istringstream is(s);
    is.imbue(formats[i]);
    is >> pt;
  }

  if (pt == ptbase) {
    return NAN;
  } else {
    const bt::ptime timet_start(boost::gregorian::date(1970,1,1));
    bt::time_duration diff = pt - timet_start;

    // Define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG to use nanoseconds
    // (and then use diff.total_nanoseconds()/1.0e9;  instead)
    return diff.total_microseconds()/1.0e6;
  }
}

template <int RTYPE>
Rcpp::DatetimeVector toPOSIXct_impl(const Rcpp::Vector<RTYPE>& sv) {

  int n = sv.size();
  Rcpp::DatetimeVector pv(n);

  for (int i=0; i<n; i++) {
    std::string s = boost::lexical_cast<std::string>(sv[i]);
    //Rcpp::Rcout << sv[i] << " -- " << s << std::endl;

    // Boost Date_Time gets the 'YYYYMMDD' format wrong, even
    // when given as an explicit argument. So we need to test here.
    // While we are at it, may as well test for obviously wrong data.
    int l = s.size();
    if ((l < 8) ||          // impossibly short
        (l == 9)) {         // 8 or 10 works, 9 cannot
      Rcpp::stop("Inadmissable input: %s", s);
    } else if (l == 8) {    // turn YYYYMMDD into YYYY/MM/DD
      s = s.substr(0, 4) + "/" + s.substr(4, 2) + "/" + s.substr(6,2);
    }
    pv[i] = stringToTime(s);
  }
  return pv;
}


Rcpp::DatetimeVector toPOSIXct(SEXP x) {
  if (Rcpp::is<Rcpp::CharacterVector>(x)) {
    return toPOSIXct_impl<STRSXP>(x);
  } else if (Rcpp::is<Rcpp::IntegerVector>(x)) {
    return toPOSIXct_impl<INTSXP>(x);
  } else if (Rcpp::is<Rcpp::NumericVector>(x)) {
    // here we have two cases: either we are an int like
    // 200150315 'mistakenly' cast to numeric, or we actually
    // are a proper large numeric (ie as.numeric(Sys.time())
    Rcpp::NumericVector v(x);
    if (v[0] < 21990101) {  // somewhat arbitrary cuttoff
      // actual integer date notation: convert to string and parse
      return toPOSIXct_impl<REALSXP>(x);
    } else {
      // we think it is a numeric time, so treat it as one
      return Rcpp::DatetimeVector(x);
    }
  } else {
    Rcpp::stop("Unsupported Type");
    return R_NilValue;//not reached
  }
}
// [[Rcpp::export]]
DatetimeVector subsetDateVector(DatetimeVector dv,IntegerVector iv) {

  Function formatDate("format.Date");
  CharacterVector dvc(dv.size());
  for (int i = 0; i < dv.size(); i++) {
    dvc[i] = as<std::string>(formatDate(wrap(dv[i])));
  }
  CharacterVector dv1 = dvc[iv];
  DatetimeVector out= toPOSIXct(dv1);
  return out;

}


bool contains(NumericVector X, int z) {
  return std::find(X.begin(), X.end(), z)!=X.end();
}


// [[Rcpp::export]]
DataFrame ExecutionPriceDiff(DataFrame expected,DataFrame actual){
  StringVector esymbol=expected["symbol"];
  StringVector eentryside=expected["trade"];
  StringVector eentrytimestamp=expected["entrytimetext"];
  StringVector eexittimestamp=expected["exittimetext"];
  NumericVector eentrysize=expected["entrysize"];
  NumericVector eentryprice=expected["entryprice"];
  NumericVector eexitprice=expected["exitprice"];
  StringVector asymbol=actual["symbol"];
  StringVector aentryside=actual["trade"];
  StringVector aentrytimestamp=actual["entrytimetext"];
  StringVector aexittimestamp=actual["exittimetext"];
  NumericVector aentrysize=actual["entrysize"];
  NumericVector aentryprice=actual["entryprice"];
  NumericVector aexitprice=actual["exitprice"];
  vector<int>aindex;
  vector<int>eindex;
  vector<const char*> symbol;
  vector<const char*> entryside;
  vector<int> entrysizediff;
  vector<double> entrypricediff;
  vector<double> exitpricediff;
  vector<double> pnlimpact;
  Rcout << " eentrytimestamp length:"<<eentrytimestamp.size()<<std::endl;
  for(int i=0;i<eentrytimestamp.size();i++){
    for(int j=0;j<aentrytimestamp.size();j++){
      Rcout << "i: "<<i<<",eentrytimestamp[i]: "<<eentrytimestamp[i]<<",aentrytimestamp[j]: "<<aentrytimestamp[j]<<std::endl;
      if(eentrytimestamp[i]==aentrytimestamp[j] && eentryside[i]==aentryside[j] && esymbol[i]==asymbol[j]){
        eindex.push_back(i);
        aindex.push_back(j);
        symbol.push_back(esymbol[i]);
        entryside.push_back(eentryside[i]);
        entrysizediff.push_back(aentrysize[j]-eentrysize[i]);
        if(eentryside[i]=="BUY"){
          entrypricediff.push_back(eentryprice[i]-aentryprice[j]);
          exitpricediff.push_back(aexitprice[j]-eexitprice[i]);
        }else{
          entrypricediff.push_back(aentryprice[i]-eentryprice[j]);
          exitpricediff.push_back(eexitprice[j]-aexitprice[i]);
        }
      }
    }
  }
  StringVector expentrydate(eindex.size());
  StringVector expexitdate(eindex.size());
  StringVector actexitdate(aindex.size());
  Rcout << " asize#:"<<aindex.size()<< " ,esize:"<<eindex.size()<<std::endl;

  for(int i=0;i<eindex.size();i++){
    Rcout << " index#:"<<i<< " ,i value:"<<eindex.at(i)<<std::endl;
    expentrydate[i]=eentrytimestamp[eindex.at(i)];
    expexitdate[i]=eexittimestamp[eindex.at(i)];
    actexitdate[i]=aexittimestamp[aindex.at(i)];
  }

  for(int j=0;j<aindex.size();j++){
    Rcout << " index#:"<<j<< " ,j value:"<<aindex.at(j)<<std::endl;
    actexitdate[j]=aexittimestamp[aindex.at(j)];
  }

  NumericVector pnldiff(eindex.size());

  for(int i=0;i<aindex.size();i++){
    Rcout << " entrysize:"<<aentrysize[aindex.at(i)]<< ",entrypricediff:"<<entrypricediff.at(i)<<",exitpricediff:"<<exitpricediff.at(i)<<std::endl;
    pnldiff[i]=aentrysize[aindex.at(i)]*(entrypricediff.at(i)+exitpricediff.at(i));
  }

  return DataFrame::create(_("symbol")=wrap(symbol),_("entrydate")=expentrydate,_("entrypricediff")=wrap(entrypricediff),
                             _("expexitdate")=expexitdate,_("actexitdate")=actexitdate,_("exitpricediff")=wrap(exitpricediff),_("entrysizediff")=wrap(entrysizediff),
                               _("pnldiff")=wrap(pnldiff),_("aindex")=wrap(aindex),_("eindex")=wrap(eindex));
}


// [[Rcpp::export]]
DataFrame ExecutionSideDiff(DataFrame expected,DataFrame actual,bool option=true){
  StringVector eentrytimestamp=expected["entrytimetext"];
  StringVector aentrytimestamp=actual["entrytimetext"];
  StringVector eexittimestamp=expected["exittimetext"];
  StringVector aexittimestamp=actual["exittimetext"];
  StringVector esymbol=expected["symbol"];
  StringVector asymbol=actual["symbol"];
  StringVector eentryside=expected["trade"];
  StringVector aentryside=actual["trade"];
  NumericVector aentrysize=actual["entrysize"];
  NumericVector eentrysize=expected["entrysize"];
  NumericVector aentryprice=actual["entryprice"];
  NumericVector eentryprice=expected["entryprice"];
  NumericVector aexitprice=actual["exitprice"];
  NumericVector eexitprice=expected["exitprice"];
  vector<int>aindex;
  vector<int>eindex;
  vector<const char*> symbol;
  vector<const char*> entryside;
  vector<int> entrysizediff;
  vector<double> entrypricediff;
  vector<double> exitpricediff;
  vector<double> pnlimpact;
  Rcout << " eentrytimestamp length:"<<eentrytimestamp.size()<<std::endl;
  for(int i=0;i<eentrytimestamp.size();i++){
    for(int j=0;j<aentrytimestamp.size();j++){
      Rcout << "i: "<<i<<",eentrytimestamp[i]: "<<eentrytimestamp[i]<<",aentrytimestamp[j]: "<<aentrytimestamp[j]<<std::endl;
      bool check=false;
      if(option){
        check=eentrytimestamp[i]==aentrytimestamp[j] && eentryside[i]==aentryside[j] && esymbol[i]!=asymbol[j];
        if(check){
          // check if options have opposite values CALL vs PUT.
        }
      }else{
        check=eentrytimestamp[i]==aentrytimestamp[j] && eentryside[i]!=aentryside[j] && esymbol[i]==asymbol[j];
      }
      if(check){
        eindex.push_back(i);
        aindex.push_back(j);
        symbol.push_back(esymbol[i]);
        entryside.push_back(eentryside[i]);
        entrysizediff.push_back(aentrysize[j]-eentrysize[i]);
        entrypricediff.push_back(eentryprice[i]+aentryprice[j]);
        exitpricediff.push_back(aexitprice[j]+eexitprice[i]);

      }
    }
  }
  StringVector expentrydate(eindex.size());
  StringVector expexitdate(eindex.size());
  StringVector actexitdate(aindex.size());
  for(int i=0;i<eindex.size();i++){
    Rcout << " index#:"<<i<< " ,i value:"<<eindex.at(i)<<std::endl;
    expentrydate[i]=eentrytimestamp[eindex.at(i)];
    expexitdate[i]=eexittimestamp[eindex.at(i)];
    actexitdate[i]=aexittimestamp[aindex.at(i)];
  }
  for(int j=0;j<aindex.size();j++){
    Rcout << " index#:"<<j<< " ,j value:"<<aindex.at(j)<<std::endl;
    actexitdate[j]=aexittimestamp[aindex.at(j)];
  }

  StringVector pnldiff(eindex.size());

  for(int i=0;i<eindex.size();i++){
    Rcout << " entrysize:"<<aentrysize[aindex.at(i)]<< ",entrypricediff:"<<entrypricediff.at(i)<<",exitpricediff:"<<exitpricediff.at(i)<<std::endl;
    //    if(std::strncmp(entryside.at(i),"BUY",3)){
    if (strncmp(entryside.at(i),"BUY",3)==0){
      pnldiff[i]=aentrysize[aindex.at(i)]*(entrypricediff.at(i)-exitpricediff.at(i));
    }
    else{
      pnldiff[i]=aentrysize[aindex.at(i)]*(exitpricediff.at(i)-entrypricediff.at(i));
    }
  }
  return DataFrame::create(_("symbol")=wrap(symbol),_("entrydate")=expentrydate,_("entrypricediff")=wrap(entrypricediff),
                             _("expexitdate")=expexitdate,_("actexitdate")=actexitdate,_("exitpricediff")=wrap(exitpricediff),_("entrysizediff")=wrap(entrysizediff),
                               _("pnldiff")=wrap(pnldiff),_("aindex")=wrap(aindex),_("eindex")=wrap(eindex));
}

// [[Rcpp::export]]
DataFrame MissingTrades(DataFrame expected,DataFrame recon){
  NumericVector eindex=recon["eindex"];
  StringVector esymbol=expected["symbol"];
  StringVector eentryside=expected["trade"];
  NumericVector eentrysize=expected["entrysize"];
  StringVector eentrytime=expected["entrytimetext"];
  NumericVector eentryprice=expected["entryprice"];
  StringVector eexittime=expected["exittimetext"];
  NumericVector eexitprice=expected["exitprice"];

  vector<const char*> symbol;
  vector<const char*> entryside;
  vector<int> entrysize;
  vector<const char*> entrytime;
  vector<double> entryprice;
  vector<const char*> exittime;
  vector<double> exitprice;
  vector<double> pnlimpact;
  for(int i=0;i<esymbol.size();i++){
    if(!contains(eindex,i)){
      //add row
      symbol.push_back(esymbol[i]);
      entryside.push_back(eentryside[i]);
      entrysize.push_back(eentrysize[i]);
      entrytime.push_back(eentrytime[i]);
      entryprice.push_back(eentryprice[i]);
      exittime.push_back(eexittime[i]);
      exitprice.push_back(eexitprice[i]);
    }
  }

  StringVector pnldiff(symbol.size());
  for(int i=0;i<symbol.size();i++){
    if(strncmp(entryside.at(i),"BUY",3)==0){
      pnldiff[i]=-entrysize.at(i)*(exitprice.at(i)-entryprice.at(i));
    }
    else{
      pnldiff[i]=-entrysize.at(i)*(entryprice.at(i)-exitprice.at(i));
    }
  }
  return DataFrame::create(_("symbol")=wrap(symbol),_("side")=wrap(entryside),_("size")=wrap(entrysize),
                           _("entrytime")=wrap(entrytime),_("entryprice")=wrap(entryprice),_("exittime")=wrap(exittime),_("exitprice")=wrap(exitprice),
                           _("pnldiff")=wrap(pnldiff));
}

// [[Rcpp::export]]
DataFrame ExtraTrades(DataFrame actual,DataFrame recon){
  NumericVector aindex=recon["aindex"];
  StringVector esymbol=actual["symbol"];
  StringVector eentryside=actual["trade"];
  NumericVector eentrysize=actual["entrysize"];
  StringVector eentrytime=actual["entrytimetext"];
  NumericVector eentryprice=actual["entryprice"];
  StringVector eexittime=actual["exittimetext"];
  NumericVector eexitprice=actual["exitprice"];

  vector<const char*> symbol;
  vector<const char*> entryside;
  vector<int> entrysize;
  vector<const char*> entrytime;
  vector<double> entryprice;
  vector<const char*> exittime;
  vector<double> exitprice;
  vector<double> pnlimpact;
  for(int i=0;i<esymbol.size();i++){
    if(!contains(aindex,i)){
      //add row
      symbol.push_back(esymbol[i]);
      entryside.push_back(eentryside[i]);
      entrysize.push_back(eentrysize[i]);
      entrytime.push_back(eentrytime[i]);
      entryprice.push_back(eentryprice[i]);
      exittime.push_back(eexittime[i]);
      exitprice.push_back(eexitprice[i]);
    }
  }

  StringVector pnldiff(symbol.size());
  for(int i=0;i<symbol.size();i++){
    if(strncmp(entryside.at(i),"BUY",3)==0){
      pnldiff[i]=entrysize.at(i)*(exitprice.at(i)-entryprice.at(i));
    }
    else{
      pnldiff[i]=entrysize.at(i)*(entryprice.at(i)-exitprice.at(i));
    }
  }
  return DataFrame::create(_("symbol")=wrap(symbol),_("side")=wrap(entryside),_("size")=wrap(entrysize),
                           _("entrytime")=wrap(entrytime),_("entryprice")=wrap(entryprice),_("exittime")=wrap(exittime),_("exitprice")=wrap(exitprice),
                           _("pnldiff")=wrap(pnldiff));
}


// [[Rcpp::export]]
NumericVector Ref(NumericVector input,NumericVector shift){

  int nSize=input.size();
  NumericVector newshift(nSize);
  if(shift.size()==1){
    for(int i=0;i<nSize;i++){
      newshift[i]=shift[0];
    }
  }else{
    newshift=shift;
  }
  NumericVector result(nSize);
  for(int i=0;i<nSize;i++){
    if((newshift[i]>=0) && ((i+newshift[i])<nSize)){
      //Rcout<<"input[i+newshift[i]]:"<<input[i+newshift[i]]<<"bar:"<<i<<std::endl;
      result[i]=input[i+newshift[i]];
    }else if((newshift[i]<0) && ((i+newshift[i])>=0)){
      result[i]=input[i+newshift[i]];
    } else{
      //Rcout << "bar:"<<i<<std::endl;
      result[i]=NA_REAL;
    }
  }
  return result;
}

// [[Rcpp::export]]
CharacterVector CRef(CharacterVector input,NumericVector shift){

  int nSize=input.size();
  NumericVector newshift(nSize);
  if(shift.size()==1){
    for(int i=0;i<nSize;i++){
      newshift[i]=shift[0];
    }
  }else{
    newshift=shift;
  }
  CharacterVector result(nSize);
  for(int i=0;i<nSize;i++){
    if((newshift[i]>=0) && ((i+newshift[i])<nSize)){
      //Rcout<<"input[i+newshift[i]]:"<<input[i+newshift[i]]<<"bar:"<<i<<std::endl;
      result[i]=input[i+newshift[i]];
    }else if((newshift[i]<0) && ((i+newshift[i])>=0)){
      result[i]=input[i+newshift[i]];
    } else{
      //Rcout << "bar:"<<i<<std::endl;
      result[i]=NA_STRING;
    }
  }
  return result;
}


// [[Rcpp::export]]
NumericVector ExRem(NumericVector vec1, NumericVector vec2){
  int nSize=vec1.size();
  NumericVector result(nSize);
  // made vec2received default state to true on 5-Apr-2017.
  //bool vec2received=false;

  bool vec2received=true;
  for(int i=1;i<nSize;i++){

    if((vec1[i]>0 && vec2received)){
      result[i]=vec1[i];
      vec2received=false;
      //Rcout << " bar1:"<<i<< "result[i]:"<<result[i]<<std::endl;
    }
    if(vec2[i]>0 && result[i]>0){//we received a positive in vec1 in the same bar as positive in vec2
      //both vecs neutralized.
      result[i]=0;
      vec2received=true; // if buy and sell occur together, we dont select buy signal, but will allow next buy signal to pass.
      //Rcout << " bar2:"<<i<< "result[i]:"<<result[i]<<std::endl;
    }else if(vec2[i]>0){
      vec2received=true;
      //Rcout << " bar3:"<<i<< "result[i]:"<<result[i]<<std::endl;

    }

  }
  return result;
}


// [[Rcpp::export]]
NumericVector Flip(NumericVector vec1, NumericVector vec2){
  NumericVector result=ExRem(vec1,vec2);
  for(int i=1;i<result.size();i++){
    if((result[i-1]>0 && vec2[i]==0)||(result[i-1]>0 && vec2[i]>0 && vec1[i]>0)){
      result[i]=result[i-1];
    }
  }
  return result;
}


// [[Rcpp::export]]
NumericVector BarsSince(NumericVector vec1){
  int nSize=vec1.size();
  NumericVector result(nSize);
  for(int i=0;i<nSize;i++){
    if (vec1[i]==1){
      result[i]=0;
    }else{
      result[i]=result[i-1]+1;
    }
  }
  return result;
}


// [[Rcpp::export]]
NumericVector Cross(NumericVector snake,NumericVector reference){
  int snakeSize=snake.size();
  int referenceSize=reference.size();
  //Rcout<<"ReferenceSize:"<<referenceSize<<endl;
  //Rcout<<"SnakeSize:"<<snakeSize<<endl;

  NumericVector snakevector(max(snakeSize,referenceSize));
  NumericVector referencevector(max(snakeSize,referenceSize));
  if(snakeSize==1 && referenceSize>1){
    for(int i=0;i<referenceSize;i++){
      snakevector[i]=snake[0];
    }
    referencevector=reference;
  }

  if(snakeSize>1 && referenceSize==1){
    for(int i=0;i<snakeSize;i++){
      referencevector[i]=reference[0];
    }
    snakevector=snake;
  }

  if(snakeSize==referenceSize){
    referencevector=reference;
    snakevector=snake;
  }
  snakeSize=snakevector.size();
  NumericVector result(snakeSize);
  //Rcout<<"SnakeSize:"<<snakeSize<<endl;
  //Rcout<<"ReferenceSize:"<<referencevector.size()<<endl;


  bool set=false;
  for(int i=1;i<snakeSize;i++){
    if((set==false) && (snakevector[i]>referencevector[i])){
      result[i]=1;
      set=true;
      //   Rcout<<"Snake above reference. Set to true for bar:"<<i<<",Snake:"<<snakevector[i]<<",Ref:"<<referencevector[i]<<endl;
    }else if(snakevector[i]<referencevector[i]){
      // Rcout<<"Snake below reference. Set to false for bar:"<<i<<",Snake:"<<snakevector[i]<<",Ref:"<<referencevector[i]<<endl;
      set=false;
    }

  }
  return result;
}

// [[Rcpp::export]]

StringVector linkedsymbols(StringVector initial,StringVector final,String symbol)
{
  int nSize=final.size();
  vector<string>out;
  bool found=true;
  string value=symbol;
  while(found){
    out.push_back(value);
    label:
      for(int i=0;i<nSize;i++){
        if(final[i]==value){//original symbol has an initial value
          if(initial[i]!=value){
            value=initial[i];
            out.push_back(value);
            goto label;
          }else{
            break; //example ILFSTRANS & IL&FSTRANS
          }
        }
        found=false;
      }
      return wrap(out);
  }

  return wrap(out);
}


NumericVector Shift(NumericVector vec1,NumericVector vec2, int ref){
  int nSize=vec1.size();
  NumericVector result(nSize);
  for(int i=1;i<nSize;i++){
    bool trigger = (vec2[i] != vec2[i - 1]) && (vec2[i] == ref);
    if((vec1[i]!=vec1[i-1])|trigger){
      result[i]=vec1[i-1];
    }else{
      result[i]=result[i-1];
    }
  }
  return result;
}

// [[Rcpp::export]]
IntegerVector whichDate2(DatetimeVector x, Datetime condition) {
  IntegerVector v = Rcpp::seq(0, x.size()-1);
  //Rcout<<x.size()<<std::endl;
  DatetimeVector check(x.size());
  vector<int> indices;
  for(int i=0;i<x.size();i++){
    //Rcout<<x[i]<<","<<condition<<std::endl;
    if(x[i]==condition){
      indices.push_back(v[i]);
    }
  }
  return wrap(indices);

}

IntegerVector whichString2(StringVector x, String condition) {
  IntegerVector v = Rcpp::seq(0, x.size()-1);
  DatetimeVector check(x.size());
  vector<int> indices;
  for(int i=0;i<x.size();i++){
    if(x[i]==condition){
      indices.push_back(v[i]);
    }
  }
  return wrap(indices);

}


DataFrame CalculateEquityCurve(String symbol,DataFrame all, DataFrame trades,NumericVector size, double brokerage){
  //This calculates equity for a single security.
  int nSize=all.nrows();
  int nTrades=trades.nrows();
  const DatetimeVector timestamp=all["date"];
  const NumericVector close=all["close"];
  const DatetimeVector entrytime=trades["entrytime"];
  const DatetimeVector exittime=trades["exittime"];
  const StringVector symbols=trades["symbol"];
  //const StringVector trade=trades["trade"];
  std::vector<std::string> trade = Rcpp::as<std::vector<std::string> >(trades["trade"]);
  const NumericVector entryprice=trades["entryprice"];
  const NumericVector exitprice=trades["exitprice"];
  NumericVector profit(nSize);
  NumericVector value(nSize);
  NumericVector contracts(nSize);
  NumericVector exposure(nSize);
  NumericVector brokeragecost(nSize);
  //std::string fmt =Rcpp::as<std::string>( format );
  //value[i]=i>0?value[i-1]:0;
  //contracts[i]=i>0?size[i-1]:0;
  //Build Positions and exposure
  int entrysize=0;
  for(int i=0;i<nSize;i++){
    if(i>0){
      contracts[i]=contracts[i-1];
      value[i]=value[i-1];
      brokeragecost[i]=brokeragecost[i-1];
    }
    if(i<0){
      //Rcout << " bar:"<<i<<std::endl;
    }
    for(int j=0;j<nTrades;j++){

      if(entrytime[j]>=timestamp[0]){ //only process if the trade entry is after the first timestamp provided
        if((entrytime[j]==timestamp[i]) && (symbols[j]==symbol)){
          value[i]=(trade[j].find("SHORT")==string::npos)?(value[i]-(size[i]*entryprice[j])):(value[i]+(size[i]*entryprice[j]));
          contracts[i]=(trade[j].find("SHORT")==string::npos)?contracts[i]+size[i]:contracts[i]-size[i];
          brokeragecost[i]=brokeragecost[i]+brokerage*size[i]*entryprice[j];
          entrysize=size[i];
          if(i<0){
            //Rcout << " Entry bar:"<<i << " ,trade bar:"<<j<<" ,trade:" <<trade[j] <<" ,current contracts:"<< contracts[i]<<+
            //        " ,new value:" <<value[i] << " ,size:" <<size[i] <<" ,BrokerageCost:"<<brokeragecost[i]<<std::endl;
          }
        }
        if ((exittime[j]==timestamp[i]) && (symbols[j]==symbol)){
          value[i]=(trade[j].find("BUY")==string::npos)?(value[i]-(entrysize*exitprice[j])):(value[i]+(entrysize*exitprice[j]));
          contracts[i]=(trade[j].find("BUY")==string::npos)?contracts[i]+entrysize:contracts[i]-entrysize;
          brokeragecost[i]=brokeragecost[i]+brokerage*entrysize*exitprice[j];
          if(i<0){
            //Rcout << " Exit bar:"<<i <<" ,trade bar:" << j <<" ,trade:"<<trade[j] <<" ,current contracts:"<<contracts[i]<<+
            //        " ,new value:" <<value[i] << " ,size:" <<entrysize << " ,BrokerageCost:"<<brokeragecost[i]<<std::endl;
          }
          entrysize=0;
        }
      }
    }
  }
  //mtm positions
  for(int i=0;i<nSize;i++){
    profit[i]=value[i]+contracts[i]*close[i]-brokeragecost[i];//
    exposure[i]=abs(contracts[i])*close[i];
  }

  return DataFrame::create(_["date"]=timestamp,_["marketdues"]=value,_["contracts"]=contracts,_["profit"]=profit,_["close"]=close,_["brokerage"]=brokeragecost,_["exposure"]=exposure);

}

// [[Rcpp::export]]
DataFrame CalculatePortfolioEquityCurve(String symbol,DataFrame all, DataFrame trades,NumericVector size, double brokerage){
  //This calculates equity for a single security.
  //int nSize=all.nrows();

  DatetimeVector timestamp=all["date"];
  const NumericVector close=all["close"];
  const DatetimeVector entrytime=trades["entrytime"];
  const DatetimeVector exittime=trades["exittime"];
  const StringVector tradesymbols=trades["symbol"];
  const StringVector mdsymbols=all["symbol"];
  //const StringVector trade=trades["trade"];
  std::vector<std::string> trade = Rcpp::as<std::vector<std::string> >(trades["trade"]);
  const NumericVector entryprice=trades["entryprice"];
  const NumericVector exitprice=trades["exitprice"];
  IntegerVector tradeindices=whichString2(tradesymbols,symbol);
  IntegerVector mdindices=whichString2(mdsymbols,symbol);
  int nTrades=tradeindices.size();
  int nIndices=mdindices.size();
  NumericVector profit(nIndices);
  NumericVector value(nIndices);
  NumericVector contracts(nIndices);
  NumericVector exposure(nIndices);
  NumericVector brokeragecost(nIndices);
  DatetimeVector subtimestamp(mdindices.size());
  for(int i=0;i<mdindices.size();i++){
    subtimestamp[i]=timestamp[mdindices[i]];
  }
  NumericVector subclose=close[mdindices];
  //std::string fmt =Rcpp::as<std::string>( format );
  //value[i]=i>0?value[i-1]:0;
  //contracts[i]=i>0?size[i-1]:0;
  //Build Positions and exposure
  NumericVector entrysize(nIndices);
  for(int a=0;a<nIndices;a++){
    int i=mdindices[a];
    if(a>0){
      contracts[a]=contracts[a-1];
      value[a]=value[a-1];
      brokeragecost[a]=brokeragecost[a-1];
    }
    if(a<0){
      //Rcout << " bar:"<<i<<std::endl;
    }
    for(int b=0;b<nTrades;b++){
      int j=tradeindices[b];
      if(entrytime[j]>=subtimestamp[0]){ //only process if the trade entry is after the first timestamp provided
        if((entrytime[j]==subtimestamp[i]) && (tradesymbols[j]==symbol)){
          value[a]=(trade[j].find("SHORT")==string::npos)?(value[a]-(size[i]*entryprice[j])):(value[a]+(size[i]*entryprice[j]));
          contracts[a]=(trade[j].find("SHORT")==string::npos)?contracts[a]+size[i]:contracts[a]-size[i];
          brokeragecost[a]=brokeragecost[a]+brokerage*size[i]*entryprice[j];
          entrysize[i]=size[i];
          //Rcout<<"Entry"<<",Trade:"<<trade[j]<<",value:"<<value[a]<<",contracts:"<<contracts[a]<<",entrysize:"<<entrysize[i]<<",i:"<<i<<",a:"<<a<<",j:"<<j<<endl;
          if(i<0){
            //Rcout << " Entry bar:"<<i << " ,trade bar:"<<j<<" ,trade:" <<trade[j] <<" ,current contracts:"<< contracts[i]<<+
            //  " ,new value:" <<value[i] << " ,size:" <<size[i] <<" ,BrokerageCost:"<<brokeragecost[i]<<std::endl;
          }
        }
        if ((exittime[j]==subtimestamp[i]) && (tradesymbols[j]==symbol)){
          IntegerVector entryindex= whichDate2(subtimestamp,entrytime[j]) ;//index of corresponding purchase.
          //Rcout<<"EntryIndex:"<<entryindex[0]<<endl;
          int exitsize=entrysize[entryindex[0]];
          value[a]=(trade[j].find("BUY")==string::npos)?(value[a]-(exitsize*exitprice[j])):(value[a]+(exitsize*exitprice[j]));
          contracts[a]=(trade[j].find("BUY")==string::npos)?contracts[a]+exitsize:contracts[a]-exitsize;
          brokeragecost[a]=brokeragecost[a]+brokerage*exitsize*exitprice[j];
          if(i<0){
            //Rcout << " Exit bar:"<<i <<" ,trade bar:" << j <<" ,trade:"<<trade[j] <<" ,current contracts:"<<contracts[i]<<+
            //  " ,new value:" <<value[i] << " ,size:" <<entrysize << " ,BrokerageCost:"<<brokeragecost[i]<<std::endl;
          }
          //Rcout<<"Exit"<<",Trade:"<<trade[j]<<",value:"<<value[a]<<",contracts:"<<contracts[a]<<",exitsize:"<<entrysize[entryindex[0]]<<endl;
        }
      }
    }
  }

  //mtm positions
  for(int a=0;a<nIndices;a++){
    int i=mdindices[a];
    profit[a]=value[a]+contracts[a]*close[i]-brokeragecost[a];//
    exposure[a]=abs(contracts[a])*close[i];
  }

  return DataFrame::create(_["date"]=subtimestamp,_["cashposition"]=value,_["contracts"]=contracts,_["profit"]=profit,_["close"]=subclose,_["brokerage"]=brokeragecost,_["exposure"]=exposure);

}


// [[Rcpp::export]]
DataFrame GenerateTrades(DataFrame all){
  int nSize=all.nrows();
  //if(Rf_isNull(all)){
  if(nSize==0){
    //Rcout<<"Zero size df:"<<endl;
    return DataFrame::create(_["symbol"]=CharacterVector::create(),_["trade"]=CharacterVector::create(),_["entrytime"]=NumericVector::create(),
                             _["entryprice"]=NumericVector::create(),_["exittime"]=NumericVector::create(),_["exitprice"]=NumericVector::create(),
                             _["percentprofit"]=NumericVector::create(),_["bars"]=NumericVector::create(), _["stringsAsFactors"] = false
    );

  }
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const NumericVector buyprice=all["buyprice"];
  const NumericVector sellprice=all["sellprice"];
  const NumericVector shortprice=all["shortprice"];
  const NumericVector coverprice=all["coverprice"];
  const CharacterVector symbol=all["symbol"];
  const DatetimeVector timestamp=all["date"];
  LogicalVector buyprocessed(nSize);
  LogicalVector sellprocessed(nSize);
  LogicalVector shortprocessed(nSize);
  LogicalVector coverprocessed(nSize);



  //calculate # of trades
  int tradecount=0;
  for(int i=0;i<nSize;i++){
    if(buy[i]>0){
      tradecount++;
    }
    if(shrt[i]>0){
      tradecount++;
    }
  }
  tradecount=tradecount;
  // Rcout<<"TradeCount:"<<tradecount<<endl;

  CharacterVector tradesymbol(tradecount);
  StringVector trade(tradecount);
  DatetimeVector entrytime(tradecount);
  NumericVector entryprice(tradecount);
  DatetimeVector exittime(tradecount);
  NumericVector exitprice(tradecount);
  NumericVector percentprofit(tradecount);
  NumericVector bars(tradecount);
  StringVector exitreason(tradecount);
  Function formatDate("format.POSIXct");
  int tradesize=-1;
  StringVector uniquesymbol=unique(symbol);
  for(int z=0;z<uniquesymbol.size();z++){
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    //  Rf_PrintValue(uniquesymbol[z]);
    //  Rf_PrintValue(indices);
    //for the specified uniquesymbol, indices contains the vector of index values to "all"
    for(int a=0;a<indices.size();a++){
      int i=indices[a]; //i is the scalar index into "all"
      int entrybar=0;
      if((buyprocessed[i]==false) && ((buy[i]>0) && (buy[i]<=999))){
        tradesize++;
        tradesymbol[tradesize]=symbol[i];
        if(buy[i]==1){
          trade[tradesize]="BUY";
        }else if(buy[i]==2){
          trade[tradesize]="ReplacementBUY";
        }else if(buy[i]==999){
          trade[tradesize]="ScaleInBUY";
        }
        entrytime[tradesize]=timestamp[i];
        entryprice[tradesize]=buyprice[i];
        entrybar=i;
        buyprocessed[i]=true;
        //    Rcout<<"Buy"<<",Symbol:"<<symbol[i]<<",TradeSize:"<<tradesize<<",SignalBar:"<<i<<endl;
        int b=a+1;
        //    Rcout<<"Starting Checking Sell"<<",Symbol:"<<symbol[i]<<",b:"<<b<<",indices.size:"<<indices.size()<<endl;
        if(b<indices.size()){
          while ((b<indices.size()) && (sell[indices[b]]==0)) {
            //        Rcout<<"Checking Sell"<<",Symbol:"<<symbol[i]<<",b:"<<b<<",indices.size:"<<indices.size()<<endl;
            sellprocessed[indices[b]]=true;
            b++;
          }
        }
        if(b<indices.size()){
          int k=indices[b];
          sellprocessed[k]=true;
          //      Rcout<<"Sell"<<",Symbol:"<<symbol[k]<<",TradeSize:"<<tradesize<<",SignalBar:"<<k<<endl;
          exittime[tradesize]=timestamp[k];
          exitprice[tradesize]=sellprice[k];
          if(sell[k]==1){
            exitreason[tradesize]="RegularExit";
          }else if(sell[k]==2){
            exitreason[tradesize]="SL";
          }else if (sell[k]==3){
            exitreason[tradesize]="GapSL";
          }else if (sell[k]==4){
            exitreason[tradesize]="TP";
          }else if (sell[k]==5){
            exitreason[tradesize]="GapTP";
          }else if (sell[k]==9){
            exitreason[tradesize]="Rollover";
          }else{
            exitreason[tradesize]="Undefined";
          }
          //Rcout <<"Sell"<<exitreason[tradesize]<<endl;
          //Rcout<<"1, exitbar: "<<k<<"entrybar: "<<entrybar<<std::endl;
          bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[entrybar].getFractionalTimestamp())/86400;
          percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
          //we have found a sell. Check for any scaleinlong
          if(a+1<b){
            for(int c=a+1;c<b;c++){
              int j=indices[c];
              int k=indices[b];
              if(buy[j]==999){
                tradesize++;
                //            Rcout<<"ScaleInLong"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
                tradesymbol[tradesize]=symbol[j];
                trade[tradesize]="ScaleInBUY";
                entrytime[tradesize]=timestamp[j];
                entryprice[tradesize]=buyprice[j];
                buyprocessed[j]=true;
                exittime[tradesize]=timestamp[k];
                exitprice[tradesize]=sellprice[k];
                if(sell[k]==1){
                  exitreason[tradesize]="RegularExit";
                }else if(sell[k]==2){
                  exitreason[tradesize]="SL";
                }else if (sell[k]==3){
                  exitreason[tradesize]="GapSL";
                }else if (sell[k]==4){
                  exitreason[tradesize]="TP";
                }else if (sell[k]==5){
                  exitreason[tradesize]="GapTP";
                }else if (sell[k]==9){
                  exitreason[tradesize]="Rollover";
                }else{
                  exitreason[tradesize]="Undefined";
                }
                //Rcout <<"Scale Sell"<<exitreason[tradesize]<<endl;
                //            Rcout<<"2, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
                bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
                percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
                sellprocessed[j]=true;
                //            Rcout<<"SellScaleInLong"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              }
            }
          }

        }else if(a+1<indices.size()){ //open trade
          //Rcout<<"Check for scaleins"<<",a+1:"<<a+1<<",indices.size:"<<indices.size()<<endl;
          for(int c=a+1;c<indices.size();c++){
            //Rcout<<"OpenLongTrade"<<",c:"<<c<<",indices.size:"<<indices.size()<<endl;
            //Rcout<<"ScaleInLongOpen"<<",j:"<<indices[c]<<endl;
            int j=indices[c];
            //Rcout<<"ScaleInLongOpen"<<",k:"<<indices[indices.size()-1]<<endl;
            int k=indices[indices.size()-1];
            if(buy[j]==999){
              tradesize++;
              //Rcout<<"ScaleInLongOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              tradesymbol[tradesize]=symbol[j];
              trade[tradesize]="ScaleInBUY";
              entrytime[tradesize]=timestamp[j];
              entryprice[tradesize]=buyprice[j];
              buyprocessed[j]=true;
              //              exittime[tradesize]=0;
              exitprice[tradesize]=sellprice[k];
              if(sell[k]==1){
                exitreason[tradesize]="RegularExit";
              }else if(sell[k]==2){
                exitreason[tradesize]="SL";
              }else if (sell[k]==3){
                exitreason[tradesize]="GapSL";
              }else if (sell[k]==4){
                exitreason[tradesize]="TP";
              }else if (sell[k]==5){
                exitreason[tradesize]="GapTP";
              }else if (sell[k]==9){
                exitreason[tradesize]="Rollover";
              }else{
                exitreason[tradesize]="Undefined";
              }
              //Rcout <<"Open Sell"<<exitreason[tradesize]<<endl;
              //Rcout<<"3, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
              bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
              percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
              sellprocessed[j]=true;
              //Rcout<<"SellScaleInLongOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
            }
          }
        }
      }else if((shortprocessed[i]==false) && ((shrt[i]>0) && (shrt[i]<=999))){
        tradesize++;
        tradesymbol[tradesize]=symbol[i];
        if(shrt[i]==1){
          trade[tradesize]="SHORT";
        }else if(shrt[i]==2){
          trade[tradesize]="ReplacementSHORT";
        }else if(shrt[i]==999){
          trade[tradesize]="ScaleInSHORT";
        }

        entrytime[tradesize]=timestamp[i];
        entryprice[tradesize]=shortprice[i];
        entrybar=i;
        shortprocessed[i]=true;
        //Rcout<<"Short"<<",Symbol:"<<symbol[i]<<",TradeSize:"<<tradesize<<",SignalBar:"<<i<<endl;
        int b=a+1;
        if(b<indices.size()){
          while ((b<indices.size()) && (cover[indices[b]]==0)) {
            coverprocessed[indices[b]]=true;
            b++;
          }
        }

        if(b<indices.size()){
          int k=indices[b];
          coverprocessed[k]=true;
          //Rcout<<"Cover"<<"Symbol:"<<symbol[k]<<",TradeSize:"<<tradesize<<",SignalBar:"<<k<<endl;
          exittime[tradesize]=timestamp[k];
          exitprice[tradesize]=coverprice[k];
          if(cover[k]==1){
            exitreason[tradesize]="RegularExit";
          }else if(cover[k]==2){
            exitreason[tradesize]="SL";
          }else if (cover[k]==3){
            exitreason[tradesize]="GapSL";
          }else if (cover[k]==4){
            exitreason[tradesize]="TP";
          }else if (cover[k]==5){
            exitreason[tradesize]="GapTP";
          }else if (cover[k]==9){
            exitreason[tradesize]="Rollover";
          }else{
            exitreason[tradesize]="Undefined";
          }
          //Rcout <<"Cover"<<exitreason[tradesize]<<endl;

          //Rcout<<"4, exitbar: "<<k<<"entrybar: "<<entrybar<<std::endl;
          bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[entrybar].getFractionalTimestamp())/86400;
          percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
          //we have found a cover. Check for any scaleinshort
          if(a+1<b){
            for(int c=a+1;c<b;c++){
              int j=indices[c];
              int k=indices[b];
              if(shrt[j]==999){
                tradesize++;
                //Rcout<<"ScaleInShort"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
                tradesymbol[tradesize]=symbol[j];
                trade[tradesize]="ScaleInSHORT";
                entrytime[tradesize]=timestamp[j];
                entryprice[tradesize]=shortprice[j];
                shortprocessed[j]=true;
                exittime[tradesize]=timestamp[k];
                exitprice[tradesize]=coverprice[k];
                if(cover[k]==1){
                  exitreason[tradesize]="RegularExit";
                }else if(cover[k]==2){
                  exitreason[tradesize]="SL";
                }else if (cover[k]==3){
                  exitreason[tradesize]="GapSL";
                }else if (cover[k]==4){
                  exitreason[tradesize]="TP";
                }else if (cover[k]==5){
                  exitreason[tradesize]="GapTP";
                }else if (cover[k]==9){
                  exitreason[tradesize]="Rollover";
                }else{
                  exitreason[tradesize]="Undefined";
                }
                //Rcout <<"Scale Cover"<<exitreason[tradesize]<<endl;
                //Rcout<<"5, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
                bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
                percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
                coverprocessed[j]=true;
                //Rcout<<"ScaleInCover"<<",Symbol:"<<symbol[c]<<"TradeSize:"<<tradesize<<",SignalBar:"<<c<<endl;
              }
            }
          }

        }else if(a+1<indices.size()){ //open trade
          for(int c=a+1;c<indices.size();c++){
            int j=indices[c];
            int k=indices[indices.size()-1];
            if(shrt[j]==999){
              tradesize++;
              //              Rcout<<"ScaleInShortOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              tradesymbol[tradesize]=symbol[j];
              trade[tradesize]="ScaleInSHORT";
              entrytime[tradesize]=timestamp[j];
              entryprice[tradesize]=shortprice[j];
              shortprocessed[j]=true;
              //            exittime[tradesize]=0;
              exitprice[tradesize]=coverprice[k];

              if(cover[k]==1){
                exitreason[tradesize]="RegularExit";
              }else if(cover[k]==2){
                exitreason[tradesize]="SL";
              }else if (cover[k]==3){
                exitreason[tradesize]="GapSL";
              }else if (cover[k]==4){
                exitreason[tradesize]="TP";
              }else if (cover[k]==5){
                exitreason[tradesize]="GapTP";
              }else if (cover[k]==9){
                exitreason[tradesize]="Rollover";
              }else{
                exitreason[tradesize]="Undefined";
              }
              //Rcout <<"Open Short"<<exitreason[tradesize]<<endl;
              // Rcout<<"6, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
              bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
              percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
              coverprocessed[j]=true;
              //Rcout<<"ScaleInCover"<<",Symbol:"<<symbol[c]<<"TradeSize:"<<tradesize<<",SignalBar:"<<c<<endl;
            }
          }
        }
      }
    }
  }
  //Rf_PrintValue(exitreason);
  return DataFrame::create(_["symbol"]=tradesymbol,_["trade"]=trade,_["entrytime"]=entrytime,
                           _["entryprice"]=entryprice,_["exittime"]=exittime,_["exitprice"]=exitprice,_["exitreason"]=exitreason,
                           _["percentprofit"]=percentprofit,_["bars"]=bars,_["stringsAsFactors"] = false
  );
}

// [[Rcpp::export]]
DataFrame GenerateTradesShort(DataFrame all){
  int nSize=all.nrows();
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const NumericVector buyprice=all["buyprice"];
  const NumericVector sellprice=all["sellprice"];
  const NumericVector shortprice=all["shortprice"];
  const NumericVector coverprice=all["coverprice"];
  const CharacterVector symbol=all["symbol"];
  const DatetimeVector timestamp=all["date"];
  LogicalVector buyprocessed(nSize);
  LogicalVector sellprocessed(nSize);
  LogicalVector shortprocessed(nSize);
  LogicalVector coverprocessed(nSize);

  //calculate # of trades
  int tradecount=0;
  for(int i=0;i<nSize;i++){
    if(buy[i]>0){
      tradecount++;
    }
    if(shrt[i]>0){
      tradecount++;
    }
  }
  tradecount=tradecount;
  //Rcout<<"TradeCount:"<<tradecount<<endl;

  CharacterVector tradesymbol(tradecount);
  StringVector trade(tradecount);
  DatetimeVector entrytime(tradecount);
  NumericVector entryprice(tradecount);
  DatetimeVector exittime(tradecount);
  NumericVector exitprice(tradecount);
  NumericVector percentprofit(tradecount);
  NumericVector bars(tradecount);
  Function formatDate("format.POSIXct");
  int tradesize=-1;
  StringVector uniquesymbol=unique(symbol);
  for(int z=0;z<uniquesymbol.size();z++){
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    //Rf_PrintValue(uniquesymbol[z]);
    //Rf_PrintValue(indices);
    for(int a=0;a<indices.size();a++){
      int i=indices[a];
      int entrybar=0;
      if((buyprocessed[i]==false) && ((buy[i]>0) && (buy[i]<999))){
        tradesize++;
        tradesymbol[tradesize]=symbol[i];
        if(buy[i]==1){
          trade[tradesize]="BUY";
        }else if(buy[i]==2){
          trade[tradesize]="ReplacementBUY";
        }
        entrytime[tradesize]=timestamp[i];
        entryprice[tradesize]=buyprice[i];
        entrybar=i;
        buyprocessed[i]=true;
        //Rcout<<"Buy"<<",Symbol:"<<symbol[i]<<",TradeSize:"<<tradesize<<",SignalBar:"<<i<<endl;
        int b=a+1;
        //Rcout<<"Starting Checking Sell"<<",Symbol:"<<symbol[i]<<",b:"<<b<<",indices.size:"<<indices.size()<<endl;
        if(b<indices.size()){
          while ((b<indices.size()) && (sell[indices[b]]==0)) {
            //Rcout<<"Checking Sell"<<",Symbol:"<<symbol[i]<<",b:"<<b<<",indices.size:"<<indices.size()<<endl;
            sellprocessed[indices[b]]=true;
            b++;
          }
        }
        if(b<indices.size()){
          int k=indices[b];
          sellprocessed[k]=true;
          //Rcout<<"Sell"<<",Symbol:"<<symbol[k]<<",TradeSize:"<<tradesize<<",SignalBar:"<<k<<endl;
          exittime[tradesize]=timestamp[k];
          exitprice[tradesize]=sellprice[k];
          //bars[tradesize]=k-entrybar;
          //Rcout<<"7, exitbar: "<<k<<"entrybar: "<<entrybar<<std::endl;
          bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[entrybar].getFractionalTimestamp())/86400;
          percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
          //we have found a sell. Check for any scaleinlong
          if(a+1<b){
            for(int c=a+1;c<b;c++){
              int j=indices[c];
              int k=indices[b];
              if(buy[j]==999){
                tradesize++;
                //Rcout<<"ScaleInLong"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
                tradesymbol[tradesize]=symbol[j];
                trade[tradesize]="ScaleInBUY";
                entrytime[tradesize]=timestamp[j];
                entryprice[tradesize]=buyprice[j];
                buyprocessed[j]=true;
                exittime[tradesize]=timestamp[k];
                exitprice[tradesize]=sellprice[k];
                //bars[tradesize]=k-j;
                //Rcout<<"8, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
                bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
                percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
                sellprocessed[j]=true;
                //Rcout<<"SellScaleInLong"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              }
            }
          }

        }else if(a+1<indices.size()){ //open trade
          //Rcout<<"Check for scaleins"<<",a+1:"<<a+1<<",indices.size:"<<indices.size()<<endl;
          for(int c=a+1;c<indices.size();c++){
            //Rcout<<"OpenLongTrade"<<",c:"<<c<<",indices.size:"<<indices.size()<<endl;
            //Rcout<<"ScaleInLongOpen"<<",j:"<<indices[c]<<endl;
            int j=indices[c];
            //Rcout<<"ScaleInLongOpen"<<",k:"<<indices[indices.size()-1]<<endl;
            int k=indices[indices.size()-1];
            if(buy[j]==999){
              tradesize++;
              //Rcout<<"ScaleInLongOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              tradesymbol[tradesize]=symbol[j];
              trade[tradesize]="ScaleInBUY";
              entrytime[tradesize]=timestamp[j];
              entryprice[tradesize]=buyprice[j];
              buyprocessed[j]=true;
              //              exittime[tradesize]=0;
              exitprice[tradesize]=sellprice[k];
              //bars[tradesize]=k-j;
              //Rcout<<"9, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
              bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
              percentprofit[tradesize]=(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
              sellprocessed[j]=true;
              //Rcout<<"SellScaleInLongOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
            }
          }
        }
      }else if((shortprocessed[i]==false) && ((shrt[i]>0) && (shrt[i]<999))){
        tradesize++;
        tradesymbol[tradesize]=symbol[i];
        if(shrt[i]==1){
          trade[tradesize]="SHORT";
        }else if(shrt[i]==2){
          trade[tradesize]="ReplacementSHORT";
        }
        entrytime[tradesize]=timestamp[i];
        entryprice[tradesize]=shortprice[i];
        entrybar=i;
        shortprocessed[i]=true;
        //Rcout<<"Short"<<",Symbol:"<<symbol[i]<<",TradeSize:"<<tradesize<<",SignalBar:"<<i<<endl;
        int b=a+1;
        if(b<indices.size()){
          while ((b<indices.size()) && (cover[indices[b]]==0)) {
            coverprocessed[indices[b]]=true;
            b++;
          }
        }

        if(b<indices.size()){
          int k=indices[b];
          coverprocessed[k]=true;
          //Rcout<<"Cover"<<"Symbol:"<<symbol[k]<<",TradeSize:"<<tradesize<<",SignalBar:"<<k<<endl;
          exittime[tradesize]=timestamp[k];
          exitprice[tradesize]=coverprice[k];
          //bars[tradesize]=k-entrybar;
          //Rcout<<"10, exitbar: "<<k<<"entrybar: "<<entrybar<<std::endl;
          bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[entrybar].getFractionalTimestamp())/86400;
          percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
          //we have found a cover. Check for any scaleinshort
          if(a+1<b){
            for(int c=a+1;c<b;c++){
              int j=indices[c];
              int k=indices[b];
              if(shrt[j]==999){
                tradesize++;
                //Rcout<<"ScaleInShort"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
                tradesymbol[tradesize]=symbol[j];
                trade[tradesize]="ScaleInSHORT";
                entrytime[tradesize]=timestamp[j];
                entryprice[tradesize]=buyprice[j];
                shortprocessed[j]=true;
                exittime[tradesize]=timestamp[k];
                exitprice[tradesize]=sellprice[k];
                //bars[tradesize]=k-j;
                //Rcout<<"11, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
                bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
                percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
                coverprocessed[j]=true;
                //Rcout<<"ScaleInCover"<<",Symbol:"<<symbol[c]<<"TradeSize:"<<tradesize<<",SignalBar:"<<c<<endl;
              }
            }
          }

        }else if(a+1<indices.size()){ //open trade
          for(int c=a+1;c<indices.size();c++){
            int j=indices[c];
            int k=indices[indices.size()-1];
            if(shrt[j]==999){
              tradesize++;
              //              Rcout<<"ScaleInShortOpen"<<",Symbol:"<<symbol[j]<<",TradeSize:"<<tradesize<<",SignalBar:"<<j<<endl;
              tradesymbol[tradesize]=symbol[j];
              trade[tradesize]="ScaleInSHORT";
              entrytime[tradesize]=timestamp[j];
              entryprice[tradesize]=buyprice[j];
              shortprocessed[j]=true;
              //            exittime[tradesize]=0;
              exitprice[tradesize]=sellprice[k];
              //bars[tradesize]=k-j;
              //Rcout<<"12, exitbar: "<<k<<"entrybar: "<<j<<std::endl;
              bars[tradesize]=(timestamp[k].getFractionalTimestamp()-timestamp[j].getFractionalTimestamp())/86400;
              percentprofit[tradesize]=-(exitprice[tradesize]-entryprice[tradesize])/entryprice[tradesize];
              coverprocessed[j]=true;
              //Rcout<<"ScaleInCover"<<",Symbol:"<<symbol[c]<<"TradeSize:"<<tradesize<<",SignalBar:"<<c<<endl;
            }
          }
        }
      }
    }
  }
  return DataFrame::create(_["symbol"]=tradesymbol,_["trade"]=trade,_["entrytime"]=entrytime,
                           _["entryprice"]=entryprice,_["exittime"]=exittime,_["exitprice"]=exitprice,
                           _["percentprofit"]=percentprofit,_["bars"]=bars, _["stringsAsFactors"] = false
  );
}

// [[Rcpp::export]]
NumericVector ContinuingLong(StringVector symbol,NumericVector buy,NumericVector sell,NumericVector shrt){
  int nSize=symbol.length();
  NumericVector inlongtrade(nSize);
  StringVector uniquesymbol=unique(symbol);
  for(int z=0;z<uniquesymbol.size();z++){//for each uniquesymbol
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    for(int a=0;a<indices.size();a++){//iterate through rows. We need to ensure that data is sorted in ascending order
      int i=indices[a];
      if(buy[i]>0){
        inlongtrade[i]=1;
      }
      else if(a>0){
        int priorindex=indices[a-1];
        if(inlongtrade[priorindex]>0 & (sell[i]>0|shrt[i]>0)){
          inlongtrade[i]=0;
        }else{
          int priorvalue=inlongtrade[priorindex];
          inlongtrade[i]=priorvalue;
        }
      }
    }
  }
  return inlongtrade;
}

// [[Rcpp::export]]
NumericVector ContinuingShort(StringVector symbol,NumericVector shrt,NumericVector cover,NumericVector buy){
  int nSize=symbol.length();
  NumericVector inshorttrade(nSize);
  StringVector uniquesymbol=unique(symbol);
  for(int z=0;z<uniquesymbol.size();z++){//for each uniquesymbol
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    for(int a=0;a<indices.size();a++){//iterate through rows. We need to ensure that data is sorted in ascending order
      int i=indices[a];
      if(shrt[i]>0){
        inshorttrade[i]=1;
      }
      else if(a>0){
        int priorindex=indices[a-1];
        if(inshorttrade[priorindex]>0 & (cover[i]>0|buy[i]>0)){
          inshorttrade[i]=0;
        }else{
          int priorvalue=inshorttrade[priorindex];
          inshorttrade[i]=priorvalue;
        }
      }
    }
  }
  return inshorttrade;
}


// [[Rcpp::export]]
DataFrame ProcessPositionScore(DataFrame all,unsigned int maxposition,DatetimeVector dates,bool debug=false){
  if(debug){
    Rcout<<"### Running ProcessPositionScore ###"<<std::endl;
  }
  int nSize=all.nrows();
  const DatetimeVector timestamp=all["date"];
  const NumericVector positionscore=all["positionscore"];
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const StringVector symbol=all["symbol"];
  NumericVector lbuy(nSize);
  NumericVector lsell(nSize);
  NumericVector lshrt(nSize);
  NumericVector lcover(nSize);
  NumericVector inlongtrade(nSize);
  NumericVector inshorttrade(nSize);
  inlongtrade=ContinuingLong(symbol,buy,sell,shrt);
  inshorttrade=ContinuingShort(symbol,shrt,cover,buy);

  DatetimeVector uniqueDates=dates;
  vector<String> positionNames;

  map<double,int>daypositionscore;//holds the position score and index of dataframe

  for(int i=0;i<uniqueDates.size();i++){ //for each date
    daypositionscore.clear();
    IntegerVector indices=whichDate2(timestamp,uniqueDates[i]);
    //Rcout<<"Date Bar:"<<timestamp[i]<<std::endl;
    //Execute exits
    for(int a=1;a<indices.size();a++){ // loop through  all symbols with data for the date
      int j=indices[a];
      int priorj=indices[(a-1)];
      // if(symbol[j]=="HINDALCO"){
      //   Rcout<<"Processing Exit. Date:"<<uniqueDates[i]<<",Symbol:"<<symbol[j]<<",j:"<<j<<",sell:"<<sell[j]<<",cover:"<<cover[j]<<std::endl;
      // }
      if(sell[j]>0 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        //we have a matching sell
        lsell[j]=sell[j];
        positionNames.erase(std::remove(positionNames.begin(),positionNames.end(),symbol[j]),positionNames.end());
        if(debug){
          Rcout<<"Date: "<<uniqueDates[i]<<", Sell Symbol: "<<symbol[j]<<", New PositionCount:"<<positionNames.size()<<endl;
        }
      }
      if(cover[j]>0 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        //we have a matching cover
        lcover[j]=cover[j];
        positionNames.erase(std::remove(positionNames.begin(),positionNames.end(),symbol[j]),positionNames.end());
        if(debug){
          Rcout<<"Date: "<<uniqueDates[i]<<", Cover Symbol: "<<symbol[j]<<", New PositionCount:"<<positionNames.size()<<endl;
        }
      }
    }

    //Execute entry
    //1. If there is capacity, Update positionscore
    for(int a=1;a<indices.size();a++){
      //Rf_PrintValue(indices);
      int j=indices[a];
      if((buy[j]>0 && positionscore[j]>0) || (shrt[j]>0 && positionscore[j]>0)){
        if ( daypositionscore.find(positionscore[j]) == daypositionscore.end() ) {
          daypositionscore.insert(std::pair<double,int>(positionscore[j],j));
        }else{
          //add a positionscore that is marginally higher
          daypositionscore.insert(std::pair<double,int>(positionscore[j]+0.00001,j));
        }
      }
    }
    //insert upto limit in positionNames
    int itemsToInsert=0;
    //      Rf_PrintValue(temp);

    if(positionNames.size()<maxposition){
      itemsToInsert=maxposition-positionNames.size();
    }
    for (std::map<double,int>::reverse_iterator rit=daypositionscore.rbegin(); rit!=daypositionscore.rend
      (); ++rit){

      if(itemsToInsert>0){
        int j=rit->second;
        if(buy[j]>0){
          lbuy[j]=buy[j];
        }else{
          lshrt[j]=shrt[j];
        }
        positionNames.push_back(symbol[j]);
        if(debug){
          Rcout<< "Date: "<<timestamp[j]<<", Symbol added to position: "<< symbol[j]<<", PositionScore: "<<rit->first <<std::endl;
        }

        //    Rcout<<"Processing Entry. Signal Bar:"<<j<<",Date:"<<uniqueDates[i]<<",Symbol:"<<symbol[j]<<",lbuy:"<<lbuy[j]<<",PositionSize:"<<positionNames.size()<<std::endl;
        // for (std::vector<String>::const_iterator i = positionNames.begin(); i != positionNames.end(); ++i)
        //   Rcout << "Test" << ' '<<std::endl;
        itemsToInsert--;
      }
    }
    if(debug){
      Rcout <<"Date: "<<uniqueDates[i]<<", Closing Positions: "<<positionNames.size()<<", Max Positions:"<< maxposition<<std::endl;
      for (std::vector<Rcpp::String>::iterator rit=positionNames.begin(); rit!=positionNames.end(); ++rit){
        Rcout<< rit->get_cstring() <<std::endl;
      }
    }

    //update scale-ins
    for(int a=0;a<indices.size();a++){
      //Rf_PrintValue(indices);
      int j=indices[a];
      //Rcout<<"Scale-In Index j:"<<j<<"buy[j]:"<<buy[j]<<"symbol[j]:"<<symbol[j]<<std::endl;
      if(buy[j]==999 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        //Rcout<<"Processing Scale In. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
        lbuy[j]=999; //we have a scale-in for an existing position
      }
      if(shrt[j]==999 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        lshrt[j]=999; //we have a scale-in for an existing position
        //Rcout<<"Processing Scale In. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
      }
    }
  }

  inlongtrade=ContinuingLong(symbol,lbuy,lsell,lshrt);
  inshorttrade=ContinuingShort(symbol,lshrt,lcover,lbuy);

  return DataFrame::create(_["date"]=timestamp,_["aopen"]=all["aopen"],_["ahigh"]=all["ahigh"],
                           _["alow"]=all["alow"],_["asettle"]=all["asettle"],_["aclose"]=all["aclose"],
                             _["buy"]=lbuy,_["sell"]=lsell,_["short"]=lshrt,_["cover"]=lcover,
                               _["buyprice"]=all["buyprice"],_["sellprice"]=all["sellprice"],
                                                                               _["shortprice"]=all["shortprice"],_["coverprice"]=all["coverprice"],
                                                                                                                                    _["inlongtrade"]=inlongtrade,_["inshorttrade"]=inshorttrade,
                                                                                                                                    _["symbol"]=symbol,
                                                                                                                                    _["stringsAsFactors"] = false);
}

// [[Rcpp::export]]
DataFrame ProcessPositionScoreShort(DataFrame all,unsigned int maxposition,DatetimeVector dates){
  int nSize=all.nrows();
  const DatetimeVector timestamp=all["date"];
  const NumericVector positionscore=all["positionscore"];
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const StringVector symbol=all["symbol"];
  NumericVector inlongtrade=all["inlongtrade"];
  NumericVector inshorttrade=all["inshorttrade"];
  NumericVector lbuy(nSize);
  NumericVector lsell(nSize);
  NumericVector lshrt(nSize);
  NumericVector lcover(nSize);


  //  int uniquedatesize=dates.size();
  DatetimeVector uniqueDates=dates;
  vector<String> positionNames;
  map<double,int>daypositionscore;//holds the position score and index of dataframe
  //for(int i=0;i<uniqueDates.size();i++){
  for(int i=0;i<uniqueDates.size();i++){
    daypositionscore.clear();
    IntegerVector indices=whichDate2(timestamp,uniqueDates[i]);
    //Rcout<<"Date Bar:"<<i<<std::endl;
    //Execute exits
    for(int a=0;a<indices.size();a++){
      int j=indices[a];
      //    Rcout<<"Processing Exit. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",sell:"<<sell[j]<<std::endl;
      if(cover[j]==1 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        //we have a matching sell
        lcover[j]=1;
        positionNames.erase(std::remove(positionNames.begin(),positionNames.end(),symbol[j]),positionNames.end());
        //Rcout<<"Removed Sell. New PositionSize:"<<positionNames.size()<<endl;
      }
    }

    //Execute entry
    //1. If there is capacity, Update positionscore
    if(positionNames.size()<maxposition){
      for(int a=0;a<indices.size();a++){
        //      Rf_PrintValue(indices);
        int j=indices[a];
        //      Rcout<<"Processing PositionScore. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
        if(shrt[j]==1 && positionscore[j]>0){
          if ( daypositionscore.find(positionscore[j]) == daypositionscore.end() ) {
            //  Rcout<<"Add PositionScore. Unique Positionscore."<< " Symbol:"<<symbol[j]<<" ,PositionScore:"<<positionscore[j]<<endl;
            daypositionscore.insert(std::pair<double,int>(positionscore[j],j));
          }else{
            //add a positionscore that is marginally higher
            //Rcout<<"Add PositionScore. Duplicate Positionscore."<< " Symbol:"<<symbol[j]<<" ,PositionScore:"<<positionscore[j]<<endl;
            daypositionscore.insert(std::pair<double,int>(positionscore[j]+0.00001,j));
          }
        }
      }
    }
    //insert upto limit in positionNames
    int itemsToInsert=0;
    if(positionNames.size()<maxposition){
      itemsToInsert=maxposition-positionNames.size();
    }
    for (std::map<double,int>::reverse_iterator rit=daypositionscore.rbegin(); rit!=daypositionscore.rend
      (); ++rit){

      if(itemsToInsert>0){
        int j=rit->second;
        lshrt[j]=1;
        positionNames.push_back(symbol[j]);
        //Rcout<<"Processing Entry. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",lbuy:"<<lbuy[j]<<",PositionSize:"<<positionNames.size()<<std::endl;
        itemsToInsert--;
      }
    }
    //update scale-ins
    for(int a=0;a<indices.size();a++){
      int j=indices[a];
      if(shrt[j]==999 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        lshrt[j]=999; //we have a scale-in for an existing position
        //Rcout<<"Processing Scale In. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
      }
    }
  }

  // NumericVector inlongtrade=Flip(lbuy,lsell);
  //  NumericVector inshorttrade=Flip(lshrt,lcover);

  return DataFrame::create(_["date"]=timestamp,_["aopen"]=all["aopen"],_["ahigh"]=all["ahigh"],
                           _["alow"]=all["alow"],_["aclose"]=all["aclose"],_["asettle"]=all["asettle"],
                                                                                           _["buy"]=lbuy,_["sell"]=lsell,_["short"]=lshrt,_["cover"]=lcover,
                                                                                             _["buyprice"]=all["buyprice"],_["sellprice"]=all["sellprice"],
                                                                                                                                             _["shortprice"]=all["shortprice"],_["coverprice"]=all["coverprice"],
                                                                                                                                                                                                  _["inlongtrade"]=inlongtrade,_["inshorttrade"]=inshorttrade,
                                                                                                                                                                                                  _["symbol"]=symbol,
                                                                                                                                                                                                  _["stringsAsFactors"] = false);
}


// [[Rcpp::export]]
DataFrame ApplyStop(const DataFrame all,NumericVector amount,bool volatilesl=false){
  //stop mode can be 1: points
  int nSize=all.nrows();
  const NumericVector inlongtrade=all["inlongtrade"];
  const NumericVector inshorttrade=all["inshorttrade"];
  const NumericVector open=all["aopen"];
  const NumericVector high=all["ahigh"];
  const NumericVector low=all["alow"];
  const NumericVector close=all["aclose"];
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const StringVector symbol=all["symbol"];

  const NumericVector buyprice=all["buyprice"];
  const NumericVector sellprice=all["sellprice"];
  const NumericVector shortprice=all["shortprice"];
  const NumericVector coverprice=all["coverprice"];
  const DatetimeVector timestamp=all["date"];

  NumericVector lbuy(nSize);
  NumericVector lsell(nSize);
  NumericVector lshrt(nSize);
  NumericVector lcover(nSize);

  NumericVector lbuyprice(nSize);
  NumericVector lsellprice(nSize);
  NumericVector lshortprice(nSize);
  NumericVector lcoverprice(nSize);

  const NumericVector damount=amount;

  for(int i=0;i<nSize;i++){
    if(buy[i]>0){
      lbuy[i]=buy[i];
      lbuyprice[i]=buyprice[i];
    }
    if(shrt[i]>0){
      lshrt[i]=shrt[i];
      lshortprice[i]=shortprice[i];
    }
    if(sell[i]>0){
      lsell[i]=sell[i];
      lsellprice[i]=sellprice[i];
    }
    if(cover[i]>0){
      lcover[i]=cover[i];
      lcoverprice[i]=coverprice[i];
    }
  }
  StringVector uniquesymbol=unique(symbol);
  for(int z=0;z<uniquesymbol.size();z++){
    bool stoplosstriggered =false;
    int barstart=0;
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    for(int a=0;a<indices.size();a++){
      int i=indices[a];
      stoplosstriggered=false;
      bool newtrade=(lbuy[i-1]>0)|(lshrt[i-1]>0);
      //Rcout << "The value NewTrade at i: " << i <<" is "<< newtrade << ", lbuy[i-1]: "<<lbuy[i-1] <<" ,shrt[i-1]"<<shrt[i-1] <<std::endl;
      if(newtrade){ //reset stoplosstriggered flag.
        stoplosstriggered=false;
        barstart=i-1;
      }

      if(!stoplosstriggered){//stoplossnottriggered
        if(inlongtrade[i-1]==1){
          //check if stoploss triggered for a long trade
          double slprice=0;
          if(volatilesl){
            slprice=buyprice[barstart]-damount[i-1];
          }else{
            slprice=buyprice[barstart]-damount[barstart];
          }
          //Rcout << "The value sl at i: " << i <<" is "<< slprice << ", barstart: "<<barstart <<" ,ref buyprice:"<<buyprice[barstart]<<" ,loss amt: "<<damount[barstart] <<std::endl;
          if(open[i]<=slprice){
            lsellprice[i]=open[i];
            stoplosstriggered=true;
            lsell[i]=3;
          }else if ((low[i]<=slprice) && (high[i]>=slprice)){
            lsellprice[i]=slprice;
            stoplosstriggered=true;
            lsell[i]=2;//maxsl
            //Rcout << "SL triggered at i: " << i <<" Trigger price is"<< slprice <<std::endl;
          }
        }else if(inshorttrade[i-1]==1){
          //check if stoploss triggered for a short trade
          double slprice=0;
          if(volatilesl){
            slprice=shortprice[barstart]+damount[i-1];
          }else{
            slprice=shortprice[barstart]+damount[barstart];
          }
          if(open[i]>=slprice){
            lcoverprice[i]=open[i];
            stoplosstriggered=true;
            lcover[i]=3;
          }else if ((low[i]<=slprice) && (high[i]>=slprice)){
            lcoverprice[i]=slprice;
            stoplosstriggered=true;
            lcover[i]=2;
            //Rcout << "SL triggered at i: " << i <<" Trigger price is"<< slprice <<std::endl;
          }
        }
      }

      if(stoplosstriggered){//check if a replacement trade is needed
        //needed if intrade is true at the bar
        if(inlongtrade[i]==1 && cover[i]==0 ){
          //enter fresh long trade. Update all buyprices for bars ahead, till you arrive at a non-zero buyprice.
          int j=i;
          lbuy[i]=2;//replacement trade
          stoplosstriggered=false;
          while((inlongtrade[j]!=0) && (j<nSize)) {
            lbuyprice[j]=close[i];
            j++;
          };

        }else if((inshorttrade[i]==1) && (sell[i]==0)){
          //enter fresh short trade
          int j=i;
          stoplosstriggered=false;
          lshrt[i]=2;//replacement trade
          while((inshorttrade[j]!=0) && (j<nSize)){
            lshortprice[j]=close[i];
            j++;
          };
        }
      }
    }
  }

  return DataFrame::create(_["date"]=timestamp,_["symbol"]=symbol,_["buy"]=lbuy,_["sell"]=lsell,_["short"]=lshrt,_["cover"]=lcover,
                           _["buyprice"]= lbuyprice, _["sellprice"]= lsellprice,_["shortprice"]=lshortprice,_["coverprice"]=lcoverprice,
                             _["stringsAsFactors"] = false);

}

int getPriorIndex(StringVector symbols,int currentIndex){
  String refsymbol=symbols[currentIndex];
  IntegerVector indices=whichString2(symbols,refsymbol);
 // Rf_PrintValue(indices);
  int* it;
  it=std::find(indices.begin(),indices.end(),currentIndex);
  int index=std::distance(indices.begin(), it);
  return indices[index-1];
}

//[[Rcpp::export]]
DataFrame ProcessSignals(const DataFrame all,NumericVector slamount,NumericVector tpamount,unsigned int maxposition,bool volatilesl=false,bool volatiletp=false,bool debug=false ){
  if(debug){
    Rcout<<"### Running ProcessSignals ###"<<std::endl;
  }
  int nSize=all.nrows();
  NumericVector inlongtrade=all["inlongtrade"];
  NumericVector inshorttrade=all["inshorttrade"];
  const NumericVector open=all["aopen"];
  const NumericVector high=all["ahigh"];
  const NumericVector low=all["alow"];
  const NumericVector close=all["aclose"];
  NumericVector buy=all["buy"];
  NumericVector sell=all["sell"];
  NumericVector shrt=all["short"];
  NumericVector cover=all["cover"];
  const StringVector symbol=all["symbol"];
  const NumericVector positionscore=all["positionscore"];

  const NumericVector buyprice=all["buyprice"];
  const NumericVector sellprice=all["sellprice"];
  const NumericVector shortprice=all["shortprice"];
  const NumericVector coverprice=all["coverprice"];
  const DatetimeVector timestamp=all["date"];


  NumericVector lbuy(nSize);
  NumericVector lsell(nSize);
  NumericVector lshrt(nSize);
  NumericVector lcover(nSize);

  NumericVector lbuyprice(nSize);
  NumericVector lsellprice(nSize);
  NumericVector lshortprice(nSize);
  NumericVector lcoverprice(nSize);

  NumericVector sllevel(nSize);
  NumericVector tplevel(nSize);

  StringVector uniquesymbol=unique(symbol);
  DatetimeVector uniquedate=UniqueDateTime(timestamp);
  //Rf_PrintValue(uniquedate);
  vector<String> positionNames;
  vector<int> positionBarNumber;
  vector<String> side;
  map<double,int>daypositionscore;//holds the position score and index of dataframe
  for(int i=1;i<uniquedate.size();i++){//for each date
    IntegerVector indices=whichDate2(timestamp,uniquedate[i]); //indices contains index of uniquedate[i] in "all"
    //Execute exits
    // Rf_PrintValue(indices);
    for(int a=0;a<indices.size();a++){ // loop through  all symbols for the date
      int j=indices[a]; //index in 'all'
      int priorj=getPriorIndex(symbol,j);
   //   Rcout<<"j: "<<j<<", priorj: "<<priorj<<std::endl;
      // ### check it stops are triggered. Update signals
//      vector<String>::iterator it =std::find(positionNames.begin(),positionNames.end(),symbol[j]);
      vector<String>::iterator it =positionNames.begin();
      while ((it = std::find(it, positionNames.end(), symbol[j])) != positionNames.end()){
      //if(it!=positionNames.end()){ //if position exists, update sl,tp and sell/cover
        int index=std::distance(positionNames.begin(), it);
        int barstart=positionBarNumber.at(index);
        // update sl.level and tp.level
        if(inlongtrade[priorj]>0){
          if(volatiletp){
            tplevel[j]=tpamount[priorj];
          }else {
            tplevel[j]=tpamount[barstart];
          }
          if(volatilesl){
            sllevel[j]=slamount[priorj];
          }else {
            sllevel[j]=slamount[barstart];
          }
          if(debug){
            Rcout<<"Date: "<< timestamp[j]<<", Symbol:"<<symbol[j]<< ", Long TP Level: "<<tplevel[j]<<std::endl;
            Rcout<<"Date: "<< timestamp[j]<<", Symbol:"<<symbol[j]<< ", Long SL Level: "<<sllevel[j]<<std::endl;
          }
        }

        if(inshorttrade[priorj]>0){
          if(volatiletp){
            tplevel[j]=tpamount[priorj];
          }else {
            tplevel[j]=tpamount[barstart];
          }
          if(volatilesl){
            sllevel[j]=slamount[priorj];
          }else{
            sllevel[j]=slamount[barstart];
          }
          if(debug){
            Rcout<<"Date: "<< timestamp[j]<<", Symbol:"<<symbol[j]<< ", Short TP Level: "<<tplevel[j]<<std::endl;
            Rcout<<"Date: "<< timestamp[j]<<", Symbol:"<<symbol[j]<< ", Short SL Level: "<<sllevel[j]<<std::endl;
          }
        }
        //after updating sl and tp, check if sl or tp is triggered
        bool sltriggered=false;
        bool tptriggered=false;
        if(inlongtrade[priorj]==1 && buy[j]==0){ //dont trigger sl or tp in the entry bar
          //check if stoploss triggered for a long trade
          if(open[j]<=sllevel[j]){
            lsellprice[j]=open[j];
            sltriggered=true;
            lsell[j]=3;
            sell[j]=3;
            inlongtrade[j]=0;
            if(debug){
              Rcout<<"Date: "<<timestamp[j]<<", Gap SL Triggered for Long Symbol "<<symbol[j]<<", SellPrice: "<<lsellprice[j]<<std::endl;
            }
          }else if ((low[j]<=sllevel[j]) && (high[j]>=sllevel[j])){
            lsellprice[j]=sllevel[j];
            sltriggered=true;
            lsell[j]=2;//maxsl
            sell[j]=2;
            inlongtrade[j]=0;
            if(debug){
              Rcout<<"Date: "<<timestamp[j]<<", Max SL Triggered for Long Symbol "<<symbol[j]<<", SellPrice: "<<lsellprice[j]<<std::endl;
            }
          }

          if(!sltriggered){
            if(open[j]>=tplevel[j]){
              lsellprice[j]=open[j];
              tptriggered=true;
              lsell[j]=5; //gap tp
              sell[j]=5;
              inlongtrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Gap TP Triggered for Long Symbol "<<symbol[j]<<", SellPrice: "<<lsellprice[j]<<std::endl;
              }

            }else if ((low[j]<=tplevel[j]) && (high[j]>=tplevel[j])){
              lsellprice[j]=tplevel[j];
              tptriggered=true;
              lsell[j]=4;//maxtp
              sell[j]=4;
              inlongtrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Max TP Triggered for Long Symbol "<<symbol[j]<<", SellPrice: "<<lsellprice[j]<<std::endl;
              }
            }
            if (sell[j]==1 && !sltriggered && !tptriggered){
              lsell[j]=1;
              lsellprice[j]=sellprice[j];
              inlongtrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Regular Exit Triggered for Long Symbol "<<symbol[j]<<", SellPrice: "<<lsellprice[j]<<std::endl;
              }
            }
            if(sell[j]==0){
              lsell[j]=0;
              inlongtrade[j]=1;
            }
          }
        }else if(inshorttrade[priorj]==1 && shrt[j]==0){
          //check if stoploss triggered for a short trade
          if(open[j]>=sllevel[j]){
            lcoverprice[j]=open[j];
            sltriggered=true;
            lcover[j]=3;
            cover[j]=3;
            inshorttrade[j]=0;
            if(debug){
              Rcout<<"Date: "<<timestamp[j]<<", Gap SL Triggered for Short Symbol "<<symbol[j]<<", CoverPrice: "<<lcoverprice[j]<<std::endl;
            }

          }else if ((low[j]<=sllevel[j]) && (high[j]>=sllevel[j])){
            lcoverprice[j]=sllevel[j];
            sltriggered=true;
            lcover[j]=2;
            cover[j]=2;
            inshorttrade[j]=0;
            if(debug){
              Rcout<<"Date: "<<timestamp[j]<<", Max SL Triggered for Short Symbol "<<symbol[j]<<", CoverPrice: "<<lcoverprice[j]<<std::endl;
            }
          }

          if(!sltriggered){
            if(open[j]<=tplevel[j]){
              lcoverprice[j]=open[j];
              tptriggered=true;
              lcover[j]=5; //gaptp
              cover[j]=5;
              inshorttrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Gap TP Triggered for Short Symbol "<<symbol[j]<<", CoverPrice: "<<lcoverprice[j]<<std::endl;
              }

            }else if ((low[j]<=tplevel[j]) && (high[j]>=tplevel[j])){
              lcoverprice[j]=tplevel[j];
              tptriggered=true;
              lcover[j]=4;//tp
              cover[j]=4;
              inshorttrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Max TP Triggered for Short Symbol "<<symbol[j]<<", CoverPrice: "<<lcoverprice[j]<<std::endl;
              }
            }
            if(cover[j]==1 && !sltriggered && !tptriggered){
              lcover[j]=1;
              lcoverprice[j]=coverprice[j];
              inshorttrade[j]=0;
              if(debug){
                Rcout<<"Date: "<<timestamp[j]<<", Regular Exit Triggered for Short Symbol "<<symbol[j]<<", CoverPrice: "<<lcoverprice[j]<<std::endl;
              }
            }
            if(cover[j]==0){
              lcover[j]=0;
              inshorttrade[j]=1;
            }
          }
        }
      //}
      // ### update positions and enter new trades
      //if(it!=positionNames.end()){
        //we have a matching sell
        int positionIndex=std::distance(positionNames.begin(), it);
        String positionSide=side.at(positionIndex);
        if(positionSide=="BUY" && lsell[j]>0){
          positionNames.erase(it);
          positionBarNumber.erase(positionBarNumber.begin()+positionIndex);
          side.erase(side.begin()+positionIndex);
          it--;
          if(debug){
              Rcout<<"Date: "<<timestamp[j]<<", Sell Symbol: "<<symbol[j]<<", New PositionCount:"<<positionNames.size()<<endl;
            }
        }else if(positionSide=="SHORT" && lcover[j]>0){
          positionNames.erase(it);
          positionBarNumber.erase(positionBarNumber.begin()+positionIndex);
          side.erase(side.begin()+positionIndex);
          it--;
          if(debug){
            Rcout<<"Date: "<<timestamp[j]<<", Cover Symbol: "<<symbol[j]<<", New PositionCount:"<<positionNames.size()<<endl;
          }
        }
        it++;
      }
    }
    daypositionscore.clear();

    //Execute entry for the specific uniquedate[i]
    //1. If there is capacity, Update positionscore
    for(int a=0;a<indices.size();a++){
      //Rf_PrintValue(indices);
      int j=indices[a];
      if((buy[j]>0 && positionscore[j]>0) || (shrt[j]>0 && positionscore[j]>0)){
        if ( daypositionscore.find(positionscore[j]) == daypositionscore.end() ) {
          daypositionscore.insert(std::pair<double,int>(positionscore[j],j));
        }else{
          //add a positionscore that is marginally higher
          daypositionscore.insert(std::pair<double,int>(positionscore[j]+0.00001,j));
        }
      }
    }
    //insert upto limit in positionNames
    int itemsToInsert=0;
    if(positionNames.size()<maxposition){
      itemsToInsert=maxposition-positionNames.size();
    }
    for (std::map<double,int>::reverse_iterator rit=daypositionscore.rbegin(); rit!=daypositionscore.rend
      (); ++rit){

      if(itemsToInsert>0){
        int j=rit->second;
        if(buy[j]>0){
          lbuy[j]=buy[j];
          side.push_back("BUY");
        }else{
          lshrt[j]=shrt[j];
          side.push_back("SHORT");
        }
        positionNames.push_back(symbol[j]);
        positionBarNumber.push_back(j);
        if(debug){
          if(lbuy[j]>0){
            Rcout<< "Date: "<<timestamp[j]<<", Symbol added to long position: "<< symbol[j]<<", PositionScore: "<<rit->first <<std::endl;
          }else if (lshrt[j]>0){
            Rcout<< "Date: "<<timestamp[j]<<", Symbol added to short position: "<< symbol[j]<<", PositionScore: "<<rit->first <<std::endl;
          }
        }
        itemsToInsert--;
      }
    }
    if(debug){
      Rcout <<"Date: "<<uniquedate[i]<<", Positions Held: "<<positionNames.size()<<", Positions Allowed:"<< maxposition<<std::endl;
      for (std::vector<Rcpp::String>::iterator rit=positionNames.begin(); rit!=positionNames.end(); ++rit){
        Rcout<< rit->get_cstring() <<std::endl;
      }
    }

    //update scale-ins
    for(int a=0;a<indices.size();a++){
      //Rf_PrintValue(indices);
      int j=indices[a];
      //Rcout<<"Scale-In Index j:"<<j<<"buy[j]:"<<buy[j]<<"symbol[j]:"<<symbol[j]<<std::endl;
      if(buy[j]==999 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        //Rcout<<"Processing Scale In. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
        lbuy[j]=999; //we have a scale-in for an existing position
      }
      if(shrt[j]==999 && std::find(positionNames.begin(),positionNames.end(),symbol[j])!=positionNames.end()){
        lshrt[j]=999; //we have a scale-in for an existing position
        //Rcout<<"Processing Scale In. Signal Bar:"<<j<<",Symbol:"<<symbol[j]<<",buy:"<<buy[j]<<std::endl;
      }
    }
  }
  inlongtrade=ContinuingLong(symbol,lbuy,lsell,lshrt);
  inshorttrade=ContinuingShort(symbol,lshrt,lcover,lbuy);

  return DataFrame::create(_["date"]=timestamp,_["symbol"]=symbol,_["buy"]=lbuy,_["sell"]=lsell,_["short"]=lshrt,_["cover"]=lcover,
                           _["buyprice"]= lbuyprice, _["sellprice"]= lsellprice,_["shortprice"]=lshortprice,_["coverprice"]=lcoverprice,
                             _["inlongtrade"]=inlongtrade,_["inshorttrade"]=inshorttrade,_["sl.level"]=sllevel,_["tp.level"]=tplevel,
                               _["stringsAsFactors"] = false);

}


// [[Rcpp::export]]
DataFrame ApplySLTP(const DataFrame all,NumericVector slamount,NumericVector tpamount,bool volatilesl=false,bool volatiletp=false,bool preventReplacement=false,bool debug=false){
  //preventReplacementTrade should be set as true if the signals are not being checked daily for BUY, SELL or AVOID
  //stop mode can be 1: points
  //all has to be sorted in ascending order by date
  if(debug){
    // if(!Rf_isNull(file)){
    //   std::string tfile = as<std::string>(file.at(1));
    //   std::ofstream testfile(tfile.c_str());
    //   std::streambuf* Rcout_buffer = Rcout.rdbuf();
    //   Rcout.rdbuf( testfile.rdbuf() );
    // }
    Rcout<<"### Running ApplySLTP ###"<<std::endl;
  }
  int nSize=all.nrows();
  NumericVector inlongtrade=all["inlongtrade"];
  NumericVector inshorttrade=all["inshorttrade"];
  DatetimeVector dates=all["date"];
  const NumericVector open=all["aopen"];
  const NumericVector high=all["ahigh"];
  const NumericVector low=all["alow"];
  const NumericVector close=all["aclose"];
  const NumericVector buy=all["buy"];
  const NumericVector sell=all["sell"];
  const NumericVector shrt=all["short"];
  const NumericVector cover=all["cover"];
  const StringVector symbol=all["symbol"];

  const NumericVector buyprice=all["buyprice"];
  const NumericVector sellprice=all["sellprice"];
  const NumericVector shortprice=all["shortprice"];
  const NumericVector coverprice=all["coverprice"];
  const DatetimeVector timestamp=all["date"];

  NumericVector lbuy(nSize);
  NumericVector lsell(nSize);
  NumericVector lshrt(nSize);
  NumericVector lcover(nSize);

  NumericVector lbuyprice(nSize);
  NumericVector lsellprice(nSize);
  NumericVector lshortprice(nSize);
  NumericVector lcoverprice(nSize);

  NumericVector sllevel(nSize);
  NumericVector tplevel(nSize);

  StringVector uniquesymbol=unique(symbol);
  Rf_PrintValue(uniquesymbol);
  Rcpp::Environment base("package:base");
  Rcpp::Function sort = base["sort"];
  //NumericVector uniquesymbol1=sort(Rcpp::_["x"] = uniquesymbol);
  for(int z=0;z<uniquesymbol.size();z++){//for each uniquesymbol
    bool tptriggered =false;
    bool sltriggered=false;
    int barstart=0;
    IntegerVector indices=whichString2(symbol,uniquesymbol[z]);
    if(debug){
      Rcout<<"Updating buy and short bars for "<<uniquesymbol[z] <<std::endl;
    }
    for(int a=0;a<indices.size();a++){//iterate through rows. We need to ensure that data is sorted in ascending order
      int i=indices[a];
      if(buy[i]>0){
        lbuy[i]=buy[i];
        lbuyprice[i]=buyprice[i];
        if(debug){
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", buy: "<<lbuy[i]<<", BuyPrice: "<<lbuyprice[i]<<std::endl;
        }

      }else if(shrt[i]>0){
        lshrt[i]=shrt[i];
        lshortprice[i]=shortprice[i];
        if(debug){
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", short: "<<lshrt[i]<<", ShortPrice: "<<lshortprice[i]<<std::endl;
        }
      }
      bool newtrade= (a==1 && (lbuy[indices[(a-1)]]>0||lshrt[indices[(a-1)]]>0))||(a>1 &&((lbuy[indices[(a-1)]]>0 && lbuy[indices[(a-2)]]==0)||(lshrt[indices[(a-1)]]>0 && lshrt[indices[(a-2)]]==0)));
      if(newtrade){
        barstart=indices[(a-1)];
      }
      // update sl.level and tp.level
      if(inlongtrade[indices[(a-1)]]>0){
        if(volatiletp){
          tplevel[i]=tpamount[indices[(a-1)]];
        }else {
          tplevel[i]=tpamount[barstart];
        }
        if(volatilesl){
          sllevel[i]=slamount[indices[(a-1)]];
        }else {
          sllevel[i]=slamount[barstart];
        }
        if(debug){
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", Long TP Level: "<<tplevel[i]<<std::endl;
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", Long SL Level: "<<sllevel[i]<<std::endl;
        }
      }


      if(inshorttrade[indices[(a-1)]]>0){
        if(volatiletp){
          tplevel[i]=tpamount[indices[(a-1)]];
        }else {
          tplevel[i]=tpamount[barstart];
        }
        if(volatilesl){
          sllevel[i]=slamount[indices[(a-1)]];
        }else{
          sllevel[i]=slamount[barstart];
        }
        if(debug){
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", Short TP Level: "<<tplevel[i]<<std::endl;
          Rcout<<"Date: "<< dates[i]<<", Symbol:"<<symbol[i]<< ", Short SL Level: "<<sllevel[i]<<std::endl;
        }

      }

      if(newtrade){ //reset stoplosstriggered flag.
        tptriggered=false;
        sltriggered=false;
        barstart=indices[(a-1)];
      }

      if(!tptriggered && !sltriggered){//stoplossnottriggered
        if(inlongtrade[indices[(a-1)]]==1 && lbuy[i]==0){ //dont trigger sl or tp in the entry bar
          //check if stoploss triggered for a long trade
          if(open[i]<=sllevel[i]){
            lsellprice[i]=open[i];
            sltriggered=true;
            lsell[i]=3;
            inlongtrade[i]=0;
            if(debug){
              Rcout<<"Date: "<<dates[i]<<", Gap SL Triggered for Long Symbol "<<symbol[i]<<", SellPrice: "<<lsellprice[i]<<std::endl;
            }
          }else if ((low[i]<=sllevel[i]) && (high[i]>=sllevel[i])){
            lsellprice[i]=sllevel[i];
            sltriggered=true;
            lsell[i]=2;//maxsl
            inlongtrade[i]=0;
            if(debug){
              Rcout<<"Date: "<<dates[i]<<", Max SL Triggered for Long Symbol "<<symbol[i]<<", SellPrice: "<<lsellprice[i]<<std::endl;
            }
          }

          if(!sltriggered && !tptriggered){
            if(open[i]>=tplevel[i]){
              lsellprice[i]=open[i];
              tptriggered=true;
              lsell[i]=5; //gap tp
              inlongtrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Gap TP Triggered for Long Symbol "<<symbol[i]<<", SellPrice: "<<lsellprice[i]<<std::endl;
              }

            }else if ((low[i]<=tplevel[i]) && (high[i]>=tplevel[i])){
              lsellprice[i]=tplevel[i];
              tptriggered=true;
              lsell[i]=4;//maxtp
              inlongtrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Max TP Triggered for Long Symbol "<<symbol[i]<<", SellPrice: "<<lsellprice[i]<<std::endl;
              }
            }
            if (sell[i]==1 && !sltriggered && !tptriggered){
              lsell[i]=1;
              lsellprice[i]=sellprice[i];
              inlongtrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Regular Exit Triggered for Long Symbol "<<symbol[i]<<", SellPrice: "<<lsellprice[i]<<std::endl;
              }
            }
            if(lsell[i]==0){
              inlongtrade[i]=1;
            }
          }
        }else if(inshorttrade[indices[(a-1)]]==1 && lshrt[i]==0){
          //check if stoploss triggered for a short trade
          if(open[i]>=sllevel[i]){
            lcoverprice[i]=open[i];
            sltriggered=true;
            lcover[i]=3;
            inshorttrade[i]=0;
            if(debug){
              Rcout<<"Date: "<<dates[i]<<", Gap SL Triggered for Short Symbol "<<symbol[i]<<", CoverPrice: "<<lcoverprice[i]<<std::endl;
            }

          }else if ((low[i]<=sllevel[i]) && (high[i]>=sllevel[i])){
            lcoverprice[i]=sllevel[i];
            sltriggered=true;
            lcover[i]=2;
            inshorttrade[i]=0;
            if(debug){
              Rcout<<"Date: "<<dates[i]<<", Max SL Triggered for Short Symbol "<<symbol[i]<<", CoverPrice: "<<lcoverprice[i]<<std::endl;
            }
          }

          if(!sltriggered && !tptriggered){
            if(open[i]<=tplevel[i]){
              lcoverprice[i]=open[i];
              tptriggered=true;
              lcover[i]=5; //gaptp
              inshorttrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Gap TP Triggered for Short Symbol "<<symbol[i]<<", CoverPrice: "<<lcoverprice[i]<<std::endl;
              }

            }else if ((low[i]<=tplevel[i]) && (high[i]>=tplevel[i])){
              lcoverprice[i]=tplevel[i];
              tptriggered=true;
              lcover[i]=4;//tp
              inshorttrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Max TP Triggered for Short Symbol "<<symbol[i]<<", CoverPrice: "<<lcoverprice[i]<<std::endl;
              }
            }
            if(cover[i]==1 && !sltriggered && !tptriggered){
              lcover[i]=1;
              lcoverprice[i]=coverprice[i];
              inshorttrade[i]=0;
              if(debug){
                Rcout<<"Date: "<<dates[i]<<", Regular Exit Triggered for Short Symbol "<<symbol[i]<<", CoverPrice: "<<lcoverprice[i]<<std::endl;
              }
            }
            if(lcover[i]==0){
              inshorttrade[i]=1;
            }
          }
        }
      }
      if(!preventReplacement){
        if(tptriggered ||sltriggered){//check if a replacement trade is needed
          //needed if intrade is true at the bar
          //Rcout << "New:"<< preventReplacement<<inlongtrade[i] <<inshorttrade[i]<<"i:"<<i<<std::endl;
          if(inlongtrade[i]==1 && cover[i]==0 ){
            //enter fresh long trade. Update all buyprices for bars ahead, till you arrive at a non-zero buyprice.
            int j=i;
            lbuy[i]=2;//replacement trade
            tptriggered=false;
            lbuyprice[i]=close[i];
            // while((inlongtrade[j]!=0) && (j<nSize)) {
            //         lbuyprice[j]=close[i];
            //   j++;
            // };

          }else if((inshorttrade[i]==1) && (sell[i]==0)){
            //enter fresh short trade
            int j=i;
            tptriggered=false;
            lshrt[i]=2;//replacement trade
            lshortprice[i]=close[i];
            // while((inshorttrade[j]!=0) && (j<nSize)){
            //         lshortprice[j]=close[i];
            //         j++;
            // };
          }
        }
      }
    }
  }
  inlongtrade=ContinuingLong(symbol,lbuy,lsell,lshrt);
  inshorttrade=ContinuingShort(symbol,lshrt,lcover,lbuy);

  return DataFrame::create(_["date"]=timestamp,_["symbol"]=symbol,_["buy"]=lbuy,_["sell"]=lsell,_["short"]=lshrt,_["cover"]=lcover,
                           _["buyprice"]= lbuyprice, _["sellprice"]= lsellprice,_["shortprice"]=lshortprice,_["coverprice"]=lcoverprice,
                             _["inlongtrade"]=inlongtrade,_["inshorttrade"]=inshorttrade,_["sl.level"]=sllevel,_["tp.level"]=tplevel,
                               _["stringsAsFactors"] = false);

}


// [[Rcpp::export]]

DataFrame Trend(DatetimeVector date,NumericVector high,NumericVector low, NumericVector close){
  //int startIndex = 1;
  int swingLowStartIndex = 0;
  int swingHighStartIndex = 0;
  //int swingLowEndIndex;
  //int swingHighEndIndex;
  int nSize=close.size();
  NumericVector trendtype(nSize);
  NumericVector result(nSize);
  NumericVector updownbarclean(nSize);
  NumericVector updownbar(nSize);
  NumericVector outsidebar(nSize);
  NumericVector insidebar(nSize);

  NumericVector swinghigh(nSize);
  NumericVector swinghighhigh(nSize);
  NumericVector swinghighhigh_1(nSize);
  NumericVector swinghighhigh_2(nSize);


  NumericVector swinglow(nSize);
  NumericVector swinglowlow(nSize);
  NumericVector swinglowlow_1(nSize);
  NumericVector swinglowlow_2(nSize);

  NumericVector swinglevel(nSize);

  //global store for higherhigh and lowerlow count
  NumericVector higherhigh(nSize);
  NumericVector lowerlow(nSize);

  NumericVector movetotal(nSize);
  NumericVector movesettle(nSize);


  for (int i = 1; i < nSize; i++) {
    result[i] = 0;
    updownbarclean[i] = 0;
    updownbar[i] = 0;
    outsidebar[i] = 0;
    insidebar[i] = 0;

    bool hh = high[i] > high[i - 1];
    bool lh = high[i] < high[i - 1];
    bool ll = low[i] < low[i - 1];
    bool hl = low[i] > low[i - 1];
    bool el = low[i] == low[i - 1];
    bool eh = high[i] == high[i - 1];

    if ((hh && hl) || (eh && hl) || (hh && el)) {
      updownbarclean[i] = 1;
      updownbar[i] = 1;
    }
    else if ((lh && ll) || (el && lh) || (ll && eh)) {
      updownbarclean[i] = -1;
      updownbar[i] = -1;
    }
    else if (hh && ll) { //outside bar
      outsidebar[i] = 1;
    }
    else {
      insidebar[i] = 1;
    }
  }

  for (int i = 1; i < nSize; i++) {
    if (outsidebar[i] == 1) {
      updownbar[i] = -updownbar[i - 1];
    }
    else if (insidebar[i] == 1) {
      updownbar[i] = updownbar[i - 1];
    }
  }

  for (int i = 1; i < nSize; i++) {
    swinglevel[i] = 0;
    swinghigh[i], swinghighhigh[i], swinghighhigh_1[i] = 0;
    swinglow[i], swinglowlow[i], swinglowlow_1[i] = 0;
    if (updownbar[i] == 1) {//we are in upswing
      if (updownbar[i - 1] == 1) {//continuing upswing
        swinghigh[i] = high[i]>swinghigh[i-1]?high[i]:swinghigh[i-1];
        //swinghighhigh will be updated when the upswing ends or if its the last upswing.
        //update swinglow and swinglowlow for "i"
        if ((swinglow[i - 1] != 0) & (outsidebar[i] != 1)) { //prevent init of swinglow to 0!!
          swinglow[i] = swinglow[i - 1];
          swinglowlow[i] = swinglow[i - 1];
        }
      }
      else { //first day of upswing
        swingHighStartIndex = i;
        //swingLowEndIndex = i - 1;

        swinghigh[i] = high[i];
        if (outsidebar[i] == 1) {
          swinglow[i] = std::min(low[i], swinglow[i - 1]);
          swinglowlow[i] = swinglow[i];
        }
        else {
          swinglow[i] = swinglow[i - 1];
          swinglowlow[i] = swinglow[i - 1];
        }

        for (int j = swingLowStartIndex; j < swingHighStartIndex; j++) {
          if (outsidebar[i] == 1) {
            swinglowlow[j] = swinglow[i];
          }
          else {
            swinglowlow[j] = swinglow[i - 1];
          }
        }
      }
    }		else if (updownbar[i] == -1) {//we are in a downswing
      if (updownbar[i - 1] == -1) {//continuing downswing
        swinglow[i] = min(low[i], swinglow[i - 1]);
        //swinglowlow will be updated when the downswing ends or if its the last downswing.
        //update swinghigh and swinghighhigh for "i"
        if ((swinghigh[i - 1] != 0) & (outsidebar[i] != 1)) { //prevent init of swinghigh to 0!!
          swinghigh[i] = swinghigh[i - 1];
          swinghighhigh[i] = swinghigh[i - 1];
        }
      }
      else { //first day of downswing
        swingLowStartIndex = i;
        //swingHighEndIndex = i - 1;

        swinglow[i] = low[i];
        if (outsidebar[i] == 1) {
          swinghigh[i] = max(high[i], swinghigh[i - 1]);
          swinghighhigh[i] = swinghigh[i];
        }
        else {
          swinghigh[i] = swinghigh[i - 1];
          swinghighhigh[i] = swinghigh[i - 1];
        }
        for (int j = swingHighStartIndex; j < swingLowStartIndex; j++) {
          if (outsidebar[i] == 1) {
            swinghighhigh[j] = swinghigh[i];
          }
          else {
            swinghighhigh[j] = swinghigh[i - 1];
          }
        }
      }
    }
  }

  //update swinghighhigh,swinglowlow for last (incomplete) swing
  if (swingHighStartIndex > swingLowStartIndex) {//last incomplete swing is up
    for (int j = swingHighStartIndex; j < nSize; j++) {
      swinghighhigh[j] = swinghigh[nSize - 1];
    }
  }
  else {
    for (int j = swingLowStartIndex; j < nSize; j++) {
      swinglowlow[j] = swinglow[nSize - 1];
    }
  }

  //calculate shifted versions of highhigh and lowlow
  swinglowlow_1=Shift(swinglowlow,updownbar,-1);
  swinghighhigh_1 = Shift(swinghighhigh, updownbar,1);
  swinglowlow_2 = Shift(swinglowlow_1,updownbar,-1);
  swinghighhigh_2 = Shift(swinghighhigh_1,updownbar, 1);

  //create swing level
  for (int j = 1; j < nSize; j++) {
    if (updownbar[j] == 1) {
      swinglevel[j] = swinghighhigh[j];
    }
    else if (updownbar[j] == -1){
      swinglevel[j] = swinglowlow[j];
    }
    else {
      swinglevel[j] = 0;
    }
  }
  swinglevel[0]=swinglevel[1]; //ensure first swinglevel is never zero. else it goes outside plot


  // update trend
  for (int i = 1; i < nSize; i++) {
    result[i] = 0;

    bool up1 = (updownbar[i] == 1) && swinghigh[i] > swinghighhigh_1[i] && swinglow[i] > swinglowlow_1[i];
    bool up2 = (updownbar[i] == 1) && swinghighhigh_1[i] > swinghighhigh_2[i] && swinglow[i] > swinglowlow_1[i];
    bool up3 = (updownbar[i] == -1 || outsidebar[i] == 1) && swinghigh[i] > swinghighhigh_1[i] && swinglowlow_1[i] > swinglowlow_2[i] && swinglow[i]>swinglowlow_1[i] && low[i] > swinglowlow_1[i] ;
    bool down1 = (updownbar[i] == -1) && swinghigh[i] < swinghighhigh_1[i] && swinglow[i] < swinglowlow_1[i];
    bool down2 = (updownbar[i] == -1) && swinghigh[i] < swinghighhigh_1[i] && swinglowlow_1[i] < swinglowlow_2[i];
    bool down3 = (updownbar[i] == 1 || outsidebar[i] == 1) && swinghighhigh_1[i] < swinghighhigh_2[i] && swinglow[i] < swinglowlow_1[i] && swinghigh[i] <swinghighhigh_1[i] && high[i] < swinghighhigh_1[i];

    if (up1 || up2 || up3) {
      result[i] = 1;
      if(up1){
        trendtype[i]=1;
      }else if(up2){
        trendtype[i]=2;
      }else if(up3){
        trendtype[i]=3;
      }
    }
    else if (down1 || down2 || down3) {
      result[i] = -1;
      if(down1){
        trendtype[i]=1;
      }else if(down2){
        trendtype[i]=2;
      }else if(down3){
        trendtype[i]=3;
      }
    }
  }

  // update global higherhigh and lowerlow count
  higherhigh[0]=0;
  lowerlow[0]=0;
  for (int i = 1; i < nSize; i++) {
    if(result[i]==result[i-1]){
      //continuing trend
      if(updownbarclean[i]==1){
        higherhigh[i]=higherhigh[i-1]+1;
        lowerlow[i]=lowerlow[i-1];
      }else if(updownbarclean[i]==-1){
        higherhigh[i]=higherhigh[i-1];
        lowerlow[i]=lowerlow[i-1]+1;
      }else{
        higherhigh[i]=higherhigh[i-1];
        lowerlow[i]=lowerlow[i-1];
      }
    }else{
      if(updownbarclean[i]==1){
        higherhigh[i]=1;
        lowerlow[i]=0;
      }else if(updownbarclean[i]==-1){
        higherhigh[i]=0;
        lowerlow[i]=01;
      }else{
        higherhigh[i]=0;
        lowerlow[i]=0;
      }
    }
  }

  //update size of move
  movetotal[0]=0;
  movesettle[0]=0;
  double hhsincetrend=0;
  double llsincetrend=100000000;
  int startoftrend=0;
  for (int i = 1; i < nSize; i++) {
    if(result[i]!=result[i-1]){
      startoftrend=i;
      hhsincetrend=high[i];
      llsincetrend=low[i];
    }else{
      hhsincetrend=high[i]>hhsincetrend?high[i]:hhsincetrend;
      llsincetrend=low[i]<llsincetrend?low[i]:llsincetrend;
    }
    movetotal[i]=hhsincetrend-llsincetrend;
    movesettle[i]=close[i]-close[startoftrend];
  }

  //        DataFrame out=create()(Named("trend")=result,Named("updownbar")=updownbar);
  return DataFrame::create(_("date")=date,_("trend")=result,_("updownbar")=updownbar,
                             _("outsidebar")=outsidebar,_("insidebar")=insidebar,
                             _("swinghigh")=swinghigh,_("swinglow")=swinglow,
                             _("swinghighhigh")=swinghighhigh,_("swinglowlow")=swinglowlow,
                             _("swinghighhigh_1")=swinghighhigh_1,_("swinglowlow_1")=swinglowlow_1,
                             _("swinghighhigh_2")=swinghighhigh_2,_("swinglowlow_2")=swinglowlow_2,
                             _("swinglevel")=swinglevel,_("numberhh")=higherhigh,_("numberll")=lowerlow,
                             _("movementhighlow")=movetotal,_("movementsettle")=movesettle,
                             _("updownbarclean")=updownbarclean,_("trendtype")=trendtype);
}

bool highestSinceNBars(NumericVector price,int index, int level=1){
  int priorindex=index-level;
  Rcout << "Index: " << index <<",Level: "<< level << ",price: " <<price[index] << ",prior price: " << price[priorindex]<<std::endl;
  bool valid=false;
  if(level>1){
    return highestSinceNBars(price, index,(level-1));
  }else{
    if(price[index]>price[index-level]){
      valid=true;
    }else{
      valid=false;
    }
    Rcout << "Returning Index: " << index <<",Level: "<< level << ",valid: "<<valid <<std::endl;
    return valid;
  }
  Rcout << "Returning Final Index: " << index <<",Level: "<< level << ",valid: "<<valid <<std::endl;
  return false;

}

bool highestSorrundingNBars(NumericVector price,int index, int level=1){
  int priorindex=index-level;
  int futureindex=index+level;
  //Rcout << "Index: " << index <<",Level: "<< level << ",price: " <<price[index] << ",prior price: " << price[priorindex]<< ",future price: " << price[futureindex]<<std::endl;
  bool valid=true;
  if(level>1){
    bool out=highestSorrundingNBars(price, index,(level-1));
    //Rcout << "Intermediate Result:" <<out <<",level: "<<(level-1)<<std::endl;
    valid=price[index]>price[index-level] & price[index]>price[index+level] & out;
  }else{
    if(price[index]>price[index-level] & price[index]>price[index+level]){
      valid=true;
    }else{
      valid=false;
    }
    return valid;
    //Rcout << "Last Result:" <<valid <<std::endl;
  }
  //Rcout << "Returning Final Index: " << index <<",Level: "<< level << ",valid: "<<valid <<std::endl;
  return valid;

}

bool lowestSorrundingNBars(NumericVector price,int index, int level=1){
  int priorindex=index-level;
  int futureindex=index+level;
  //Rcout << "Index: " << index <<",Level: "<< level << ",price: " <<price[index] << ",prior price: " << price[priorindex]<< ",future price: " << price[futureindex]<<std::endl;
  bool valid=true;
  if(level>1){
    bool out=lowestSorrundingNBars(price, index,(level-1));
    //Rcout << "Intermediate Result:" <<out <<",level: "<<(level-1)<<std::endl;
    valid=price[index]<price[index-level] & price[index]<price[index+level] & out;
  }else{
    if(price[index]<price[index-level] & price[index]<price[index+level]){
      valid=true;
    }else{
      valid=false;
    }
    return valid;
    //Rcout << "Last Result:" <<valid <<std::endl;
  }
  //Rcout << "Returning Final Index: " << index <<",Level: "<< level << ",valid: "<<valid <<std::endl;
  return valid;

}

// [[Rcpp::export]]

DataFrame TDSupplyPoints(DatetimeVector dates,NumericVector price, int level=1){
  int nSize=price.size();
  vector<int>index;
  vector<double>tdprices;
  if(nSize>level){
    for(int i=level;i<nSize-level;i++){
      int tdsupplypoint=0;
      //Rcout << "Making Call: " <<std::endl;
      tdsupplypoint=highestSorrundingNBars(price,i,level);
      //Rcout << "TDSupplyPoint: " << tdsupplypoint <<std::endl;
      if(tdsupplypoint==1){
        index.push_back(i+1);
        tdprices.push_back(price[i]);
        //Rcout << "TDSupplyPoint Index: " << i+1 <<",TDPrice: "<< price[i] <<std::endl;
      }
    }
  }
  DatetimeVector timestamp(index.size());
  for(int i=0;i<index.size();i++){
    timestamp[i]=dates[index.at(i)];
  }
  return DataFrame::create(_("date")=timestamp,_("tdsupplyprices")=wrap(tdprices),_("index")=wrap(index));

}


// [[Rcpp::export]]

DataFrame TDDemandPoints(DatetimeVector dates,NumericVector price, int level=1){
  int nSize=price.size();
  vector<int>index;
  vector<double>tdprices;
  if(nSize>level){
    for(int i=level;i<nSize-level;i++){
      bool tddemandpoint=false;
      // Rcout << "Making Call: " <<std::endl;
      tddemandpoint=lowestSorrundingNBars(price,i,level);
      //Rcout << "TDSupplyPoint: " << tdsupplypoint <<std::endl;
      if(tddemandpoint){
        index.push_back(i);
        tdprices.push_back(price[i]);
        //Rcout << "TDSupplyPoint Index: " << i+1 <<",TDPrice: "<< price[i] <<std::endl;
      }
    }
  }
  DatetimeVector timestamp(index.size());
  for(int i=0;i<index.size();i++){
    timestamp[i]=dates[index.at(i)];
  }

  return DataFrame::create(_("date")=timestamp,_("tddemandprices")=wrap(tdprices),_("index")=wrap(index));

}

// [[Rcpp::export]]

DataFrame TDDemandLine(DatetimeVector dates,NumericVector price,NumericVector origindices){
  int nSize=price.size();
  vector<int>index;
  vector<double>tdprices;
  if(nSize>0){
    index.push_back(nSize-1);
    tdprices.push_back(price[nSize-1]);
    if(nSize>=2){
      for(int i=nSize-2;i>=0;i--){
        if(price[i]<tdprices.back()){
          index.push_back(i);
          tdprices.push_back(price[i]);
          //Rcout << "TDSupplyPoint Index: " << i+1 <<",TDPrice: "<< price[i] <<std::endl;
        }
      }
    }
  }
  DatetimeVector timestamp(index.size());
  NumericVector shortlistedindices(index.size());
  for(int i=0;i<index.size();i++){
    timestamp[i]=dates[index.at(i)];
    shortlistedindices[i]=origindices[index.at(i)];
  }
  return DataFrame::create(_("date")=timestamp,_("tddemandline")=wrap(tdprices),_("index")=shortlistedindices);
}

// [[Rcpp::export]]

DataFrame TDSupplyLine(DatetimeVector dates,NumericVector price,NumericVector origindices){
  int nSize=price.size();
  vector<int>index;
  vector<double>tdprices;
  if(nSize>0){
    index.push_back(nSize-1);
    tdprices.push_back(price[nSize-1]);
    if(nSize>=2){
      for(int i=nSize-2;i>=0;i--){
        if(price[i]>tdprices.back()){
          index.push_back(i);
          tdprices.push_back(price[i]);
          //Rcout << "TDSupplyPoint Index: " << i+1 <<",TDPrice: "<< price[i] <<std::endl;
        }
      }
    }
  }
  DatetimeVector timestamp(index.size());
  NumericVector shortlistedindices(index.size());
  for(int i=0;i<index.size();i++){
    timestamp[i]=dates[index.at(i)];
    shortlistedindices[i]=origindices[index.at(i)];

  }
  return DataFrame::create(_("date")=timestamp,_("tdsupplyline")=wrap(tdprices),_("index")=shortlistedindices);
}

NumericVector CustomSubset(NumericVector input, int start, int end){
  NumericVector out(end-start);
  int j=0;
  for(int i=start;i<end;i++){
    out[j]=input[i];
    j++;
  }
}

// [[Rcpp::export]]

DataFrame TDSupplyPoints1(DatetimeVector dates,NumericVector price, int level=1){
  vector<vector<int> > outindex ;
  vector<vector<double> > outprices ;
  for(int j=0;j<price.size();j++){
    NumericVector subprice=CustomSubset(price,0,j);
    vector<int>index;
    vector<double>tdprices;
    int nSize=subprice.size();
    if(nSize>2*level+1){
      for(int i=level;i<nSize-level;i++){
        int tdsupplypoint=0;
        //Rcout << "Making Call: " <<std::endl;
        tdsupplypoint=highestSorrundingNBars(subprice,i,level);
        //Rcout << "TDSupplyPoint: " << tdsupplypoint <<std::endl;
        if(tdsupplypoint==1){
          index.push_back(i+1);
          tdprices.push_back(price[i]);
          //Rcout << "TDSupplyPoint Index: " << i+1 <<",TDPrice: "<< price[i] <<std::endl;
        }
      }
    }
    outindex.push_back(index);
    outprices.push_back(tdprices);

  }

  return DataFrame::create(_("index")=wrap(outindex),_("prices")=wrap(outprices));

}

// [[Rcpp::export]]
NumericVector getBuyIndices(DataFrame all, int sellIndex, int lookback=1){
  //getBuyIndices and getSellIndices have now a default parameter "lookback" that defines the adjustment to the second index
  //parameter. If lookback is 0, the search for buy/sell indices considers the current row.
  //If lookback is 1, it considers the prior row
  NumericVector buy=all["buy"];
  vector<int>index;
  for(int i=(sellIndex-1-lookback);i>=0;i--){
    //                Rcout << "i: "<<i<<","<<buy[i] <<std::endl;
    if(buy[i]>=1 && buy[i]<999){
      index.push_back(i+1);
      break;
    }else if(buy[i]==999){
      index.push_back(i+1);
    }
  }
  return wrap(index);

}

// [[Rcpp::export]]
NumericVector getShortIndices(DataFrame all, int coverIndex, int lookback=1){
  NumericVector shrt=all["short"];
  vector<int>index;
  for(int i=(coverIndex-1-lookback);i>=0;i--){
    //                Rcout << "i: "<<i<<","<<buy[i] <<std::endl;
    if(shrt[i]>=1 && shrt[i]<999){
      index.push_back(i+1);
      break;
    }else if(shrt[i]==999){
      index.push_back(i+1);
    }
  }
  return wrap(index);

}
