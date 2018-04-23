#include <Rcpp.h>
using namespace Rcpp;

// This is a simple example of exporting a C++ function to R. You can
// source this function into an R session using the Rcpp::sourceCpp
// function (or via the Source button on the editor toolbar). Learn
// more about Rcpp at:
//
//   http://www.rcpp.org/
//   http://adv-r.had.co.nz/Rcpp.html
//   http://gallery.rcpp.org/
//

// [[Rcpp::export]]
NumericVector timesTwo(NumericVector x) {
  return x * 2;
}

// [[Rcpp::export]]
IntegerVector whichDate2(DatetimeVector x, DatetimeVector condition) {
  IntegerVector v = Rcpp::seq(0, x.size()-1);
  //Rcout<<x.size()<<std::endl;
  DatetimeVector check(x.size());
  std::vector<int> indices;
  for(int i=0;i<x.size();i++){
    //Rcout<<x[i]<<","<<condition<<std::endl;
    if(x[i]==condition[1]){
      indices.push_back(v[i]);
    }
  }
  return wrap(indices);
}



// You can include R code blocks in C++ files processed with sourceCpp
// (useful for testing and development). The R code will be automatically
// run after the compilation.
//

/*** R
timesTwo(42)
*/
