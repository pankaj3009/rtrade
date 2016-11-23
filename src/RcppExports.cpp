// This file was generated by Rcpp::compileAttributes
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include <Rcpp.h>

using namespace Rcpp;

// Ref
NumericVector Ref(NumericVector input, NumericVector shift);
RcppExport SEXP RTrade_Ref(SEXP inputSEXP, SEXP shiftSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< NumericVector >::type input(inputSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type shift(shiftSEXP);
    __result = Rcpp::wrap(Ref(input, shift));
    return __result;
END_RCPP
}
// ExRem
NumericVector ExRem(NumericVector vec1, NumericVector vec2);
RcppExport SEXP RTrade_ExRem(SEXP vec1SEXP, SEXP vec2SEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< NumericVector >::type vec1(vec1SEXP);
    Rcpp::traits::input_parameter< NumericVector >::type vec2(vec2SEXP);
    __result = Rcpp::wrap(ExRem(vec1, vec2));
    return __result;
END_RCPP
}
// Flip
NumericVector Flip(NumericVector vec1, NumericVector vec2);
RcppExport SEXP RTrade_Flip(SEXP vec1SEXP, SEXP vec2SEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< NumericVector >::type vec1(vec1SEXP);
    Rcpp::traits::input_parameter< NumericVector >::type vec2(vec2SEXP);
    __result = Rcpp::wrap(Flip(vec1, vec2));
    return __result;
END_RCPP
}
// BarsSince
NumericVector BarsSince(NumericVector vec1);
RcppExport SEXP RTrade_BarsSince(SEXP vec1SEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< NumericVector >::type vec1(vec1SEXP);
    __result = Rcpp::wrap(BarsSince(vec1));
    return __result;
END_RCPP
}
// Cross
NumericVector Cross(NumericVector snake, NumericVector reference);
RcppExport SEXP RTrade_Cross(SEXP snakeSEXP, SEXP referenceSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< NumericVector >::type snake(snakeSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type reference(referenceSEXP);
    __result = Rcpp::wrap(Cross(snake, reference));
    return __result;
END_RCPP
}
// linkedsymbols
StringVector linkedsymbols(StringVector initial, StringVector final, String symbol);
RcppExport SEXP RTrade_linkedsymbols(SEXP initialSEXP, SEXP finalSEXP, SEXP symbolSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< StringVector >::type initial(initialSEXP);
    Rcpp::traits::input_parameter< StringVector >::type final(finalSEXP);
    Rcpp::traits::input_parameter< String >::type symbol(symbolSEXP);
    __result = Rcpp::wrap(linkedsymbols(initial, final, symbol));
    return __result;
END_RCPP
}
// whichDate2
IntegerVector whichDate2(DatetimeVector x, Datetime condition);
RcppExport SEXP RTrade_whichDate2(SEXP xSEXP, SEXP conditionSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DatetimeVector >::type x(xSEXP);
    Rcpp::traits::input_parameter< Datetime >::type condition(conditionSEXP);
    __result = Rcpp::wrap(whichDate2(x, condition));
    return __result;
END_RCPP
}
// CalculatePortfolioEquityCurve
DataFrame CalculatePortfolioEquityCurve(String symbol, DataFrame all, DataFrame trades, NumericVector size, double brokerage);
RcppExport SEXP RTrade_CalculatePortfolioEquityCurve(SEXP symbolSEXP, SEXP allSEXP, SEXP tradesSEXP, SEXP sizeSEXP, SEXP brokerageSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< String >::type symbol(symbolSEXP);
    Rcpp::traits::input_parameter< DataFrame >::type all(allSEXP);
    Rcpp::traits::input_parameter< DataFrame >::type trades(tradesSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type size(sizeSEXP);
    Rcpp::traits::input_parameter< double >::type brokerage(brokerageSEXP);
    __result = Rcpp::wrap(CalculatePortfolioEquityCurve(symbol, all, trades, size, brokerage));
    return __result;
END_RCPP
}
// GenerateTrades
DataFrame GenerateTrades(DataFrame all);
RcppExport SEXP RTrade_GenerateTrades(SEXP allSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DataFrame >::type all(allSEXP);
    __result = Rcpp::wrap(GenerateTrades(all));
    return __result;
END_RCPP
}
// GenerateTradesShort
DataFrame GenerateTradesShort(DataFrame all);
RcppExport SEXP RTrade_GenerateTradesShort(SEXP allSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DataFrame >::type all(allSEXP);
    __result = Rcpp::wrap(GenerateTradesShort(all));
    return __result;
END_RCPP
}
// ProcessPositionScore
DataFrame ProcessPositionScore(DataFrame all, unsigned int maxposition, DatetimeVector dates);
RcppExport SEXP RTrade_ProcessPositionScore(SEXP allSEXP, SEXP maxpositionSEXP, SEXP datesSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DataFrame >::type all(allSEXP);
    Rcpp::traits::input_parameter< unsigned int >::type maxposition(maxpositionSEXP);
    Rcpp::traits::input_parameter< DatetimeVector >::type dates(datesSEXP);
    __result = Rcpp::wrap(ProcessPositionScore(all, maxposition, dates));
    return __result;
END_RCPP
}
// ProcessPositionScoreShort
DataFrame ProcessPositionScoreShort(DataFrame all, unsigned int maxposition, DatetimeVector dates);
RcppExport SEXP RTrade_ProcessPositionScoreShort(SEXP allSEXP, SEXP maxpositionSEXP, SEXP datesSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DataFrame >::type all(allSEXP);
    Rcpp::traits::input_parameter< unsigned int >::type maxposition(maxpositionSEXP);
    Rcpp::traits::input_parameter< DatetimeVector >::type dates(datesSEXP);
    __result = Rcpp::wrap(ProcessPositionScoreShort(all, maxposition, dates));
    return __result;
END_RCPP
}
// ApplyStop
DataFrame ApplyStop(const DataFrame all, NumericVector amount);
RcppExport SEXP RTrade_ApplyStop(SEXP allSEXP, SEXP amountSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< const DataFrame >::type all(allSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type amount(amountSEXP);
    __result = Rcpp::wrap(ApplyStop(all, amount));
    return __result;
END_RCPP
}
// Trend
DataFrame Trend(DatetimeVector date, NumericVector high, NumericVector low, NumericVector close);
RcppExport SEXP RTrade_Trend(SEXP dateSEXP, SEXP highSEXP, SEXP lowSEXP, SEXP closeSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< DatetimeVector >::type date(dateSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type high(highSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type low(lowSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type close(closeSEXP);
    __result = Rcpp::wrap(Trend(date, high, low, close));
    return __result;
END_RCPP
}
