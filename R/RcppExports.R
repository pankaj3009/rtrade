# Generated by using Rcpp::compileAttributes() -> do not edit by hand
# Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

Ref <- function(input, shift) {
    .Call('RTrade_Ref', PACKAGE = 'RTrade', input, shift)
}

ExRem <- function(vec1, vec2) {
    .Call('RTrade_ExRem', PACKAGE = 'RTrade', vec1, vec2)
}

Flip <- function(vec1, vec2) {
    .Call('RTrade_Flip', PACKAGE = 'RTrade', vec1, vec2)
}

BarsSince <- function(vec1) {
    .Call('RTrade_BarsSince', PACKAGE = 'RTrade', vec1)
}

Cross <- function(snake, reference) {
    .Call('RTrade_Cross', PACKAGE = 'RTrade', snake, reference)
}

linkedsymbols <- function(initial, final, symbol) {
    .Call('RTrade_linkedsymbols', PACKAGE = 'RTrade', initial, final, symbol)
}

whichDate2 <- function(x, condition) {
    .Call('RTrade_whichDate2', PACKAGE = 'RTrade', x, condition)
}

CalculatePortfolioEquityCurve <- function(symbol, all, trades, size, brokerage) {
    .Call('RTrade_CalculatePortfolioEquityCurve', PACKAGE = 'RTrade', symbol, all, trades, size, brokerage)
}

GenerateTrades <- function(all) {
    .Call('RTrade_GenerateTrades', PACKAGE = 'RTrade', all)
}

GenerateTradesShort <- function(all) {
    .Call('RTrade_GenerateTradesShort', PACKAGE = 'RTrade', all)
}

ProcessPositionScore <- function(all, maxposition, dates) {
    .Call('RTrade_ProcessPositionScore', PACKAGE = 'RTrade', all, maxposition, dates)
}

ProcessPositionScoreShort <- function(all, maxposition, dates) {
    .Call('RTrade_ProcessPositionScoreShort', PACKAGE = 'RTrade', all, maxposition, dates)
}

ApplyStop <- function(all, amount) {
    .Call('RTrade_ApplyStop', PACKAGE = 'RTrade', all, amount)
}

Trend <- function(date, high, low, close) {
    .Call('RTrade_Trend', PACKAGE = 'RTrade', date, high, low, close)
}

