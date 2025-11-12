#nolint start

library(data.table)
library(ggplot2)

dtr = function(x) as.data.table(read.csv(x))

dt = dtr('data/raw/new_sentiment_data.csv')

dt[,yr := as.integer(substr(date,nchar(date)-1,nchar(date)))]

dts = dt[yr >= 12 & yr <= 25]
dts[, date_clean := as.IDate(date, format = "%m/%d/%Y")]

fwrite(dts,file = 'data/derived/news_sentiment_filtered.csv')









