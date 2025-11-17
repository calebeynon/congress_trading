# nolint start
library(data.table)
library(ggplot2)
library(fixest)
library(stargazer)
library(jsonlite)
library(modelsummary)

dtr = function(x) as.data.table(read.csv(x))

dt = dtr('data/derived/panel.csv')
max_map = fromJSON('data/derived/max_maps.json')
min_map = fromJSON('data/derived/min_maps.json')

map = as.data.table(rbind(stack(min_map),stack(max_map)))
map[, treat := paste0('event_',ind)]
map[, event_date := as.IDate(values, format = "%Y-%m-%d")]


# some preliminary filtering
dt[,Date := as.IDate(Date, format = "%Y-%m-%d")]
dt[,Cong := Total_Trade_Size_USD]
dt[,SP := S.P_500_Volume]
dt[,DJ := Dow_Jones_Volume]
dt[,NQ := NASDAQ_100_Volume]
dt = dt[apply(dt[, -c('Date','event','X','treat'), with = FALSE], 1, function(x) all(x != 0))]
dt[, X := NULL]


# remove any events with < 20 days of valid data
dt[, N := .N, by = treat]
dt = dt[N >= 20]

dt = merge(dt,map, by = c('treat'))


dt = dt[,.(Date,treat,event,Cong,SP,DJ,NQ,event_date)]
dt_long = melt(dt,id.vars = c('Date','treat','event','event_date'),value.name = 'Vol',variable.name = 'Loc')

# dummies for treatment and control
dt_long[, post_treat := Date >= event_date]
dt_long[, is_cong := Loc == 'Cong']
dt_long[, rel_time := as.numeric(Date - event_date)]
dt_long[, leakage_window := rel_time %in% c(-3,-2,-1)]
#fwrite(dt_long, file = 'data/derived/long_panel_final.csv')

dt_long = dt_long[Loc %in% c('Cong','SP')]


#regression finally!
# Result: ONE coefficient PER EVENT (Event 1 effect, Event 2 effect...)
res_by_event_max <- feols(log(Vol) ~ i(treat, is_cong * post_treat) | 
                      Loc + treat + rel_time, 
                      cluster = ~treat,
                      data = dt_long[event == 'max'])


res_by_event_min <- feols(log(Vol) ~ i(treat, is_cong * post_treat) | 
                      Loc + treat + rel_time, 
                      cluster = ~treat,
                      data = dt_long[event == 'min'])


# three period regression
res_leakage <- feols(log(Vol) ~ 
                     i(event, is_cong * leakage_window) + # The "Informed" Effect
                     i(event, is_cong * post_treat) |    # The "Public" Effect
                     Loc + treat + rel_time,               # The Fixed Effects
                     cluster = ~treat,
                     data = dt_long)


res_leakage_by_event <- feols(log(Vol) ~ 
                            i(treat, is_cong * leakage_window)+
                            i(treat, is_cong * post_treat) |
                            Loc + treat + rel_time,
                            cluster = ~treat,
                            data = dt_long[event == 'max'])








