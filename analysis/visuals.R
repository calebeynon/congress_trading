# nolint start
library(data.table)
library(ggplot2)


dtr = function(x) as.data.table(read.csv(x))

dt_long = dtr('data/derived/long_panel_final.csv')

p = ggplot(dt_long[treat == 'event_5' & (Loc == 'Cong' | Loc == 'SP')],aes(x = rel_time, y = log(Vol),color = Loc))+
        theme_minimal()+
        geom_line()+
        geom_vline(xintercept = 0,color = 'red')

p1 = ggplot(dt_long[Loc %in% c('Cong', 'SP') & event == 'max'], aes(x = rel_time, y = log(Vol), color = Loc)) +
    theme_minimal() +
    geom_line() +
    geom_vline(xintercept = 0, color = 'red') +
    facet_wrap(~ treat) # Creates a separate panel for each unique value in 'treat'


p2 = ggplot(dt_long[Loc %in% c('Cong', 'SP') & event == 'min'], aes(x = rel_time, y = log(Vol), color = Loc)) +
    theme_minimal() +
    geom_line() +
    geom_vline(xintercept = 0, color = 'red') +
    facet_wrap(~ treat) # Creates a separate panel for each unique value in 'treat'








