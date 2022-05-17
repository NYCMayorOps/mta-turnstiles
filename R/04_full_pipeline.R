## full pipeline turnstile analysis
## 2022-05-06
## renata gerecke

# libraries --------------------------------

library(tidyverse)
library(lubridate)
library(fs)
library(scales)
library(vroom)
library(tictoc)

# data -------------------------------------



## download latest data ------------------------------------------
## updated weekly on saturdays

tic("all")
mta_base_url <- "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"
latest_date <- floor_date(Sys.Date(), "week", 6)
latest_date_fmt <- label_date(format = "%y%m%d")(latest_date)
latest_date_url <- str_c(mta_base_url, latest_date_fmt, ".txt")

tic("load data")
df_latest <- read_csv(
  latest_date_url,
  show_col_types = FALSE,
  col_types = "cccccccccdd"
) %>%
  set_names(str_to_lower(names(.))) %>%
  rename(c_a = `c/a`) %>%
  filter(!(division %in% c("PTH", "RIT"))) %>%
  transmute(
    id = str_c(c_a, unit, scp, sep = "_"),
    datetime = mdy_hms(str_c(date, time, sep = " ")),
    station,
    linename,
    entries
  ) %>%
  arrange(id, datetime) %>%
  write_csv(path("data", "2022", str_c(latest_date_fmt, ".csv")))



## read data ------------------------------------------------------

df_old_ts <- read_csv(path("output", "turnstiles.csv"))
df_old_weekly <- read_csv(path("output", "current_station_counts"),
                          na = "NULL")
df_stations <- read_csv(path("data", "stations.csv"))
df_baseline <- read_csv(path("data", "baseline_station_counts.csv"))
toc(log = TRUE, quiet = TRUE)

# daily turnstile counts ---------------------------------------------------

tic("daily turnstile counts")
df_daily_raw <- bind_rows(
  select(df_old_ts, id, datetime, entries),
  select(df_latest, id, datetime, entries)
) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries)) %>%
  ungroup()



## get turnstile directions -----------------------------------------------


df_ts_dir <- group_by(df_daily_raw, id) %>%
  filter(n() >= 5) %>%
  ungroup() %>%
  # identify most common nonzero sign direction
  mutate(sign = sign(d_entries)) %>%
  filter(sign != 0) %>%
  count(id, sign) %>%
  group_by(id) %>%
  filter(n == max(n)) %>%
  select(-n)



## save turnstile database ------------------------------------------------

tic("turnstile db")
df_ts <- group_by(df_daily_raw, id) %>%
  filter(datetime == max(datetime)) %>%
  ungroup() %>%
  full_join(
    select(df_old_ts, id, station, linename),
    by = "id"
  ) %>%
  {
    if (nrow(.) > nrow(df_old_ts)) {
      ## TODO: email me if there are new turnstiles??
      old_ts <- filter(., !is.na(station))

      new_ts <- filter(., is.na(station)) %>%
        select(id, datetime, entries) %>%
        left_join(distinct(df_latest, id, station, linename), by = "id") %>%
        left_join(df_stations, by = c("station", "linename"))
        transmute(
          id, datetime, entries,
          station = coalesce(station_new, station),
          linename = coalesce(linename_new, linename)
        ) %>%
          write_csv(path("output", "turnstiles", str_c(latest_date, ".csv")))

      bind_rows(old_ts, new_ts)
    } else {
      select(., -d_entries)
    }
  } %>%
  write_csv(path("output", "turnstiles.csv"))
toc(log = TRUE, quiet = TRUE)

## finish turnstile counts ------------------------------------------------


df_daily <- left_join(
  df_daily_raw,
  df_ts_dir,
  by = "id"
) %>%
  filter(sign * d_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)
toc(log = TRUE, quiet = TRUE)



# weekly station counts ---------------------------------------------------

tic("latest weekly")
df_new_weekly <- filter(
  df_daily,
  hour(datetime) %in% c(8:11, 18:21) |
    wday(datetime) %in% c(1,7)
) %>%
  mutate(
    weekdate = floor_date(datetime, unit = "week", week_start = 1),
    time = map_chr(
      datetime,
      ~ case_when(
        wday(.) %in% c(1,7) ~ "weekend",
        hour(.) <= 11 ~ "weekday_am",
        hour(.) >  11 ~ "weekday_pm"
      )
    )
  ) %>%
  left_join(df_ts, by = "id") %>%
  group_by(linename, station, weekdate, time) %>%
  summarise(entries = sum(d_entries),
            .groups = "drop") %>%
  pivot_wider(
    names_from = time,
    values_from = entries
  ) %>%
  mutate(week = week(weekdate))
toc(log = TRUE, quiet = TRUE)

tic("combined weekly")
df_all_weekly <- full_join(
  df_new_weekly,
  df_old_weekly,
    by = c("linename", "station", "weekdate", "week"),
    suffix = c(".new", ".old")
  ) %>%
  transmute(
    linename,
    station,
    weekdate,
    week,
    weekend = coalesce(weekend.new, weekend.old),
    weekday_am = coalesce(weekday_am.new, weekday_am.old),
    weekday_pm = coalesce(weekday_pm.new, weekday_pm.old)
  ) %>%
  write_csv(path("output", "current_station_counts.csv"), na = "NULL")
toc(log = TRUE, quiet = TRUE)

# compare weekly to baseline ------------------------------------------

df_delta <- left_join(df_all_weekly, df_baseline,
          by = c("linename", "station", "week"),
          suffix = c("_2022", "_bl")) %>%
  mutate(
    d_weekend = (weekend_2022) / weekend_bl,
    d_weekday_am = (weekday_am_2022) / weekday_am_bl,
    d_weekday_pm = (weekday_pm_2022) / weekday_pm_bl
  ) %>%
  na_if(Inf) %>%
  left_join(df_stations, by = c("linename", "station")) %>%
  write_csv(path("output", "baseline_2022_station_delta.csv"), na = "")




toc(log = TRUE, quiet = TRUE)

tic.log(format = FALSE) %>%
  reduce(bind_rows) %>%
  mutate(d = toc - tic) %>%
  write_csv(path("output", "time-log", str_c(latest_date, ".csv")))


## EDITS FOR NEXT WEEK
## - CONSIDER SHIFTING TIME WINDOW BY 2 HOURS TO CAPTURE MORE ACCURATE WEEKEND
##   - This is going to create a *lot* of issues with the timeframes of the
##     weekly upload; have to wait to calculate the last day of the week until
##     the following week? Feels unreasonable
## - PUSH TO CARTO W/ API
## - SEND EMAIL WITH VALIDATIONS
