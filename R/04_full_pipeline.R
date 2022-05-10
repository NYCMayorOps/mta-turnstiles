## full pipeline turnstile analysis
## 2022-05-06
## renata gerecke

# libraries --------------------------------

library(tidyverse)
library(lubridate)
library(fs)
library(scales)
library(vroom)

# data -------------------------------------



## download latest data ------------------------------------------
## updated weekly on saturdays

mta_base_url <- "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"
latest_date <- floor_date(Sys.Date(), "week", 6)
latest_date_fmt <- label_date(format = "%y%m%d")(latest_date)
latest_date_url <- str_c(mta_base_url, latest_date_fmt, ".txt")

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

df_ts <- read_csv(path("data", "turnstiles.csv"))
df_old_weekly <- read_csv(path("output", "2022_station_counts.csv"))


# new turnstiles? -----------------------------------------------


# TODO: Need `df_ts` to be complete object, if new rows are added
# TODO: If new rows are added, need to be able to assign them a sign
df_ts_new <- distinct(df_latest, id, station, linename) %>%
  anti_join(df_ts, by = "id") %>%
  {
    if (nrow(.) > 0) {
      # TODO: Add email component here
      df_stations <- read_csv(path("data", "station_conversion.csv"))

      left_join(
        .,
        df_stations,
        by = c("station", "linename")
      ) %>%
        transmute(id, station = coalesce(station_new, station),
                  linename = coalesce(linename_new, linename),
                  lat = Latitude, lon = Longitude) %>%
        bind_rows(df_ts) %>%
        write_csv(path("data", "turnstiles.csv"))
    } else {
      .
    }
  }


# daily turnstile counts ---------------------------------------------------

# TODO: Look into whether certain turnstiles swap their direction over time
#   - Should we be recalculating the direction each week?
#   - With what time window? (Ugh)
df_daily <- bind_rows(
  select(df_ts, id, datetime, entries),
  select(df_latest, id, datetime, entries)
) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries)) %>%
  ungroup() %>%
  left_join(select(df_ts, id, sign), by = "id") %>%
  filter(sign * d_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)


# weekly station counts ---------------------------------------------------

tictoc::tic()
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
  )
tictoc::toc()

tictoc::tic()
df_all_weekly <- full_join(
  df_new_weekly,
  df_old_weekly,
    by = c("linename", "station", "weekdate"),
    suffix = c(".new", ".old")
  ) %>%
  transmute(
    linename,
    station,
    weekdate,
    weekend = coalesce(weekend.new, weekend.old),
    weekday_am = coalesce(weekday_am.new, weekday_am.old),
    weekday_pm = coalesce(weekday_pm.new, weekday_pm.old)
  ) %>%
  write_csv(path("output", str_c("test", "2022_station_counts.csv")), na = "")
tictoc::toc()



## EDITS FOR NEXT WEEK
## - CONSIDER SHIFTING TIME WINDOW BY 2 HOURS TO CAPTURE MORE ACCURATE WEEKEND
##   - This is going to create a *lot* of issues with the timeframes of the
##     weekly upload; have to wait to calculate the last day of the week until
##     the following week? Feels unreasonable
## - CAN WE REDUCE NUMBER OF REDUNDANT CALCULATIONS WEEK-ON-WEEK?
## - MERGE CURRENT DATA W/ BASELINE DATA
## - RERUN CALCS ON BASELINE DATA
## - PUSH TO CARTO W/ API
## - SEND EMAIL WITH VALIDATIONS
## - GET ON REMOTE SERVER
