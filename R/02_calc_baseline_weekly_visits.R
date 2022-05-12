## 2022 turnstile data
## 2022-05-04
## renata gerecke



## --- setup -------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(fs)
library(scales)
library(vroom)
library(tictoc)


## load 2022 raw ---------------------------------------------------------

yr <- "pan_baseline"
yr_outfile <- str_c(yr, "_station_counts.csv")

df <- map_dfr(
  dir_ls(path("data", yr)),
  vroom, show_col_types = FALSE, .id = "url"
)

## read data ------------------------------------------------------

df_old_ts <- read_csv(path("output", "turnstiles", "2022-04-30.csv"))
df_stations <- read_csv(path("data", "stations.csv"))
toc(log = TRUE, quiet = TRUE)

# daily turnstile counts ---------------------------------------------------

df_daily_raw <- select(df, url, id, datetime, entries) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries)) %>%
  ungroup()



## get turnstile directions -----------------------------------------------


df_ts_dir <- group_by(df_daily_raw, url, id) %>%
  filter(n() >= 5) %>%
  ungroup() %>%
  # identify most common nonzero sign direction
  mutate(sign = sign(d_entries)) %>%
  filter(sign != 0) %>%
  count(url, id, sign) %>%
  group_by(url, id) %>%
  filter(n == max(n)) %>%
  select(-n)



## save turnstile database ------------------------------------------------

df_ts <- group_by(df_daily_raw, id) %>%
  filter(datetime == max(datetime)) %>%
  ungroup() %>%
  full_join(
    select(df_old_ts, id, station, linename, lon, lat),
    by = "id"
  ) %>%
  {
    if (nrow(.) > nrow(df_old_ts)) {
      old_ts <- filter(., !is.na(station))

      new_ts <- filter(., is.na(station)) %>%
        select(id, datetime, entries) %>%
        left_join(distinct(df_latest, id, station, linename), by = "id") %>%
        left_join(df_stations, by = c("station", "linename"))
      transmute(
        id, datetime, entries,
        station = coalesce(station_new, station),
        linename = coalesce(linename_new, linename),
        lon = Longitude,
        lat = Latitude
      )

      bind_rows(old_ts, new_ts)
    } else {
      select(., -d_entries)
    }
  }


## finish turnstile counts ------------------------------------------------


df_daily <- left_join(
  df_daily_raw,
  df_ts_dir,
  by = c("url", "id")
) %>%
  filter(sign * d_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)




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
  mutate(week = week(weekdate)) %>%
  filter(complete.cases(.), weekdate <= ymd("2020-02-10")) %>%
  write_csv(path("output", "pan_baseline_station_counts.csv"), na = "NULL")
toc()

