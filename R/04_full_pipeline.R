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
latest_date <- floor_date(Sys.Date(), "week", 6) - weeks(1)
latest_date_fmt <- label_date(format = "%y%m%d")(latest_date)
latest_date_url <- str_c(mta_base_url, latest_date_fmt, ".txt")

read_csv(latest_date_url, show_col_types = FALSE, col_types = "cccccccccdd") %>%
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

### station counts ------------------------------------------------
yr <- "2022"
yr_outfile <- str_c(yr, "_station_counts.csv")

df <- map_dfr(
  dir_ls(path("data", yr)),
  vroom, show_col_types = FALSE, .id = "url"
)

### station info --------------------------------------------------
df_stations <- read_csv(path("data", "stations.csv"))
df_stations_conversion <- read_csv(path("data", "station_conversion.csv"))

## munge daily data -----------------------------------------------

df_daily <- select(df, id, datetime, entries) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries),
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup()

## identify new turnstiles ------------------------------------------

df_stations_new <- distinct(
    df, id,
    station,
    linename
  ) %>%
  anti_join(df_stations, by = "id") %>%
  left_join(df_stations_conversion, by = c("station", "linename")) %>%
  transmute(id, station = coalesce(station_new, station),
            linename = coalesce(linename_new, linename),
            lat = Latitude, lon = Longitude)

df_stations <- bind_rows(df_stations, df_stations_new) %>%
  write_csv(path("data", "stations.csv"))


## categorize turnstiles ------------------------------------------

df_ts_easy <- group_by(df_daily, id) %>%
  filter(n() >= 5) %>%
  ungroup() %>%
  # identify most common nonzero sign direction
  mutate(sign = sign(d_entries)) %>%
  count(id, sign) %>%
  filter(sign != 0) %>%
  group_by(id) %>%
  filter(n == max(n)) %>%
  filter(n() == 1) %>%
  ungroup()

### uncategorized turnstiles -------------------------------------

df_ts_med <- anti_join(df_daily, df_ts_easy, by = "id") %>%
    mutate(sign = sign(d_entries)) %>%
    group_by(id) %>%
    filter(n() >= 5) %>%
    summarise(
      sign_0 = sum(sign == 0, na.rm = TRUE),
      sign_p = sum(sign == 1, na.rm = TRUE),
      sign_n = sum(sign == -1, na.rm = TRUE),
      med = median(d_entries, na.rm = TRUE),
      avg = mean(d_entries, na.rm = TRUE),
      min = min(d_entries, na.rm = TRUE),
      max = max(d_entries, na.rm = TRUE)
    )


## daily entries by turnstile ------------------------------------

df_ts_daily_entries <- left_join(df_ts_easy, df_daily, by = "id") %>%
  filter(sign * d_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)

## weekday pm entries by station --------------------------------------

tictoc::tic()
df_sta_weekly_entries <- filter(
  df_ts_daily_entries,
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
  left_join(df_stations, by = "id") %>%
  group_by(linename, station, weekdate, time) %>%
  summarise(entries = sum(d_entries),
            .groups = "drop") %>%
  pivot_wider(names_from = time,
              values_from = entries) %>%
  write_csv(path("output", yr_outfile), na = "")
tictoc::toc()

## EDITS FOR NEXT WEEK
## - CONSIDER SHIFTING TIME WINDOW BY 2 HOURS TO CAPTURE MORE ACCURATE WEEKEND
## - CAN WE REDUCE NUMBER OF REDUNDANT CALCULATIONS WEEK-ON-WEEK?
## - MERGE CURRENT DATA W/ BASELINE DATA
## - RERUN CALCS ON BASELINE DATA
## - PUSH TO CARTO W/ API
## - SEND EMAIL WITH VALIDATIONS
## - GET ON REMOTE SERVER
