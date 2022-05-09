## 2022 turnstile data
## 2022-05-04
## renata gerecke



## --- setup -------------------------------------------------------------

library(tidyverse)
library(fs)
library(lubridate)


## load 2022 raw ---------------------------------------------------------

yr <- "2020"
yr_outfile <- str_c(yr, "_station_counts.csv")

df <- map_dfr(
  dir_ls(path("data", yr)),
  vroom::vroom, show_col_types = FALSE, col_types = "cccccccccdd", .id = "url"
) %>%
  set_names(str_to_lower(names(.))) %>%
  rename(c_a = `c/a`)



## initial cleaning ----------------------------------------------------------


df_daily <- df %>%
  filter(!(division %in% c("PTH", "RIT"))) %>%
  transmute(
  id = str_c(c_a, unit, scp, sep = "_"),
  datetime = str_c(date, time, sep = " ") %>% mdy_hms,
  entries
  ) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries),
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup()



df_stations <- read_csv(path("data", "stations.csv"))


df_sta_lat<- read_csv(path("data", "station_conversion.csv"))

df_stations_new <- df %>%
  filter(!(division %in% c("PTH", "RIT"))) %>%
  distinct(
    c_a, unit, scp,
    station,
    linename
  ) %>%
  mutate(id = str_c(c_a, unit, scp, sep = "_"),
         c_a = NULL, unit = NULL, scp = NULL) %>%
  anti_join(df_stations, by = "id") %>%
left_join(df_sta_lat, by = c("station", "linename")) %>%
transmute(id, station = coalesce(station_new, station),
          linename = coalesce(linename_new, linename),
       lat = Latitude, lon = Longitude)

bind_rows(df_stations, df_stations_new) %>%
  write_csv(path("data", "stations.csv"))



## establish valid turnstiles -----------------------------------------------


# determine threshold for removing turnstiles due to low frequency

# count(df_daily, id) %>%
#   ggplot(aes(x = n)) +
#   stat_ecdf(geom = "step") +
#   theme_bw()

# remove turnstiles w/ fewer than 300 observations

df_enter_easy <- group_by(df_daily, id) %>%
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



# df_enter_med <- anti_join(df_daily, df_enter_easy, by = "id") %>%
#     mutate(sign = sign(d_entries)) %>%
#     group_by(id) %>%
#     filter(n() >= 5) %>%
#     summarise(
#       sign_0 = sum(sign == 0, na.rm = TRUE),
#       sign_p = sum(sign == 1, na.rm = TRUE),
#       sign_n = sum(sign == -1, na.rm = TRUE),
#       med = median(d_entries, na.rm = TRUE),
#       avg = mean(d_entries, na.rm = TRUE),
#       min = min(d_entries, na.rm = TRUE),
#       max = max(d_entries, na.rm = TRUE)
#     )

# all other turnstiles have insufficient info to assign a value (or include in our data set)


## calculate entries by turnstile ----------------------------------------
df_ts_daily_entries <- left_join(df_enter_easy, df_daily, by = "id") %>%
  mutate(norm_entries = sign * d_entries) %>%
  filter(norm_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)



## calculate entries by station --------------------------------------------

df_sta_weekly_entries <- mutate(
  df_ts_daily_entries,
  weekdate = floor_date(datetime, unit = "week", week_start = 1),
  hr = hour(datetime)
) %>%
  filter(hr %in% c(8:11, 18:21)) %>%
  mutate(
  time = ifelse(hr <= 11, "am", "pm"),
  weekday = ifelse(wday(datetime, week_start = 1) %in% 1:5, "weekday", "weekend")
) %>%
  filter(!is.na(time)) %>%
  left_join(df_stations, by = "id") %>%
  group_by(linename, station, weekdate, time, weekday) %>%
  summarise(entries = sum(d_entries),
            .groups = "drop") %>%
  pivot_wider(names_from = c(weekday, time),
              values_from = entries) %>%
  write_csv(path("output", yr_outfile))


