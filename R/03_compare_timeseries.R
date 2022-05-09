## Comparing w pre-pandemic baseline
## 2022-05-04
## renata gerecke

# libraries -----------------------------------------------

library(tidyverse)
library(fs)
library(lubridate)



# data ---------------------------------------------------

df_bl <- read_csv(path("output", "pan_baseline_station_counts.csv")) %>%
  filter(complete.cases(.)) %>%
  filter(weekdate <= ymd("2020-02-10")) %>%
  mutate(week = week(weekdate))

df_22 <- read_csv(path("output", "2022_station_counts.csv")) %>%
  filter(complete.cases(.), year(weekdate) == 2022) %>%
  mutate(week = week(weekdate))

df_stations <- read_csv(path("data", "stations.csv")) %>%
  distinct(linename, station, lat, lon)

# combine -----------------------------------------------

df_delta <- left_join(df_22, df_bl,
          by = c("linename", "station", "week"),
          suffix = c("_2022", "_bl")) %>%
  mutate(
    d_weekend_am = (weekend_am_2022) / weekend_am_bl,
    d_weekend_pm = (weekend_pm_2022) / weekend_pm_bl,
    d_weekday_am = (weekday_am_2022) / weekday_am_bl,
    d_weekday_pm = (weekday_pm_2022) / weekday_pm_bl
  ) %>%
  na_if(Inf) %>%
  left_join(df_stations, by = c("linename", "station")) %>%
  write_csv(path("output", "2022_vs_pandemic_baseline.csv"))


# map most recent delta ----------------------------------
