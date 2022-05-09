## ----setup, include=FALSE-----------------------------------------------
knitr::opts_chunk$set(echo = TRUE)
library(tidyverse)
library(fs)
library(lubridate)


## -----------------------------------------------------------------------
df <- map_dfr(
  dir_ls(path("..", "data"), regexp = "200[1-6](..)\\.csv"), 
  vroom::vroom, show_col_types = FALSE, col_types = "ccccccccccc", .id = "url"
) %>%
  set_names(str_to_lower(names(.))) %>%
  rename(c_a = `c/a`)

# %>%
#   clean_names()


## -----------------------------------------------------------------------
df_daily <- df %>%
  filter(!(division %in% c("PTH", "RIT"))) %>%
  mutate(
  id = str_c(c_a, unit, scp, sep = "_"),
  datetime = str_c(date, time, sep = " ") %>% mdy_hms,
  entries = parse_number(entries),
  exits = parse_number(exits)
  ) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries),
         d_exits   = exits   - lag(exits),
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup()


## -----------------------------------------------------------------------
# determine threshold for removing turnstiles due to low frequency

# count(df_daily, id) %>%
#   ggplot(aes(x = n)) + 
#   stat_ecdf(geom = "step") + 
#   theme_bw()

# remove turnstiles w/ fewer than 300 observations
df_enter_easy <- group_by(df_daily, id) %>%
  filter(n() >= 300) %>%
  ungroup() %>%
  # identify most common nonzero sign direction
  mutate(sign = sign(d_entries)) %>% 
  count(id, sign) %>%
  filter(sign != 0) %>%
  group_by(id) %>%
  filter(n == max(n)) %>%
  filter(n() == 1) %>%
  ungroup()



# df_enter_med <- filter(df_daily, !(id %in% df_enter_easy$id)) %>%
#     mutate(sign = sign(d_entries)) %>% 
#     group_by(id) %>%
#   filter(n() >= 300) %>%
#     summarise(
#       sign_0 = sum(sign == 0, na.rm = TRUE),
#       sign_p = sum(sign == 1, na.rm = TRUE),
#       sign_n = sum(sign == -1, na.rm = TRUE),
#       med = median(d_entries, na.rm = TRUE),
#       avg = mean(d_entries, na.rm = TRUE),
#       min = min(d_entries, na.rm = TRUE),
#       max = max(d_entries, na.rm = TRUE)
#     ) %>%
#   filter(min != max)

# all other turnstiles have insufficient info to assign a value (or include in our data set)


## -----------------------------------------------------------------------
df_agg_enter <- left_join(df_enter_easy, df_daily, by = "id") %>%
  mutate(norm_entries = sign * d_entries) %>%
  filter(norm_entries >= 0) %>% 
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12) %>%
  mutate(
    hr = hour(datetime),
    am = between(hr, 8, 11),
    pm = between(hr, 18, 21),
    weekday = wday(datetime) %in% 2:6
  ) %>% 
  filter(am | pm)
  


## -----------------------------------------------------------------------
station_enter <- df_agg_enter %>% 
  group_by(station, am, pm, weekday) %>%
  summarise(entries = sum(d_entries),
            .groups = "drop") %>%
  mutate(time = map2_chr(am, pm, ~case_when(
    .x == TRUE ~ "am",
    .y == TRUE ~ "pm",
    TRUE ~ NA_character_
  )),
  weekday = ifelse(weekday, "weekday", "weekend"),
  weekend = NULL, am = NULL, pm = NULL) %>%
  pivot_wider(
    names_from = c(weekday, time),
    values_from = entries
  )



## -----------------------------------------------------------------------
df_sample <- df_daily %>%
  filter(
  c_a == "PTH03",
  unit == "R552",
  scp == "00-01-08"
) %>%
  # filter(second(datetime) %in% c(7, 34)) %>%
  mutate(d_entries = entries - lag(entries),
         d_exits   = exits   - lag(exits),
         d_time    = (datetime %--% lag(datetime)) %/% hours())



## -----------------------------------------------------------------------
df_sample2 <- filter(df_daily, id == "N215_R237_00-00-02")


## -----------------------------------------------------------------------
df_daily %>%
  group_by(id) %>%
  summarise(med_entries = median(d_entries, na.rm = TRUE),
            med_exits   = median(d_exits, na.rm = TRUE),
            .groups = "drop") %>%
  slice_max(med_entries, n = 10)


## -----------------------------------------------------------------------
df_daily %>%
  group_by(id) %>%
  summarise(med_entries = median(d_entries, na.rm = TRUE),
            med_exits   = median(d_exits, na.rm = TRUE),
            .groups = "drop") %>%
  slice_min(med_entries, n = 10)


## -----------------------------------------------------------------------
df_daily %>%
  count(id) %>%
  arrange(n)

