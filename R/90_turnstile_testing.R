## investigate turnstile directions
## 2022-05-10
## renata gerecke

# libraries ----------------------------


library(tidyverse)
library(lubridate)
library(fs)
library(scales)
library(vroom)
library(janitor)

# data ---------------------------------

df <- map_dfr(
  dir_ls(path("data", "2022")),
  ~ vroom(., show_col_types = FALSE),
  .id = "path"
)


df_daily <- arrange(df, id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = entries - lag(entries)) %>%
  ungroup()


%>%
  left_join(select(df_ts, id, sign), by = "id") %>%
  filter(sign * d_entries >= 0) %>%
  arrange(id, datetime) %>%
  group_by(id) %>%
  mutate(d_entries = (entries - lag(entries)) * sign,
         d_time    = (datetime %--% lag(datetime)) %/% hours()) %>%
  ungroup() %>%
  filter(d_entries < 10000, d_entries >= 0, d_time >= -12)


df_ts_dir <- group_by(df_daily, id, path) %>%
  filter(n() >= 5) %>%
  ungroup() %>%
  # identify most common nonzero sign direction
  mutate(sign = sign(d_entries)) %>%
  count(id, path, sign) %>%
  group_by(id, path) %>%
  filter(n == max(n))


  pivot_wider(names_from = sign, values_from = n)



  filter(sign != 0) %>%
  group_by(id) %>%
  filter(n == max(n)) %>%
  filter(n() == 1) %>%
  ungroup()


prob_turnstiles <- c(
  "B019_R149_00-00-01",
  "H007A_R248_02-03-00",
  "N329_R201_00-03-03",
  "R232A_R176_03-06-00",
  "R249_R179_01-00-05",
  "R401_R445_00-00-00",
  "R412_R146_00-00-00"
)

df_daily %>% filter(id == prob_turnstiles[[2]]) %>% View


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
