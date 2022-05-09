## Get MTA Turnstiles Data
## 2022-04-28
## renata gerecke

# libraries ------------------

library(tidyverse)
library(lubridate)
library(scales)
library(rvest)
library(here)

# data -----------------------

## data comes from
## http://web.mta.info/developers/turnstile.html

mta_base_url <- "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"


tictoc::tic()
tibble(date = seq.Date(mdy("01012022"), mdy("04232022"), by = "week")) %>%
  mutate(date_fmt = label_date(format = "%y%m%d")(date),
         url = str_c(mta_base_url, date_fmt, ".txt")) %>%
  pmap(~download.file(..3, here("data", str_c(..2, ".csv"))))
tictoc::toc()
# 6 minutes

tictoc::tic()
tibble(date = seq.Date(mdy("01022021"), mdy("12252021"), by = "week")) %>%
  mutate(date_fmt = label_date(format = "%y%m%d")(date),
         url = str_c(mta_base_url, date_fmt, ".txt")) %>%
  pmap(~download.file(..3, here("data", str_c(..2, ".csv"))))
tictoc::toc()
# 18 minutes

tictoc::tic()
tibble(date = seq.Date(mdy("01042020"), mdy("12262020"), by = "week")) %>%
  mutate(date_fmt = label_date(format = "%y%m%d")(date),
         url = str_c(mta_base_url, date_fmt, ".txt")) %>%
  pmap(~download.file(..3, here("data", str_c(..2, ".csv"))))
tictoc::toc()
# 37 minutes wow lol it's bc i did 2020 and 2021 over again

tictoc::tic()
tibble(date = seq.Date(mdy("01052019"), mdy("12282019"), by = "week")) %>%
  mutate(date_fmt = label_date(format = "%y%m%d")(date),
         url = str_c(mta_base_url, date_fmt, ".txt")) %>%
  pmap(~download.file(..3, here("data", str_c(..2, ".csv"))))
tictoc::toc()
# 18 minutes


tictoc::tic()
tibble(date = seq.Date(mdy("01062018"), mdy("12292018"), by = "week")) %>%
  mutate(date_fmt = label_date(format = "%y%m%d")(date),
         url = str_c(mta_base_url, date_fmt, ".txt")) %>%
  pmap(~download.file(..3, here("data", str_c(..2, ".csv"))))
tictoc::toc()
