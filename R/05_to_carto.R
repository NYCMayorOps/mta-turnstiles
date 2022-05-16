## export to carto experiments
## 2022-05-13
## renata gerecke

# post to carto --------------------------------------------------------

library(httr)

carto_user <- "rgerecke"
carto_key <- "789f8a11c0c72e3891ac69f1044bcd335cfe9e18"

carto_url <- stringr::str_glue("https://{carto_user}.carto.com/api/v1/imports/?api_key={carto_key}&collision_strategy=overwrite")

test_df <- readr::read_csv(fs::path("output", "2022_vs_pandemic_baseline.csv"),
                           na = "NULL")

readr::write_csv(dplyr::slice_sample(test_df, n = 2000),
            fs::path("output", "baseline_2022_delta_station.csv"))


test <- POST(
  carto_url,
  body = list(
    file = upload_file(fs::path("output", "baseline_2022_delta_station.csv"))
  )
)

