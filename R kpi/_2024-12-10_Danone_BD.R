source("2024-12-10_core_kpi.R")
source("2024-12-18_penetration_correction.R")


library(tidyverse)
library(DBI)
library(dbplyr)
library(xlbox)
library(glue)

projectc <- 2728.031



# Connect to posgres server: --------------------------

init_connection <- function() {
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host = "10.10.0.111",
                        dbname = "pet",
                        user ="nyzhur", #Sys.getenv("nyzhur"),
                        password = "GfkPass121293" #Sys.getenv("GfkPass121293"))
  )

}


# technical pages
schema_cltg_axis <- in_schema("metadata", "ctlg_axis")
schema_axis_data <- in_schema("metadata", "axis_data")



# schemas: ---------------------------
client_schema <- "cmt_international_cluster_1"


schema_axsh <- in_schema(client_schema,
                         "axsh_hn_catman_socdem")


schema_axsm <- in_schema(client_schema,
                         "axsm_hn_catman_upd")

schema_axsa <- NULL#in_schema(client_schema,
                    #     "axsm_hn_catman_upd")

schema_axss <-  in_schema(client_schema,
                          "axss_hn_catman_channels")

# Period list: -----------------------

vctr_data_end <-                       list(
as.Date("2021-03-01"),as.Date("2021-06-01"), as.Date("2021-09-01"),as.Date("2021-12-01"),
as.Date("2022-03-01"),as.Date("2022-06-01"), as.Date("2022-09-01"),as.Date("2022-12-01"),
as.Date("2023-03-01"),as.Date("2023-06-01"), as.Date("2023-09-01"),as.Date("2023-12-01"),
as.Date("2024-03-01"),as.Date("2024-06-01"), as.Date("2024-09-01")
)


duration_touse <- list(12)

count =1

n <- length(vctr_data_end) * length(duration_touse)


for (el in vctr_data_end) {

  for (d in duration_touse) {
    file_path <- glue("DT_Danone/kpi_{d}_{el}.xlsx")
      period = list(end   = el %m+% months(1),
                  start = el %m-% months(d - 1)
                  )
    print(period)

    if (file.exists(file_path)) {
      print(paste(file_path, 'is EXIST!',sep =' '))
      count = count+1
      next } else {


      print(paste(file_path, count,'from',n,sep =' '))


    con_pet <- init_connection()

    fetch_kpi_table(
      con = con_pet,
      panel_id = 1,
      schema_axsm = schema_axsm,
      schema_axss = schema_axss,
      schema_axsa = NULL, #schema_axsa,
      schema_axsh = schema_axsh,
      schema_flth = NULL,
      period = period,
      positions = 1:1000,
      su_options = which_su_calc(FALSE,# что бы не считать
                                 "pgsufac1",
                                 "no_factor"),
      repeat_rate = TRUE) |>
      add_derivative_kpi() |>
      mutate(month_end = el,
             duration = d) |>
       add_kpi_labels(con = con_pet, schema_axisdata = schema_axis_data, "position_name_shop",
                      id_axis_shop, position_number_shop, autolabel_shop,
                      value_group_name_shop, level_shop, hide_concat_shop) |>
      #add_kpi_labels(con = con_pet, schema_axisdata = schema_axis_data, "position_name_art",
      #               id_axis_art, position_number_art, autolabel_art,
      #               value_group_name_art, level_art, hide_concat_art) |>
      add_kpi_labels(con = con_pet, schema_axisdata = schema_axis_data, "position_name_hh",
                     id_axis_hh, position_number_hh, autolabel_hh,
                     value_group_name_hh, level_hh, hide_concat_hh) |>
      add_kpi_labels(con = con_pet,
                     schema_axisdata = schema_axis_data,
                     "position_name",
                     id_axis, position_number, autolabel, value_group_name,
                     level, hide_concat) |>
      write_xlsx(glue(file_path))

    dbDisconnect(con_pet)
    count = count+1

  }

}

}

