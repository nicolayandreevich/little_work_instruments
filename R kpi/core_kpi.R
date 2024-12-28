#' adjustments
get_max_value <- cpkpisql:::get_max_value
get_period_before <- cpkpisql:::get_period_before
agg_weight_wave <- function(weights_total, period) {
  length_in_months <- interval(period$start, period$end) %/% months(1)

  # get latest rwbasis:
  db_fltd <- weights_total |>
    filter(dt_start >= period$start,
           dt_end < period$end) |>
    compute()

  db_rwbasis_last <-
    db_fltd |>
    group_by(hhkey) |>
    filter(dt_start == max(dt_start)) |>
    select(hhkey, rwbasis)

  # keep only one-month periods:
  db_ww <- db_fltd |>
    group_by(hhkey) |>
    summarise(
      weight_wave = sum(continuo) / length_in_months) |>
    left_join(db_rwbasis_last, by = "hhkey")

  db_ww
}

get_trips <- function(data, occaskey, rwcompen, fullmasw,  weight_wave, projectf, scale = 1e3, .add = TRUE) {
  trips <-
    data |>
    # NB workes for grouped datasets as well
    group_by({{ occaskey}}, .add = .add) |>
    summarise_at(vars({{ rwcompen }}, {{ fullmasw }}, {{ weight_wave}}), mean) |>
    summarise(
      trips_raw = n(),
      trips_wave = sum({{ weight_wave }} * {{ projectf }} / scale),
      trips_fullmass = sum({{ fullmasw }} * {{ rwcompen }} * {{ projectf }} / scale)
    ) |>
    collect()

  return(trips)
}

get_buyers_wave <- function(data, hhkey, weight_wave, projectf, scale = 1e3, .add = TRUE) {
  # NB originally the projectf was used.
  # projectc used instead, since it's single value per hhkey
  buyers_wave <- data |>
    # workaround to ensure single value per hhkey
    group_by({{ hhkey }}, .add = .add) |>
    summarise_at(vars({{ weight_wave }}), mean) |>
    # get weight weighted buyers
    summarise(
      buyers_raw  = n(),
      buyers_wave = sum({{ weight_wave }} * {{ projectf }} / scale)
    ) |>
    collect()
}

#' Generate KPI table for view
#'
#' @param db_purch
#' @param db_wt
#'
#' @return
#' @export
#'
#' @examples
get_core_kpi_v2 <- function(db_grouped, db_population, projectc = 2728.03,
                            scale = 1e3) {

  population_df <- db_population |> collect()
  population_value = sum(population_df$population)

  # volumetric:
  kpi_volumetric <- db_grouped |>
    # get_volumetric_su(valuetot, volumeto, number, rwcompen, fullmasw, projectc, scale = 1e3)
    get_volumetric(valuetot, volumeto, number, rwcompen, fullmasw, projectc, scale = 1e3)

  # if kpi volumetric is null (empty df) return null
  if (nrow(kpi_volumetric) == 0) {
    return(NULL)
  }

  # trips
  kpi_trips_rwbasis <- db_grouped |>
    mutate(id_trip = sql("hashtext(concat(hhkey::text, movedate::text, id_shop::text))::integer")) |>
    group_by(rwbasis, .add = TRUE) |>
    get_trips(id_trip, rwcompen, fullmasw, weight_wave, projectc)


  # Since trips are additive, we can just sum them up
  kpi_trips <- kpi_trips_rwbasis |>
    summarise_if(is.numeric, sum) |>
    # only req. for penetration correction
    select(-trips_wave)

  # buyers:
  buyers_wave_rwbasis <- db_grouped |>
    group_by(rwbasis, .add = TRUE) |>
    get_buyers_wave(hhkey, weight_wave, projectc)
  buyers_raw <- buyers_wave_rwbasis |> summarise_at("buyers_raw", sum)

  kpi_buyers <- get_buyers_rp(
    buyers_wave_rwbasis,
    kpi_trips_rwbasis,
    population_df
  )

  # value / volume
  if (nrow(kpi_volumetric) == 1) {
  kpi_df <-
    list(kpi_volumetric,
         kpi_trips,
         kpi_buyers,
         buyers_raw) |>
    bind_cols() |>
    mutate(population = population_value) |>
    # convert to R numeric format representation
    mutate_if(is.numeric, as.double)
  }else{
    kpi_df <-
      kpi_volumetric |>
      left_join(kpi_trips) |>
      left_join(kpi_buyers) |>
      left_join(buyers_raw) |>
      ungroup() |>
      mutate(population = population_value) |>
      # convert to R numeric format representation
      mutate_if(is.numeric, as.double)
  }

  kpi_df
}


#' Calculate KPIs
#'
#' @param con connection to the database
#' @param panel_id Panel id for weights
#' @param schema_axsm Schema for AXS-M
#' @param schema_axss Schema for AXS-S (optional). NULL by default
#' @param period_end Use first day of month in the end of the period, e.g. "2024-04-01"
#' @param duration How many months should be taken retrospectively
#' @param positions Number of positions for calculation, 1+
#'
#' @return Data frame with calculations
#' @export
#'
#' @examples
fetch_kpi_table <- function(con,
                            panel_id = 1,
                            period_end = ymd("2024-03-01"),
                            axss_view = NULL,
                            schema_flth = NULL,
                            duration = 1,
                            def_packed = packed_full,
                            def_prodgr = prod_groups_fmcg_pepsico,
                            db_article_to_add = NULL
                            ) {

  # Set timer
  start = Sys.time()
  print(sprintf("Start KPIs calculations %s", start))

  ## Period length -----------------------------------------------------------

  # fetch filtered view for purchase data
  period <- list(
    start = period_end %m-% months(duration - 1),
    end = period_end %m+% months(1)
  )

  ## Schema def --------------------------------

  schema_wt = in_schema("cps", "hh_weights_monthly_pet")

  # default schemas
  schema_move <- in_schema("cps", "movements_pet")
  schema_move_panel <- in_schema("cps", "movements_panel_pet")
  schema_projection <- in_schema("cps", "ctlg_panels_pet")

  schema_article <- in_schema("cps", "ctlg_article_pet")
  schema_prodgr <- in_schema("cps", "art_product_groups_pet")


  ## load data -----------------------------------------

  print(sprintf("Load the data ..."))

  # factors & waves
  db_move <- con |>
    tbl(schema_move) |>
    filter(is.na(recordau)) |>
    select(
      id_rec,
      movedate,
      hhkey,
      article,
      id_shop,
      valuetot,
      volumeto,
      number
    )

  db_move_panel <- con |> tbl(schema_move_panel) |>
    filter(id_panel == panel_id) |>
    select(
      id_panel,
      id_rec,
      movedate,
      rwbalanc,
      rwcompen
    )

  db_wt <- con |>
    tbl(schema_wt) |>
    filter(
      id_panel == panel_id,
      dt_start >= period$start,
      dt_start < period$end
    ) |>
    # FIXME id_panel column requires
    select(
      dt_start,
      dt_end,
      hhkey,
      rwbasis,
      continuo
    ) |>
    compute(index = c("hhkey", "dt_start", "dt_end"))

  # FLT-H -------------------------------------------------------------
  # NB: use fullmass to identify end of period
  # household features will be taken as they are for the last weighting period
  if (!is.null(schema_flth)) {
    msg <- sprintf("Prepare FLT-H ...")
    print(msg)
    # setProgress(.15, message = msg)

    db_hh_available <- db_wt |>
      group_by(hhkey) |>
      # find last reporting year for each hh
      summarise(dt_year = to_date(as.character(max(dt_start)), 'yyyy-01-01'))

    db_flth <- con |>
      tbl(schema_flth) |>
      semi_join(db_hh_available, by = join_by(hhkey, dt_year)) |>
      select(id_flt, hhkey) |>
      compute()
  }


  # Projection
  db_projection <- con |> tbl(schema_projection)
  projectc <- db_projection |>
    filter(id_panel == panel_id) |>
    collect() |>
    pull(projectc)

  # Article fltr: --------------------------

  if (is.null(db_article_to_add)) {
    db_article <- con |>
      tbl(schema_article) |>
      filter(imputati %in% def_packed) |>
      select(id_pg, article, reporting_from, to_date) |>
      left_join(tbl(con,
                    schema_prodgr) |>
                  select(id_pg, product_group),
                by = "id_pg") |>
      filter(product_group %in% def_prodgr)
  }else{
    db_article <- db_article_to_add
  }


  ## Weight wave --------------------------

  msg <- sprintf(sprintf("Get wave weights ..."))
  print(msg)
  # setProgress(.2, message = msg)

  # get ww and population. If flth is specified
  if (is.null(schema_flth)) {
    db_ww <- db_wt |>
      agg_weight_wave(period) |>
      compute()
  } else {
    db_ww <- db_wt |>
      semi_join(db_flth, by = join_by(hhkey)) |>
      agg_weight_wave(period) |>
      compute()
  }

  msg <- sprintf("Get Population ...")
  print(msg)
  # setProgress(.25, message = msg)

  population_df <- db_ww |>
    group_by(rwbasis) |>
    get_population(weight_wave, projectc)



  ## Merge together --------------------------------------------------------

  if (is.null(axss_view)) {

    db_calc <- db_move |>
      left_join(db_move_panel, by = join_by(movedate, id_rec)) |>
      filter(movedate >= period$start,
             movedate < period$end) |># filter period
      inner_join(db_article,
                 join_by(article,
                         between(movedate, reporting_from, to_date,
                                 bounds = "[]"))) |>
      # add fullmass:
      add_fullmass(db_wt) |>
      # add weight waves
      inner_join(db_ww, by = c("hhkey"))

  } else{

    db_calc <- db_move |>
      left_join(db_move_panel, by = join_by(movedate, id_rec)) |>
      filter(movedate >= period$start,
             movedate < period$end) |># filter period
      inner_join(db_article,
                 join_by(article,
                         between(movedate, reporting_from, to_date,
                                 bounds = "[]"))) |>
      inner_join(tbl(con_pet, axss_view),
                 by = join_by(id_shop)) |>
      # add fullmass:
      add_fullmass(db_wt) |>
      # add weight waves
      inner_join(db_ww, by = c("hhkey"))


  }




  # Calculate KPIs: ---------------------------------

  t_interim <- Sys.time()
  print(sprintf("Get core KPIs... %s", t_interim - start))

  if (is.null(axss_view)) {

  kpi_df <- db_calc |>
      get_core_kpi_v2(population_df, projectc, scale = 1e3)
  } else{
    kpi_df <- db_calc |>
      group_by(id_axis, position_number, autolabel) |>
      get_core_kpi_v2(population_df, projectc, scale = 1e3)


  }


  end = Sys.time()

  # NB: kpi_df can be NULL in the end

  print("Completed!")
  print(end - start)

  kpi_df
}

add_fullmass <- function(db, db_wt) {
  db |>
    # NB fullmass defines panel, therefore it's always inner join
    inner_join(db_wt |>
                 # mutate(dt_end = dt_start + months(1) - days(1)) |>
                 select(hhkey, dt_start, dt_end, fullmasw = continuo),
               join_by(hhkey, between(movedate, dt_start, dt_end))) |>
    select(-dt_end, -dt_start)
}


add_kpi_labels <- function(kpi_df, con, schema_axisdata, position_name,
                           id_axis, position_number, autolabel, value_group_name,
                           level, hide_concat) {

  # collect data from axis data registry
  axis_id <- kpi_df |> distinct({{ id_axis }}) |> pull(1)
  db_axis <- get_axis_data(con, schema_axisdata, axis_id) |>
    # workaround to have consistent names
    rename({{ id_axis }} := id_axis,
           {{ position_number }} := position_number,
           {{ value_group_name }} := value_group_name,
           {{ level }} := level,
           {{ hide_concat }} := hide_concat)

  # add labels to the kpi_df
  kpi_df |>
    left_join(db_axis,
              by = join_by({{ id_axis }}, {{ position_number }})) |>
    # if autolabel != "" replace value_group_name with it
    mutate({{ position_name }} := ifelse({{ autolabel }} == "",
                                         {{ value_group_name }},
                                         {{ autolabel }})) |>
    select(-{{ value_group_name }}, -{{ autolabel }}) |>
    # rearrange cols
    select({{ id_axis }}, {{ position_number }}, {{ level }},
           {{ hide_concat }}, {{ position_name }}, everything())

}


get_axis_data <- function(con, schema_axisdata, axis_id) {
  con |>
    tbl(schema_axisdata) |>
    filter(id_axis %in% axis_id) |>
    select(id_axis, position_number, level, hide_concat, value_group_name) |>
    collect()
}


add_derivative_kpi <- function(kpi_df, scale = 1e3) {
  kpi_df |>
    rename(buyers = buyers_rp,
           trips = trips_fullmass) |>
    mutate(penetration = buyers / population * 100,
           frequency = trips / buyers) |>
    # derivative KPI
    mutate(
      spend_per_buyer = value / buyers,
      spend_per_trip = value / trips,
      volume_per_buyer_g = volume_gl / buyers / scale * 1e3,
      volume_per_buyer_pack = volume_pack / buyers,
      volume_per_trip_g = volume_gl / trips / scale  * 1e3, # per kg
      volume_per_trip_pack = volume_pack / trips,
      avg_price_g = value / volume_gl * scale / 1e3, # per kg
      avg_price_pack = value / volume_pack,
      avg_pack_size = volume_gl / volume_pack / scale  * 1e3
    )
}


