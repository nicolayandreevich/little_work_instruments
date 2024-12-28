#' adjustments
get_max_value <- get_max_value <- function(db, column) {
  db |> summarise(across({{ column }}, max)) |> collect() |>  pull(1)
}

get_period_before <- get_period_before <- function(period, length_in_months) {
  period %m-% months(length_in_months - 1)
}

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
get_core_kpi <- function(db_grouped,
                         db_ww,
                         db_axsh = NULL,
                         is_flth = FALSE,
                         repeat_rate = FALSE,
                         buyers_rp = TRUE,
                         projectc = 2728.03,
                         scale = 1e3) {

  is_axsh <- !is.null(db_axsh)
  # add AXS-H
  # we need total for pen. correction input
  # and merged data for 'regular' KPIs like volumetric
  if (is_axsh) {
    db_total <- db_grouped
    db_grouped <- db_grouped |>
      left_join(db_axsh, by = join_by(hhkey), suffix = c("", "_hh")) |>
      group_by(id_axis_hh, position_number_hh, autolabel_hh, .add = TRUE)
    db_ww_total <- db_ww
    db_ww <- db_ww |>
      left_join(db_axsh, by = join_by(hhkey), suffix = c("", "_hh")) |>
      rename_with(~sprintf("%s_hh", .x), c(id_axis, position_number, autolabel)) |>
      group_by(id_axis_hh, position_number_hh, autolabel_hh, .add = TRUE)
  } else if (is_flth) {
    db_total <- db_grouped
    db_grouped <- db_grouped |> filter(flth)
    db_ww_total <- db_ww
    db_ww <- db_ww |> filter(flth)
  }

  # volumetric: -----------------

  msg <- sprintf("Get Volumetric...")
  print(msg)
  # setProgress(.60, message = msg)

  kpi_volumetric <- db_grouped |>
    get_volumetric_su(valuetot, volumeto, number,
                      rwcompen, fullmasw, projectc, scale = 1e3)

  # if kpi volumetric is null (empty df) return null
  if (nrow(kpi_volumetric) == 0) {
    return(NULL)
  }

  # trips--------------------------

  msg <- sprintf("Get Trips...")
  print(msg)
  # setProgress(.70, message = msg)

  if (is_axsh || is_flth) {
    # total trips are required for pen.correction in case of AXS/FLT-H
    kpi_trips_rwbasis_total <- db_total |>
      group_by(rwbasis, .add = TRUE) |>
      get_trips(id_trip, rwcompen, fullmasw, weight_wave, projectc)

  }
  kpi_trips_rwbasis <- db_grouped |>
    group_by(rwbasis, .add = TRUE) |>
    get_trips(id_trip, rwcompen, fullmasw, weight_wave, projectc)

  # Since trips are additive, we can just sum them up
  kpi_trips <- kpi_trips_rwbasis |>
    summarise_if(is.numeric, sum) |>
    # only req. for penetration correction
    select(-trips_wave)

  # population ---------------------------

  msg <- sprintf("Get Population...")
  print(msg)
  # setProgress(.75, message = msg)
  if (is_axsh || is_flth) {
    # req. for AXS/FLT-H pen. corr.
    population_df_total <- db_ww_total |>
      group_by(rwbasis, .add = TRUE) |>
      get_population(weight_wave, projectc)
  }
  population_df <- db_ww |>
    group_by(rwbasis, .add = TRUE) |>
    get_population(weight_wave, projectc)

  # This value is the same for all positions
  population_value_df <- population_df |> summarise_at("population", sum)

  # buyers wave --------------------------

  msg <- sprintf("Got Buyers...")
  print(msg)
  # setProgress(.8, message = msg)

  if (is_axsh || is_flth) {
    buyers_wave_rwbasis_total <- db_total |>
      group_by(rwbasis, .add = TRUE) |>
      get_buyers_wave(hhkey, weight_wave, projectc)

  }
  buyers_wave_rwbasis <- db_grouped |>
    group_by(rwbasis, .add = TRUE) |>
    get_buyers_wave(hhkey, weight_wave, projectc)

  buyers_df <- buyers_wave_rwbasis |> summarise_at(c("buyers_raw", "buyers_wave"), sum)

  # buyers RP --------------------------

  msg <- sprintf("Apply pen. correction...")
  print(msg)
  # setProgress(.85, message = msg)

  if (is_axsh || is_flth) {
    # get pen. corr. for total values and use buyers share ww for filtered data
    kpi_buyers_total <- get_buyers_rp_correction(
      buyers_wave_rwbasis_total,
      kpi_trips_rwbasis_total,
      population_df_total
    )

    buyers_share <- buyers_wave_rwbasis_total |>
      rename(buyers_wave_total = buyers_wave,
             buyers_raw_total = buyers_raw) |>
      inner_join(buyers_wave_rwbasis |>
                   rename(buyers_wave_flt = buyers_wave,
                          buyers_raw_flt = buyers_raw)) |>
      # after flth some values can be NA, substitue 0 in this case
      mutate(buyers_wave_share = buyers_wave_flt / buyers_wave_total)

    kpi_buyers <- kpi_buyers_total |>
      inner_join(buyers_share) |>
      # adjust buyers
      mutate_at("buyers_rp", ~ . * buyers_wave_share) |>
      # replace total values with filtered:
      mutate(buyers_wave = buyers_wave_flt,
             buyers_raw = buyers_raw_flt) |>
      select(-ends_with(c("_total", "_flt")), -buyers_wave_share, -buyers_wave) |>
      # summarise
      group_by(id_axis_hh, position_number_hh, autolabel_hh, .add = TRUE) |>
      summarise_at("buyers_rp", sum)
  } else {
    kpi_buyers <- get_buyers_rp_correction(
      buyers_wave_rwbasis,
      kpi_trips_rwbasis,
      population_df
    ) |>
      summarise_at("buyers_rp", sum)
  }

  # merge all KPIs together
  kpi_df <-
    kpi_volumetric |>
    left_join(kpi_trips) |>
    left_join(kpi_buyers) |>
    left_join(buyers_df |> select(-buyers_wave)) |>
    ungroup() |>
    # workaround use tmp variable to attach population
    mutate(tmp = 1) |>
    left_join(population_value_df |> mutate(tmp = 1)) |>
    select(-tmp) |>
    # convert to R numeric format representation
    mutate_if(is.numeric, as.double)

  # repeat rate [cs] ---------------------------
  if (repeat_rate) {
    repeat_rate <- db_grouped |> get_repeat_rate(hhkey, continuo, projectc)
    kpi_df <- kpi_df |> left_join(repeat_rate)

    msg <- sprintf("Got Repeat Rate...")
    print(msg)
    # setProgress(.9, message = msg)
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
                            schema_axsm = in_schema("cmt_international_cluster_1", "axsm_colgate_sg_kpi_report_2024"),
                            schema_axsa = NULL,
                            schema_axss = NULL,
                            schema_axsh = NULL,
                            schema_flth = NULL,
                            period = list(start = ymd("2024-03-01"),
                                          end   = ymd("2024-04-01")),
                            positions = 1:3,
                            su_options = which_su_calc(FALSE,
                                                       "no_factor",
                                                       "no_factor"),
                            repeat_rate = FALSE
                            ) {

  # Set timer
  start = Sys.time()
  msg <- sprintf("Start KPIs calculations %s", start)
  print(msg)
  # setProgress(0, message = msg)

  ## Schema def --------------------------------

  schema_wt = in_schema("cps", "hh_weights_monthly_pet")
  schema_cs = in_schema("cps","hh_weights_continuous_pet")

  # default schemas
  schema_move <- in_schema("cps", "movements_pet")
  schema_move_panel <- in_schema("cps", "movements_panel_pet")
  schema_projection <- in_schema("cps", "ctlg_panels_pet")
  # technical pages
  schema_cltg_axis <- in_schema("metadata", "ctlg_axis")
  schema_axis_data <- in_schema("metadata", "axis_data")

  ## Load data -----------------------------------------

  msg <- sprintf("Load the data ...")
  print(msg)
  # setProgress(.1, message = msg)

  # factors & waves
  db_move <- con |>
    tbl(schema_move) |>
    filter(movedate >= period$start,
           movedate < period$end) |>
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
    filter(movedate >= period$start,
           movedate < period$end) |>
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

  # CS weights -----------------------------------------

  if (repeat_rate) {
    # use last day of month for join with cs
    period_end2 <- period$end - days(1)
    # use fullmass table instead of cs in case of 1 month:
    if ((period$end - period$start) > 31) {
      db_cs <- con |> tbl(schema_cs)
    } else {
      db_cs <- con |> tbl(schema_wt)
    }

    db_cs <-
      db_cs |>
      filter(
        id_panel == panel_id,
        dt_start == period$start,
        dt_end == period_end2
      ) |>
      # FIXME id_panel column requires
      select(hhkey, continuo) |>
      compute()

    # check if continuos weights are non-empty
    n_cs <- db_cs |> count() |> collect() |> pull(n)
    print(sprintf("Number of cs records: %s", n_cs))
    # if (n_cs == 0) {
    #   return("empty cs")
    # }
  }


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
      summarise(dt_start = max(dt_start))

    db_flth <- con |>
      tbl(schema_flth) |>
      semi_join(db_hh_available, by = join_by(hhkey, dt_year == dt_start)) |>
      select(id_flt, hhkey) |>
      # add indicator column
      mutate(flth = TRUE) |>
      compute()
  }

  # Projection
  db_projection <- con |> tbl(schema_projection)
  projectc <- db_projection |>
    filter(id_panel == panel_id) |>
    collect() |>
    pull(projectc)


  ## Weight wave --------------------------

  msg <- sprintf(sprintf("Get wave weights ..."))
  print(msg)
  # setProgress(.2, message = msg)

  # get ww and population. If flth is specified, add respective column:
  db_ww <- db_wt |> agg_weight_wave(period)
  if (!is.null(schema_flth) && is.null(schema_axsh)) {
    db_ww <- db_ww |>
      left_join(db_flth, by = "hhkey") |>
      mutate_at("flth", coalesce, FALSE)
  }
  db_ww <- db_ww |> compute()

  ## Condition for db_su table: -----------------------------

  if ((su_options$volume != "no_factor") |
      (su_options$volume_packs != "no_factor")) {

    db_su <- prep_su_tbl(con,
                         period$start,
                         period$end,
                         su_options)
  }


  ## Load axes ---------------------------------------------------------------

  msg <- sprintf("Load AXS-M ...")
  print(msg)
  # setProgress(.3, message = msg)

  db_axsm <- con |>
    tbl(schema_axsm) |>
    filter(position_number %in% positions) |>
    filter(movedate >= period$start,
           movedate < period$end) |>
    # precompute for speed up
    compute(indexes = c("id_rec"))

  # tweak for axsa
  if (!is.null(schema_axsa)) {
    msg <- sprintf("Load AXS-A ...")
    print(msg)
    # setProgress(.33, message = msg)

    db_axsa <- con |>
      tbl(schema_axsa) |>
      filter(reporting_from <= period$end,
             to_date >= period$start) |>
      compute(indexes = c("article", "reporting_from", "to_date"))
  }

  # tweak for axss
  if (!is.null(schema_axss)) {
    msg <- sprintf("Load AXS-S ...")
    print(msg)
    # setProgress(.36, message = msg)
    db_axss <- con |>
        tbl(schema_axss) |>
        compute(indexes = c("id_shop"))
  }

  # axsh
  if (!is.null(schema_axsh)) {
    msg <- sprintf("Load AXS-H ...")
    print(msg)
    # setProgress(.39, message = msg)

    db_hh_available <- db_wt |>
      group_by(hhkey) |>
      # find last reporting year for each hh
      summarise(dt_record = max(dt_start))

    # TODO combine with FLT-H
    db_axsh <- con |>
      tbl(schema_axsh) |>
      semi_join(db_hh_available, by = join_by(hhkey, dt_record)) |>
      select(id_axis, position_number, autolabel, hhkey) |>
      compute(indexes = c("hhkey"))
    if (!is.null(schema_flth)) {
      db_axsh <- db_axsh |> semi_join(db_flth, by = "hhkey")
    }
  } else {
    db_axsh <- NULL
  }

  ## Merge together --------------------------------------------------------

  msg <- sprintf("Merge data together ...")
  print(msg)
  # setProgress(.35, message = msg)

  db_calc <- db_axsm |>
    # Add wave weights:
    left_join(db_move, by = join_by(movedate, id_rec)) |>
    left_join(db_move_panel, by = join_by(movedate, id_rec)) |>
    filter(movedate >= period$start,
           movedate < period$end) |># filter period
    # add fullmass:
    add_fullmass(db_wt) |>
    # add weight waves
    inner_join(db_ww, by = c("hhkey")) |>
    # add trip id (move here to remove dependent )
    mutate(id_trip = sql("hashtext(concat(hhkey::text, movedate::text, id_shop::text))::integer")) |>
    compute(index = c("position_number", "id_rec", "hhkey", "article", "id_shop", "movedate", "id_trip"))

  # > Add extra cols -------------------------------
  # SU factors
  if ((su_options$volume != "no_factor") |
      (su_options$volume_packs != "no_factor")) {
    db_calc <- db_calc |>
      # FIXME Why do we need hhkey here?
      left_join(db_su, by = join_by(id_rec, movedate, hhkey))
  } else {
    db_calc <- db_calc |>
      mutate(su_factor_vol = 1,
             su_factor_volpacks = 1)

  }

  # add CS for Repeat Rate
  if (repeat_rate) {
    db_calc <- db_calc |>
      left_join(db_cs, by = join_by(hhkey)) |>
      mutate_at("continuo", coalesce, 0)
  }

  # > add Axes ----------------------------------------

  # define default grouping:
  db_calc <- db_calc |> group_by(id_axis, position_number, autolabel)

  # Add AXS-A if any
  if (!is.null(schema_axsa)) {
    db_calc <- db_calc |>
      inner_join(db_axsa,
                 by = join_by(article,
                              between(movedate, reporting_from, to_date)),
                 suffix = c("", "_art")) |>
      group_by(id_axis_art, position_number_art, autolabel_art, .add = TRUE)
  }
  # Add AXS-S if any
  if (!is.null(schema_axss)) {
    db_calc <- db_calc |>
      inner_join(db_axss, by = join_by(id_shop), suffix = c("", "_shop")) |>
      group_by(id_axis_shop, position_number_shop, autolabel_shop, .add = TRUE)
  }

  # Calculate KPIs: ---------------------------------

  t_interim <- Sys.time()
  msg <- sprintf("Get core KPIs... %s", round(t_interim - start, 2))
  print(msg)
  # setProgress(.5, message = msg)

  kpi_df <- db_calc |>
    get_core_kpi(db_ww,
                 projectc,
                 db_axsh = db_axsh,
                 repeat_rate = repeat_rate,
                 is_flth = !is.null(schema_flth))

  end = Sys.time()
  msg <- sprintf("Completed! Elapsed time %s", round(end - start, 2))
  print(msg)
  # setProgress(1, message = msg)
  # showNotification(msg, duration = NULL, type = "message")

  if (is.null(kpi_df)) {
    return("empty df")
  }else{
    kpi_df
  }

}

add_fullmass <- function(db, db_wt) {
  db |>
    # NB fullmass defines panel, therefore it's always inner join
    inner_join(db_wt |>
                 select(hhkey, dt_start, dt_end, fullmasw = continuo),
               join_by(hhkey, between(movedate, dt_start, dt_end))) |>
    select(-dt_end, -dt_start)
}


get_volumetric_su <- function(db, value, volume, number, rwcompen, fullmasw, projectf,
                              scale) {
      db |>
      summarise(
        value       =  sum({{ value }}  * {{ rwcompen }} * {{ fullmasw }}   * {{ projectf }} / {{ scale }}),
        volume_gl   =  sum({{ volume }} * {{ rwcompen }} *  {{ fullmasw }}  * {{ projectf }}  / {{ scale }}),
        volume_SU   =  sum({{ volume }} * {{ rwcompen }} *  {{ fullmasw }}  * {{ projectf }} * su_factor_vol / {{ scale }}/1e3),
        volume_pack =  sum({{ number }} * {{ rwcompen }} *  {{ fullmasw }}  * {{ projectf }}  / {{ scale }})
      ) |>
      collect()
}


which_su_calc <- function(su_enabled,
                          su_option_vol,
                          su_option_volpacks) {

  if (su_enabled == TRUE) {

    names(su_option_vol) <- NULL
    names(su_option_volpacks) <- NULL

    list("volume" = su_option_vol,
         "volume_packs" = su_option_volpacks)
  }else{
    list("volume" = "no_factor",
         "volume_packs" = "no_factor")

  }
}



prep_su_tbl <- function(con,
                        start_period = period$start,
                        end_period = period$end,
                        su_options){

  schema_move <- in_schema("cps", "movements_pet")
  # article table:
  schema_article <- in_schema("cps", "ctlg_article_pet")

  db_su <- tbl(con, schema_move) |>
    filter(movedate >= start_period,
           movedate < end_period) |>
    select(
      id_rec,
      movedate,
      hhkey,
      article) |>
    inner_join(tbl(con, schema_article) |>
                 select(article, reporting_from, to_date,
                        pgsufac1, suinstan,
                        henkelde, henkeld0,
                        dwl_washload_num),
               join_by(article, between(movedate, reporting_from, to_date, bounds = "[]"))) |>
    select(-c(reporting_from, to_date)) |>
    mutate(across(pgsufac1:dwl_washload_num, as.numeric),
           across(pgsufac1:dwl_washload_num, ~ifelse(is.na(.x), 1, .x)))

  if ((su_options$volume != "no_factor") &
      (su_options$volume_packs != "no_factor")) {

    db_su <- db_su |>
      mutate(
        su_factor_vol = !! rlang::sym(su_options$volume) * 1e3,
        su_factor_volpacks =  !! rlang::sym(su_options$volume_packs)) |>
      select(id_rec, movedate, hhkey,
             su_factor_vol, su_factor_volpacks)


  } else if ((su_options$volume == "no_factor") &
             (su_options$volume_packs != "no_factor")) {

    db_su <- db_su |>
      mutate(su_factor_vol = 1e3,
             su_factor_volpacks = !! rlang::sym(su_options$volume_packs)) |>
      select(id_rec, movedate, hhkey, su_factor_vol, su_factor_volpacks)

  } else {

    db_su <- db_su |>
      mutate(su_factor_volpacks = 1,
             su_factor_vol = !! rlang::sym(su_options$volume) * 1e3) |>
      select(id_rec, movedate, hhkey,
             su_factor_volpacks, su_factor_vol)
  }

  db_su

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
      volume_per_buyer_g = volume_gl / buyers, # as needs conversion from tonnes to kg
      volume_per_buyer_su = volume_SU / buyers,  # I'm here
      volume_per_buyer_pack = volume_pack / buyers,
      volume_per_trip_g = volume_gl / trips, # as needs conversion from tonnes to kg
      volume_per_trip_su = volume_SU / trips,
      volume_per_trip_pack = volume_pack / trips,
      avg_price_g = value / volume_gl, # per kg
      avg_price_su = value / volume_SU,
      avg_price_pack = value / volume_pack,
      avg_pack_size = volume_gl / volume_pack
    )
}

get_repeat_rate <- function(db_grouped, hhkey, continuo, projectc = 2728.03,
                            scale = 1e3, .add = TRUE) {
  # count raw trips to identify repeaters
  hhkey_trips_raw <-
    db_grouped |>
    filter({{ continuo }} != 0) |>
    mutate(id_trip = sql("hashtext(concat(hhkey::text, movedate::text, id_shop::text))::integer")) |>
    group_by({{ hhkey }}, .add = .add) |>
    # trips are based on hhkey + movedate + id_shop
    summarise(n_trips = n_distinct(id_trip),
              continuo = max({{ continuo }})) |>
    collect()

  # bind total / repeaters and summarise buyers
  repeat_rate_df <- hhkey_trips_raw |>
    summarise(
      buyers_total_cs = sum({{ continuo }} * projectc),
      buyers_repeat_cs = sum((n_trips > 1) * {{ continuo }} * projectc),
      repeat_rate_cs = buyers_repeat_cs / buyers_total_cs * 100
    ) |>
    select(repeat_rate_cs)
}


add_kpi_labels <- function(kpi_df, con, schema_axisdata,
                           position_name, id_axis, position_number, autolabel,
                           value_group_name, level, hide_concat,
                           show_empty_positions = FALSE, positions = NULL) {
  # collect data from axis data registry
  axis_id <- kpi_df |> distinct({{ id_axis }}) |> pull(1)
  db_axis <- get_axis_data(con, schema_axisdata, axis_id) |>
    # remove hidden columns from report
    filter(hide_concat != "hide only") |>
    # workaround to have consistent names
    rename({{ id_axis }} := id_axis,
           {{ position_number }} := position_number,
           {{ value_group_name }} := value_group_name,
           {{ level }} := level,
           {{ hide_concat }} := hide_concat)

  if (show_empty_positions) {
    # add labels to the kpi_df
    if (!is.null(positions)) {
      db_axis <- db_axis |>
        filter({{ position_number }} %in% positions)
    }
    db_axis |>
      left_join(kpi_df,
                by = join_by({{ id_axis }}, {{ position_number }})) |>
      # if autolabel != "" replace value_group_name with it
      mutate({{ position_name }} := ifelse({{ autolabel }} == "" | is.na({{ autolabel }}),
                                           {{ value_group_name }},
                                           {{ autolabel }})) |>
      select(-{{ value_group_name }}, -{{ autolabel }}) |>
      # rearrange cols
      select({{ id_axis }}, {{ position_number }}, {{ level }},
             {{ hide_concat }}, {{ position_name }}, everything()) |>
      mutate(across(!contains(c("position_number",
                                "level")) &
                      where(is.numeric),
                    ~replace_na(.x, 0)))

  } else {
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

}

get_buyers_rp_correction <- function(buyers_wave_rwbasis, trips_rwbasis, population_rwbasis) {

  rp_df <- buyers_wave_rwbasis |>
    left_join(trips_rwbasis) |>
    left_join(population_rwbasis)

  buyers_rp <- rp_df |>
    rowwise() |>
    mutate(buyers_rp = penetration_correction(
      buyers_wave = buyers_wave,
      trips_wave   = trips_wave,
      trips_fullmass    = trips_fullmass,
      population  = population
    )) |>
    # switch off rowwise
    group_by(.add = TRUE)

  buyers_rp
}

# wrapper for buyers rp
# get_buyers_rp <- function(db_grouped) {
#   # trips--------------------------
#   kpi_trips_rwbasis <- db_grouped |>
#     group_by(rwbasis, .add = TRUE) |>
#     get_trips(id_trip, rwcompen, fullmasw, weight_wave, projectc)
#
#   # population ---------------------------
#   population_df <- db_ww |>
#     group_by(rwbasis) |>
#     get_population(weight_wave, projectc)
#
#   population_df <- db_population |> collect()
#   population_value = sum(population_df$population)
#
#   # buyers wave --------------------------
#   buyers_wave_rwbasis <- db_grouped |>
#     group_by(rwbasis, .add = TRUE) |>
#     get_buyers_wave(hhkey, weight_wave, projectc)
#   buyers_raw <- buyers_wave_rwbasis |> summarise_at("buyers_raw", sum)
#
#   # buyers wave --------------------------
#   kpi_buyers <- get_buyers_rp_correction(
#     buyers_wave_rwbasis,
#     kpi_trips_rwbasis,
#     population_df
#   )
# }

get_population <- function(data_total, weight_wave, projectc,
                           scale = 1e3, .add = FALSE) {
  population <- data_total |>
    summarise(population = sum({{ weight_wave }} * {{ projectc }}) / scale) |>
    collect()

  return(population)
}


get_axis_data <- function(con, schema_axisdata, axis_id) {
  con |>
    tbl(schema_axisdata) |>
    filter(id_axis %in% axis_id) |>
    select(id_axis, position_number, level, hide_concat, value_group_name) |>
    collect()
}


