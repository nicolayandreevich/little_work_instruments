penetration_correction <- function(buyers_wave, trips_wave, trips_fullmass, population, interval = c(0, 100)) {
  p0 = 1 - buyers_wave/population
  # close to zero p0-s
  if (round(p0, 8) <= 0) {
    return(population)
  }
  m = trips_wave/population
  # when dist. is not existing, apply to wave buyers factor occasion[fs,ww] / occasions[avg]
  if (m < -log(p0)) {
    buyer_rp = trips_fullmass / trips_wave * buyers_wave
    return(buyer_rp)
  }
  k = find_k(p0 = p0, m = m, interval = interval)
  m2 = trips_fullmass/population
  p0_2 = p_nonbuyer(m = m2, k = k)
  buyer_rp = population * (1 - p0_2)
  # p0_2 typically nan if we have close estimates to population
  if ((buyer_rp > population) | is.nan(buyer_rp)) {
    return(population)
  }
  buyer_rp
}

find_k <- function(p0, m, interval = c(0, 100)) {
  k <- optimise(function(p0, m, k) (p_nonbuyer(m, k) - p0)^2,
    interval, m = m, p0 = p0)
  return(k$minimum)
}

p_nonbuyer <- function(m, k) {
  (1 + m/k)^(-k)
}
