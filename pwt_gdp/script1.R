# combined_pipeline.R
# Place in /Users/emmagordon/Desktop/my_project/ and run from the .Rproj
# Robust pipeline: discover -> standardize -> harmonize -> prioritise -> interpolate -> splice -> extrapolate -> save

# ------------------ PACKAGES ------------------
library(here)
library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(janitor)
library(stringr)
library(tibble)

# ------------------ PATHS ------------------
preferred_data_dir <- here::here("data")
legacy_data_dir <- "/Users/emmagordon/Desktop/gdp1"

base_dir <- if (dir.exists(preferred_data_dir)) {
  message("Using project data folder: ", preferred_data_dir)
  preferred_data_dir
} else if (dir.exists(legacy_data_dir)) {
  message("Preferred project 'data/' not found. Falling back to legacy path: ", legacy_data_dir)
  legacy_data_dir
} else {
  stop("Neither project 'data/' nor legacy path exists. Create one or change legacy_data_dir.")
}

paths <- list(
  PWT = file.path(base_dir, "pwt_gdp"),
  MPD = file.path(base_dir, "mpd_gdp"),
  WDI = file.path(base_dir, "wdi_gdp"),
  UN  = file.path(base_dir, "un_gdp"),
  BU  = file.path(base_dir, "barroursua_gdp"),
  SC  = file.path(base_dir, "scaled")
)

intermediate_dir <- file.path(base_dir, "intermediate")
if (!dir.exists(intermediate_dir)) dir.create(intermediate_dir, recursive = TRUE)

# ------------------ METADATA ------------------
dataset_info <- list(
  "pwt_10.1" = list(base_year = 2017, gdp_type = "total", gdp_unit = "millions"),
  "pwt_10"   = list(base_year = 2017, gdp_type = "total", gdp_unit = "millions"),
  "pwt_9.1"  = list(base_year = 2017, gdp_type = "total", gdp_unit = "millions"),
  "pwt_9"    = list(base_year = 2011, gdp_type = "total", gdp_unit = "millions"),
  "pwt_8.1"  = list(base_year = 2005, gdp_type = "total", gdp_unit = "millions"),
  "pwt_8"    = list(base_year = 2011, gdp_type = "total", gdp_unit = "millions"),
  "pwt_7.1"  = list(base_year = 2005, gdp_type = "pc",    gdp_unit = "total"),
  "pwt_7"    = list(base_year = 2005, gdp_type = "total", gdp_unit = "total"),
  "pwt_6.3"  = list(base_year = 2005, gdp_type = "pc",    gdp_unit = "total"),
  "pwt_6.2"  = list(base_year = 2000, gdp_type = "pc",    gdp_unit = "total"),
  "pwt_6.1"  = list(base_year = 1996, gdp_type = "pc",    gdp_unit = "total"),
  "pwt_5.6"  = list(base_year = 1985, gdp_type = "total", gdp_unit = "total"),
  
  "mpd_2010" = list(base_year = 1990, gdp_type = "pc", gdp_unit = "total"),
  "mpd_2013" = list(base_year = 1990, gdp_type = "pc", gdp_unit = "total"),
  "mpd_2018" = list(base_year = 2011, gdp_type = "pc", gdp_unit = "total"),
  "mpd_2020" = list(base_year = 2011, gdp_type = "pc", gdp_unit = "total"),
  "mpd_2023" = list(base_year = 2011, gdp_type = "pc", gdp_unit = "total"),
  
  "wdi_archive" = list(base_year = 2011, gdp_type = "total", gdp_unit = "total"),
  "wdi_ppp"     = list(base_year = 2021, gdp_type = "total", gdp_unit = "total"),
  "wdi_current_lcu_ppp" = list(base_year = 2010, gdp_type = "total", gdp_unit = "total"),
  
  "un_data_ppp" = list(base_year = 2015, gdp_type = "total", gdp_unit = "total"),
  "barro_ursua" = list(base_year = 2006, gdp_type = "pc",    gdp_unit = "total"),
  
  "gdp_scaled"  = list(base_year = 2017, gdp_type = "total", gdp_unit = "total"),
  "gdp_scaling" = list(base_year = 2017, gdp_type = "total", gdp_unit = "total")
)

priority_table <- tibble(source = c("PWT","MPD","WDI","UN","BU","SC"), source_priority = c(1,2,3,4,5,6))

# ------------------ MANIFEST ------------------
manifest <- purrr::imap_dfr(paths, function(folder, source_name) {
  if (!dir.exists(folder)) return(tibble())
  files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)
  tibble(file = files, source = source_name)
}) %>%
  mutate(
    filename = basename(file),
    version = tools::file_path_sans_ext(filename),
    dataset_key = tolower(version)
  ) %>%
  filter(!str_detect(filename, "_combined\\.csv$"),
         !str_detect(filename, "_test\\.csv$"),
         !str_detect(filename, "\\.R$")) %>%
  left_join(priority_table, by = "source") %>%
  arrange(source, filename)

readr::write_csv(manifest, file.path(intermediate_dir, "manifest.csv"))
message("Manifest saved to: ", file.path(intermediate_dir, "manifest.csv"))
message("Files discovered per source:")
print(manifest %>% group_by(source) %>% summarise(n = n(), .groups = "drop"))

# ------------------ UNIT CONVERSION ------------------
convert_gdp_vec <- function(values, from_unit_vec, to_unit = "millions") {
  if (length(from_unit_vec) == 1) from_unit_vec <- rep(from_unit_vec, length(values))
  out <- rep(NA_real_, length(values))
  for (i in seq_along(values)) {
    val <- values[i]
    fu <- from_unit_vec[i]
    if (is.na(val)) { out[i] <- NA_real_; next }
    if (is.na(fu) || fu == to_unit) { out[i] <- val; next }
    if (fu == "total" && to_unit == "millions") out[i] <- val / 1e6
    else if (fu == "millions" && to_unit == "total") out[i] <- val * 1e6
    else stop("Unhandled GDP unit conversion: ", fu, " -> ", to_unit)
  }
  out
}

#------------------ MPD GDP UNIT CORRECTION ------------------
  adjust_mpd_units <- function(df) {
    df %>%
      mutate(
        gdp = ifelse(source == "MPD" & gdp < 1e3 & gdppc < 1, gdp * 1e6, gdp),
        gdppc = ifelse(source == "MPD" & gdppc < 1, gdppc * 1e3, gdppc)
      )
  }

#Apply correction
all_standardized <- adjust_mpd_units(all_standardized)

# ------------------ STANDARDIZER (SAFE + SMALL SCHEMA) ------------------
read_and_standardize <- function(path, source, version, dataset_key, source_priority) {
  info <- dataset_info[[dataset_key]]
  if (is.null(info)) {
    message("Skipping (no metadata): ", basename(path))
    return(tibble())
  }
  
  df <- tryCatch(
    readr::read_csv(path, show_col_types = FALSE),
    error = function(e) { warning("Failed to read ", path, ": ", e$message); return(tibble()) }
  )
  
  # report parsing problems if any
  pr <- readr::problems(df)
  if (nrow(pr) > 0) {
    message("Parsing problems in ", basename(path), " (first 5):")
    print(head(pr, 5))
  }
  
  if (nrow(df) == 0) return(tibble())
  
  df <- janitor::clean_names(df)
  
  # GDP candidates (includes gdp_2015_ppp)
  gdp_candidates   <- c("gdp_2015_ppp", "rgdpo", "rgdp", "gdp", "gdp_total", "gdp_usd", "gdp_current",
                        "gdp_current_us", "gdp_constant_2010", "gdp_constant_2015")
  gdppc_candidates <- c("gdppc", "gdp_percap", "gdp_pc", "gdp_per_capita", "gdp_pc_ppp", "gdppc_2015_usd")
  pop_candidates   <- c("pop", "population", "pop_total", "population_total", "pop_est", "population_est")
  
  get_first_numeric_col <- function(df_local, candidates) {
    present <- intersect(candidates, names(df_local))
    if (length(present) == 0) return(rep(NA_real_, nrow(df_local)))
    vec <- df_local[[present[[1]]]]
    if (!is.numeric(vec)) {
      vec_num <- suppressWarnings(as.numeric(gsub(",", "", as.character(vec))))
      return(vec_num)
    }
    return(as.numeric(vec))
  }
  
  df$gdp_raw   <- get_first_numeric_col(df, gdp_candidates)
  df$gdppc_raw <- get_first_numeric_col(df, gdppc_candidates)
  df$pop       <- get_first_numeric_col(df, pop_candidates)
  
  if (all(is.na(df$gdp_raw)) && all(is.na(df$gdppc_raw))) {
    message("WARNING: No GDP or GDPpc detected in ", basename(path), " ó imported but gdp/gdppc will be NA.")
  } else {
    picked_gdp <- intersect(gdp_candidates, names(df))[1]
    picked_gdppc <- intersect(gdppc_candidates, names(df))[1]
    if (!is.na(picked_gdp)) message("Using GDP column '", picked_gdp, "' for ", basename(path))
    if (!is.na(picked_gdppc)) message("Using GDPpc column '", picked_gdppc, "' for ", basename(path))
  }
  
  df_standard <- df %>%
    mutate(
      source = source,
      version = version,
      dataset_key = dataset_key,
      gdp_unit_raw = info$gdp_unit,
      gdp = case_when(
        info$gdp_type == "total" & !is.na(gdp_raw) ~ as.numeric(gdp_raw),
        info$gdp_type == "pc" & !is.na(gdppc_raw) & !is.na(pop) ~ as.numeric(gdppc_raw) * as.numeric(pop),
        TRUE ~ NA_real_
      ),
      gdppc = case_when(
        info$gdp_type == "pc" & !is.na(gdppc_raw) ~ as.numeric(gdppc_raw),
        info$gdp_type == "total" & !is.na(gdp_raw) & !is.na(pop) ~ as.numeric(gdp_raw) / as.numeric(pop),
        TRUE ~ NA_real_
      ),
      source_priority = source_priority
    ) %>%
    mutate(gdp = convert_gdp_vec(gdp, gdp_unit_raw, to_unit = "millions"),
           gdp_unit = "millions")
  
  # Normalize essential metadata columns to fixed types
  df_standard <- df_standard %>%
    mutate(
      iso3_alpha = if ("iso3_alpha" %in% names(df_standard)) as.character(iso3_alpha) else NA_character_,
      iso_num    = if ("iso_num" %in% names(df_standard)) suppressWarnings(as.integer(iso_num)) else NA_integer_,
      year       = if ("year" %in% names(df_standard)) suppressWarnings(as.integer(year)) else NA_integer_,
      country    = if ("country" %in% names(df_standard)) as.character(country) else NA_character_,
      pop        = as.numeric(pop)
    ) %>%
    select(iso3_alpha, iso_num, year, country, pop, gdp, gdppc, gdp_unit_raw, source, version, dataset_key, source_priority)
  
  return(df_standard)
}

# ------------------ STANDARDIZE ALL FILES ------------------
to_process <- manifest %>% filter(dataset_key %in% names(dataset_info))
if (nrow(to_process) == 0) stop("No files to process. Check manifest and dataset_info keys.")

standardized_list <- to_process %>%
  mutate(data = pmap(list(file, source, version, dataset_key, source_priority),
                     read_and_standardize)) %>%
  select(file, source, version, dataset_key, source_priority, data)

# build list of non-empty tibbles and file paths
processed_rows <- standardized_list %>% filter(map_lgl(data, ~ nrow(.x) > 0))
if (nrow(processed_rows) == 0) stop("No non-empty dataframes found after standardization.")

df_list <- processed_rows %>% pull(data)
processed_files <- processed_rows %>% pull(file)

# === Use canonical combined_pop_dataset.csv to fill/populate population ===
# >>> Ensures population ALWAYS comes from the canonical combined_pop_dataset.csv

combined_pop_path <- file.path(base_dir, "combined_pop_dataset.csv")
if (!file.exists(combined_pop_path)) {
  stop("Combined population file not found: ", combined_pop_path)
}

combined_pop <- readr::read_csv(combined_pop_path, show_col_types = FALSE) %>%
  janitor::clean_names()

# Detect column names automatically
possible_iso_num <- c("iso_num", "isocode", "iso_numeric", "iso")
possible_year    <- c("year", "yr")
possible_pop     <- c("pop", "population", "population_total", "pop_total", "pop_count", "population_est", "pop_est")

pick_col <- function(df, candidates) {
  cands <- intersect(candidates, names(df))
  if (length(cands) == 0) return(NA_character_) else return(cands[1])
}

iso_col <- pick_col(combined_pop, possible_iso_num)
year_col <- pick_col(combined_pop, possible_year)
pop_col  <- pick_col(combined_pop, possible_pop)

if (is.na(iso_col) || is.na(year_col) || is.na(pop_col)) {
  stop("combined_pop_dataset.csv doesn't have expected columns. Found: ",
       paste(names(combined_pop), collapse = ", "))
}

# Build canonical population table (iso_num, year, pop_combined)
combined_pop_small <- combined_pop %>%
  transmute(
    iso_num = as.integer(.data[[iso_col]]),
    year    = as.integer(.data[[year_col]]),
    pop_combined = as.numeric(.data[[pop_col]])
  ) %>%
  filter(!is.na(iso_num), !is.na(year))

# ------------------ BIND with SAFE HARMONIZATION ------------------
# Try straightforward bind; fallback to conservative harmonizer if errors occur
all_standardized <- tryCatch({
  bind_rows(df_list)
}, error = function(e) {
  message("bind_rows() failed: ", e$message)
  message("Running conservative harmonizer for the small schema (gdp/gdppc/pop/etc.)")
  
  all_cols <- unique(unlist(map(df_list, names)))
  # find which columns are numeric in at least one df
  col_has_numeric <- map_lgl(all_cols, function(col) {
    any(map_lgl(df_list, function(.x) {
      if (!is.data.frame(.x)) return(FALSE)
      if (!col %in% names(.x)) return(FALSE)
      is.numeric(.x[[col]]) || is.integer(.x[[col]])
    }))
  })
  names(col_has_numeric) <- all_cols
  
  to_numeric_if_possible <- function(vec) {
    v <- as.character(vec)
    v[v %in% c("", "-", "NA", "na", "NaN", "nan", ".")] <- NA
    v2 <- v %>%
      str_replace_all(",", "") %>%
      str_replace_all("\\(", "") %>%
      str_replace_all("\\)", "") %>%
      str_replace_all("%", "") %>%
      str_replace_all("[[:space:]]+", "")
    v2 <- str_replace(v2, "^([0-9.+-eE]+).*", "\\1")
    num <- suppressWarnings(as.numeric(v2))
    frac_num <- mean(!is.na(num))
    if (frac_num >= 0.4) return(num) else return(as.character(vec))
  }
  
  harmonized_list <- map(df_list, function(df) {
    for (col in all_cols) if (!col %in% names(df)) df[[col]] <- NA
    for (col in all_cols) {
      if (col_has_numeric[[col]]) {
        if (!is.numeric(df[[col]]) && !is.integer(df[[col]])) {
          df[[col]] <- to_numeric_if_possible(df[[col]])
        } else {
          df[[col]] <- as.numeric(df[[col]])
        }
      } else {
        if (!is.character(df[[col]])) df[[col]] <- as.character(df[[col]])
      }
    }
    df <- df[all_cols]
    df
  })
  
  # try to bind harmonized list
  tryCatch({
    bind_rows(harmonized_list)
  }, error = function(e2) {
    message("bind_rows failed even after harmonization: ", e2$message)
    col_types_post <- map(harmonized_list, ~ map_chr(.x, ~ class(.x)[1]))
    col_type_tbl_post <- imap_dfr(col_types_post, function(cts, idx) {
      tibble(df_index = idx, col = names(cts), type = unname(cts))
    })
    mixed_post <- col_type_tbl_post %>%
      group_by(col) %>%
      summarize(n_types = n_distinct(type), .groups = "drop") %>%
      filter(n_types > 1) %>%
      pull(col)
    
    if (length(mixed_post) == 0) {
      stop("Unexpected: no mixed columns but binding still failed.")
    }
    
    message("Forcing columns to character (safe fallback): ", paste(mixed_post, collapse = ", "))
    harmonized_list2 <- map(harmonized_list, function(df) {
      for (col in mixed_post) {
        if (col %in% names(df)) df[[col]] <- as.character(df[[col]]) else df[[col]] <- NA_character_
      }
      df <- df[all_cols]
      df
    })
    bind_rows(harmonized_list2)
  })
})

# === Now join canonical population onto all_standardized ===
# >>> CHANGED: force pop to be canonical pop_combined, drop any pop from individual datasets
all_standardized <- all_standardized %>%
  mutate(iso_num = as.integer(iso_num), year = as.integer(year)) %>%
  left_join(combined_pop_small, by = c("iso_num", "year")) %>%
  mutate(
    pop = pop_combined,  # override any original 'pop' with canonical pop
    pop = ifelse(!is.na(pop) & pop > 0, as.numeric(pop), NA_real_)
  ) %>%
  select(-pop_combined)


# ------------------ PRIORITISATION ------------------
# Choose highest-priority non-missing gdp per iso_num+year
final_by_priority <- all_standardized %>%
  mutate(version_year = readr::parse_number(version)) %>%
  arrange(iso_num, year, source_priority, desc(version_year)) %>%
  group_by(iso_num, year) %>%
  
#pick highest version within the same source priority
filter(!is.na(gdp)) %>%
  slice_min(order_by = source_priority, with_ties = FALSE) %>%
  ungroup() %>%
  select(iso3_alpha, iso_num, year, country, pop, gdp, gdppc, source, dataset_key, version)

# ------------------ INTERPOLATION (log-linear internal gaps) ------------------
# NOTE: interpolation performed on gdp (preferred) and gdppc computed from canonical pop where possible
log_linear_interpolate_series <- function(df_series, var = "gdp") {
  y <- df_series[[var]]
  years <- df_series$year
  known <- !is.na(y) & y > 0
  if (sum(known) < 2) {
    df_series[[paste0(var, "_interp")]] <- y
    return(df_series)
  }
  logy_known <- log(y[known])
  interp_log <- approx(x = years[known], y = logy_known, xout = years, rule = 1)$y
  interp_y <- exp(interp_log)
  y_filled <- ifelse(is.na(y) & !is.na(interp_y), interp_y, y)
  df_series[[paste0(var, "_interp")]] <- y_filled
  df_series
}

# Interpolate gdp (recommended). gdppc will be recomputed from gdp/pop after interpolation.
interp_gdp <- final_by_priority %>%
  group_by(iso_num) %>%
  arrange(year) %>%
  group_modify(~ log_linear_interpolate_series(.x, "gdp")) %>%
  ungroup()

# Fill gdp from gdp_interp where missing
interp_full <- interp_gdp %>%
  mutate(
    gdp = if_else(is.na(gdp) & !is.na(gdp_interp), gdp_interp, gdp)
  ) %>%
  select(-gdp_interp)

# Recompute gdppc from canonical pop to ensure no Inf (pop is canonical)
interp_full <- interp_full %>%
  mutate(
    gdppc = if_else(!is.na(pop) & pop > 0, gdp / pop, NA_real_)
  )

final_interp <- interp_full

saveRDS(final_interp, file = file.path(intermediate_dir, "final_interpolated.rds"))
message("Saved final_interpolated.rds (rows = ", nrow(final_interp), ")")

# ------------------ SPLICING (growth-rate splicing) ------------------
# >>> CHANGED: Splicing operates on gdp only (gdppc is derived from canonical pop)
# Maddison-style growth-rate splicing functions (operate on "gdp")
splice_one <- function(backbone_df, donor_df, iso, anchor_year = NULL,
                       direction = c("backward","forward"),
                       min_overlap_years = 1, max_years_to_splice = 200) {
  direction <- match.arg(direction)
  b <- backbone_df %>% filter(iso_num == iso) %>% arrange(year)
  d <- donor_df %>% filter(iso_num == iso) %>% arrange(year)
  if (nrow(b) == 0 && nrow(d) == 0) return(tibble())
  
  if (is.null(anchor_year)) {
    common_years <- intersect(b$year, d$year)
    if (length(common_years) == 0) stop("No overlap between backbone and donor for iso ", iso)
    anchor_year <- if (direction == "backward") max(common_years) else min(common_years)
  }
  if (length(intersect(b$year, d$year)) < min_overlap_years) {
    warning("Overlap < min_overlap_years for iso ", iso)
  }
  
  # donor growth rates (log growth)
  d_series <- d %>% select(year, gdp) %>% filter(!is.na(gdp) & gdp > 0) %>% arrange(year)
  if (nrow(d_series) < 2) return(b)  # nothing to splice
  
  d_growth <- tibble(year = d_series$year[-1], gr = diff(log(d_series$gdp)))
  
  # anchor value from backbone
  anchor_val <- b %>% filter(year == anchor_year) %>% pull(gdp)
  if (length(anchor_val) == 0 || is.na(anchor_val) || anchor_val <= 0) return(b)
  
  if (direction == "backward") {
    donor_gr_for_back <- d_growth %>% filter(year <= anchor_year) %>% arrange(desc(year))
    if (nrow(donor_gr_for_back) == 0) return(b)
    out <- tibble()
    current_val <- anchor_val
    for (i in seq_len(nrow(donor_gr_for_back))) {
      gr_val <- donor_gr_for_back$gr[i]
      yr <- donor_gr_for_back$year[i] - 1
      val <- current_val / exp(gr_val)
      out <- bind_rows(out, tibble(year = yr, gdp = val))
      current_val <- val
      if (nrow(out) >= max_years_to_splice) break
    }
    out <- out %>% arrange(year)
  } else {
    donor_gr_for_forw <- d_growth %>% filter(year >= anchor_year + 1) %>% arrange(year)
    if (nrow(donor_gr_for_forw) == 0) return(b)
    out <- tibble()
    current_val <- anchor_val
    for (i in seq_len(nrow(donor_gr_for_forw))) {
      gr_val <- donor_gr_for_forw$gr[i]
      yr <- donor_gr_for_forw$year[i]
      val <- current_val * exp(gr_val)
      out <- bind_rows(out, tibble(year = yr, gdp = val))
      current_val <- val
      if (nrow(out) >= max_years_to_splice) break
    }
    out <- out %>% arrange(year)
  }
  
  # Merge: prefer backbone values where present, otherwise use spliced
  years_range <- sort(unique(c(b$year, out$year)))
  result <- tibble(year = years_range) %>%
    left_join(b, by = "year") %>%
    left_join(out, by = "year", suffix = c("", ".spliced")) %>%
    mutate(
      gdp = coalesce(gdp, gdp.spliced)
    ) %>%
    select(-gdp.spliced) %>%
    mutate(iso_num = iso,
           country = if ("country" %in% names(b)) unique(b$country) else if ("country" %in% names(d)) unique(d$country) else NA_character_) %>%
    arrange(year)
  
  return(result)
}

splice_many <- function(backbone_df, donor_df, iso_list = NULL, direction = "backward",
                        anchor_years = NULL, min_overlap_years = 1, max_years_to_splice = 200) {
  if (is.null(iso_list)) iso_list <- sort(intersect(unique(backbone_df$iso_num), unique(donor_df$iso_num)))
  res <- map(iso_list, function(iso_i) {
    ay <- if (!is.null(anchor_years) && as.character(iso_i) %in% names(anchor_years)) anchor_years[[as.character(iso_i)]] else NULL
    tryCatch({
      splice_one(backbone_df, donor_df, iso_i, anchor_year = ay, direction = direction,
                 min_overlap_years = min_overlap_years, max_years_to_splice = max_years_to_splice)
    }, error = function(e) {
      message("Splicing failed for ", iso_i, ": ", e$message)
      tibble()
    })
  })
  bind_rows(res)
}

# Example donor: choose MPD (or other historical donor) but ensure donor uses canonical pop where needed
donor_df <- all_standardized %>% filter(source == "MPD")
# Join canonical pop into donor too (so any computed gdppc from donor uses canonical pop)
donor_df <- donor_df %>% left_join(combined_pop_small, by = c("iso_num", "year")) %>%
  mutate(pop = pop_combined) %>% select(-pop_combined)

# Run splicing: backbone = final_interp (interpolated gdp), donor = donor_df
# >>> CHANGED: now splicing applied AFTER interpolation (we use final_interp as backbone)
isos_to_splice <- unique(intersect(final_interp$iso_num, donor_df$iso_num))
spliced_all <- splice_many(
  backbone_df = final_interp,
  donor_df = donor_df,
  iso_list = isos_to_splice,
  direction = "backward",
  min_overlap_years = 1,
  max_years_to_splice = 200
)

# After splicing, recompute gdppc using canonical pop
spliced_all <- spliced_all %>%
  mutate(pop = NA_real_) %>%  # ensure pop column present
  left_join(combined_pop_small, by = c("iso_num", "year")) %>%
  mutate(pop = pop_combined, pop = ifelse(!is.na(pop) & pop > 0, as.numeric(pop), NA_real_)) %>%
  select(-pop_combined) %>%
  mutate(gdppc = ifelse(!is.na(pop) & pop > 0, gdp / pop, NA_real_))

# Save spliced result for inspection
saveRDS(spliced_all, file = file.path(intermediate_dir, "spliced_all.rds"))
message("Saved spliced_all.rds (rows = ", nrow(spliced_all), ")")

# ------------------ EXTRAPOLATION (controlled by existence table) ------------------
# >>> CHANGED: Use country existence table to limit extrapolation
existence_path <- file.path(base_dir, "country_years.csv")
if (file.exists(existence_path)) {
  country_exist <- readr::read_csv(existence_path, show_col_types = FALSE) %>% janitor::clean_names()
  # Expect columns iso_num, start_year, end_year (end_year can be NA)
  if (!all(c("iso_num", "start_year") %in% names(country_exist))) {
    warning("country_years.csv found but missing required columns 'iso_num' and 'start_year'. Ignoring existence constraints.")
    country_exist <- NULL
  } else {
    # normalize
    country_exist <- country_exist %>%
      mutate(iso_num = as.integer(iso_num), start_year = as.integer(start_year),
             end_year = ifelse("end_year" %in% names(.) , as.integer(end_year), NA_integer_))
  }
} else {
  warning("country_years.csv not found in data/. Extrapolation will use generic years_to_extend behavior.")
  country_exist <- NULL
}

# Extrapolation functions updated to respect existence table
extrapolate_one_controlled <- function(df, iso, var = "gdp", direction = c("backward","forward"),
                                       years_to_extend = 20, min_obs = 5, existence_df = NULL) {
  direction <- match.arg(direction)
  cdata <- df %>% filter(iso_num == iso) %>% arrange(year)
  if (nrow(cdata) < min_obs) {
    message("Skipping ISO ", iso, ": not enough data points for extrapolation")
    return(tibble())
  }
  observed_years <- sort(unique(cdata$year[!is.na(cdata[[var]])]))
  if (length(observed_years) == 0) return(tibble())
  observed_min <- min(observed_years)
  observed_max <- max(observed_years)
  
  # Determine target years based on existence table (if present)
  if (!is.null(existence_df) && iso %in% existence_df$iso_num) {
    exist_row <- existence_df %>% filter(iso_num == iso) %>% slice(1)
    start_exist <- exist_row$start_year
    end_exist <- exist_row$end_year
  } else {
    start_exist <- NA_integer_
    end_exist <- NA_integer_
  }
  
  # Compute years to extrapolate depending on direction and existence constraints
  if (direction == "backward") {
    if (!is.na(start_exist)) {
      # only extrapolate from start_exist up to observed_min - 1
      if (start_exist >= observed_min) return(tibble()) # nothing to extrapolate
      new_years <- seq(max(start_exist, observed_min - years_to_extend), observed_min - 1)
    } else {
      # fallback: extrapolate up to years_to_extend earlier
      new_years <- seq(observed_min - years_to_extend, observed_min - 1)
    }
  } else { # forward
    if (!is.na(end_exist)) {
      if (end_exist <= observed_max) return(tibble())
      new_years <- seq(observed_max + 1, min(end_exist, observed_max + years_to_extend))
    } else {
      new_years <- seq(observed_max + 1, observed_max + years_to_extend)
    }
  }
  if (length(new_years) == 0) return(tibble())
  
  # Fit model on last/first min_obs years depending on direction
  # We'll use up to the first/last min_obs observed points for growth estimation
  if (direction == "backward") {
    # use earliest min_obs years in donor (to estimate backward trend)
    idx <- which(!is.na(cdata[[var]]) & cdata[[var]] > 0)
    if (length(idx) < min_obs) return(tibble())
    use_idx <- head(idx, min_obs)
    x <- cdata$year[use_idx]
    y <- cdata[[var]][use_idx]
  } else {
    idx <- which(!is.na(cdata[[var]]) & cdata[[var]] > 0)
    if (length(idx) < min_obs) return(tibble())
    use_idx <- tail(idx, min_obs)
    x <- cdata$year[use_idx]
    y <- cdata[[var]][use_idx]
  }
  if (length(y) < 2) return(tibble())
  model <- tryCatch(lm(log(y) ~ x), error = function(e) NULL)
  if (is.null(model)) return(tibble())
  coef <- coef(model)
  pred_log <- coef[1] + coef[2] * new_years
  pred_y <- exp(pred_log)
  tibble(iso_num = iso, country = unique(cdata$country), year = new_years, !!var := pred_y,
         source = paste0("extrapolated_", direction), version = "extrapolated")
}

extrapolate_many_controlled <- function(df, iso_list = NULL, var = "gdp",
                                        direction = "backward", years_to_extend = 20,
                                        min_obs = 5, existence_df = NULL) {
  if (is.null(iso_list)) iso_list <- unique(df$iso_num)
  res <- map(iso_list, function(i) {
    tryCatch({
      extrapolate_one_controlled(df, i, var = var, direction = direction,
                                 years_to_extend = years_to_extend, min_obs = min_obs,
                                 existence_df = existence_df)
    }, error = function(e) {
      message("Extrapolation failed for ", i, ": ", e$message)
      tibble()
    })
  })
  bind_rows(res)
}

# === Example controlled extrapolation: apply on spliced_all (spliced gdp) ===
# Backward extrapolation only for years before earliest observed year, but no earlier than country start_year (if provided)
isos_to_extrapolate <- unique(spliced_all$iso_num)

extrapolated_backward <- extrapolate_many_controlled(
  df = spliced_all,
  iso_list = isos_to_extrapolate,
  var = "gdp",
  direction = "backward",
  years_to_extend = 200,      # large cap, but existence table will limit
  min_obs = 5,
  existence_df = country_exist
)

# Extrapolated forward if needed and existence table indicates an end_year in the future (less common)
extrapolated_forward <- extrapolate_many_controlled(
  df = spliced_all,
  iso_list = isos_to_extrapolate,
  var = "gdp",
  direction = "forward",
  years_to_extend = 50,
  min_obs = 5,
  existence_df = country_exist
)

# Build final combined dataset:
# combine spliced + extrapolated rows, recompute gdppc from canonical pop
combined_with_extrapolated <- bind_rows(
  spliced_all %>% mutate(source = coalesce(source, "spliced")),
  extrapolated_backward,
  extrapolated_forward
) %>%
  arrange(iso_num, year) %>%
  # join canonical population and compute gdppc
  left_join(combined_pop_small, by = c("iso_num", "year")) %>%
  mutate(pop = pop_combined, pop = ifelse(!is.na(pop) & pop > 0, as.numeric(pop), NA_real_)) %>%
  select(-pop_combined) %>%
  mutate(gdppc = ifelse(!is.na(pop) & pop > 0, gdp / pop, NA_real_))

# Save combined result
output_file_csv <- file.path(base_dir, "combined_gdp.csv")
readr::write_csv(combined_with_extrapolated, output_file_csv)
saveRDS(combined_with_extrapolated, file = file.path(intermediate_dir, "combined_gdp.rds"))

message("Final combined_gdp.csv saved to: ", output_file_csv)
message("Intermediate RDS saved to: ", file.path(intermediate_dir, "combined_gdp.rds"))

# ------------------ DONE ------------------
message("Pipeline finished. Inspect intermediate/ for debug artifacts (manifest.csv, all_standardized.rds, final_by_priority.rds, final_interpolated.rds, spliced_all.rds, combined_gdp.rds).")

# Basic quick checks on saved output
final <- read_csv(output_file_csv, show_col_types = FALSE)

#2) Look at the structure
glimpse(final)

#3) Show the first 10 rows
head(final, 10)

#4) Check how many rows in total
message("Rows in final: ", nrow(final))

#5) Show years covered
message("Year range: ", paste(range(final$year, na.rm = TRUE), collapse = " - "))

#6) Pick one country (example: Germany)
print(final %>% dplyr::filter(country == "Germany") %>% head(10))
