# R/demographic_labels.R
# Shared codebook and canonicalization helpers for demographic categories.

# Authoritative mapping of subgroup codes to category types and labels,
# including common aliases that appear in "Other" reporting categories.
demographic_codebook <- tibble::tribble(
  ~subgroup_code, ~category_type,         ~subgroup,
  # Total
  "TA",           "Total",                "All Students",
  # Sex / Gender (canonical codes)
  "SF",           "Sex",                  "Female",
  "SM",           "Sex",                  "Male",
  "SNB",          "Sex",                  "Non-Binary",
  # Additional sex values
  "SGZ",          "Sex",                  "Missing Gender",
  "SNR",          "Sex",                  "Not Reported",
  # Aliases promoted from "Other"
  "GF",           "Sex",                  "Female",
  "GM",           "Sex",                  "Male",
  "GX",           "Sex",                  "Non-Binary Gender (Beginning 2019–20)",
  "GZ",           "Sex",                  "Missing Gender",
  "RD",           "Sex",                  "Not Reported",
  # Special Education
  "SE",           "Special Education",    "Students with Disabilities",
  "SN",           "Special Education",    "Non-Students with Disabilities",
  # Socioeconomic
  "SD",           "Socioeconomic",        "Socioeconomically Disadvantaged",
  "NS",           "Socioeconomic",        "Not Socioeconomically Disadvantaged",
  # Alias under Other
  "SS",           "Socioeconomic",        "Socioeconomically Disadvantaged",
  # Homeless
  "HL",           "Homeless",             "Homeless",
  "NH",           "Homeless",             "Not Homeless",
  # Alias under Other
  "SH",           "Homeless",             "Homeless",
  # English Learner
  "EL",           "English Learner",      "English Learner",
  "EO",           "English Learner",      "English Only",
  "IFEP",         "English Learner",      "Initially Fluent English Proficient",
  "RFEP",         "English Learner",      "Reclassified Fluent English Proficient",
  # Foster
  "FY",           "Foster",               "Foster Youth",
  "NF",           "Foster",               "Not Foster Youth",
  # Migrant
  "MG",           "Migrant",              "Migrant",
  "NM",           "Migrant",              "Non-Migrant"
)

# Map a free-form description to canonical subgroup code.
desc_to_canon <- function(desc) {
  d <- tolower(desc %||% "")
  dplyr::case_when(
    grepl("\\bnot\\s*foster|\\bnf\\b", d)                                     ~ "NF",
    grepl("\\bfoster|\\bfy\\b", d)                                             ~ "FY",
    grepl("\\bnon[- ]?migrant|\\bnot\\s*migrant|\\bnm\\b", d)               ~ "NM",
    grepl("\\bmigrant|\\bmg\\b", d)                                            ~ "MG",
    grepl("\\bnot\\s*homeless|\\bnh\\b", d)                                  ~ "NH",
    grepl("\\bhomeless|\\bhl\\b", d)                                          ~ "HL",
    grepl("\\bnot\\s*(students?\\s*with\\s*disab|special\\s*education)|\\bsn\\b", d) ~ "SN",
    grepl("students?\\s*with\\s*disab|special\\s*education|\\bswd\\b", d)  ~ "SE",
    grepl("\\bnot\\s*socioeconomically\\s*disadv|\\bns\\b", d)            ~ "NS",
    grepl("socioeconomically\\s*disadv|\\bsed\\b|low\\s*income|economically\\s*disadv", d) ~ "SD",
    grepl("reclassified\\s*fluent", d)                                           ~ "RFEP",
    grepl("initially\\s*fluent", d)                                              ~ "IFEP",
    grepl("\\benglish\\s*only|\\beo\\b", d)                                  ~ "EO",
    grepl("\\benglish\\s*learner|\\bel(l)?\\b", d)                            ~ "EL",
    grepl("\\bfemale\\b|\\bsf\\b", d)                                       ~ "SF",
    grepl("\\bmale\\b|\\bsm\\b", d)                                         ~ "SM",
    grepl("non[- ]?binary|\\bsnb\\b", d)                                       ~ "SNB",
    grepl("missing\\s*gender|\\bsgz\\b", d)                                   ~ "SGZ",
    grepl("^not\\s*reported$|\\bsnr\\b", d)                                   ~ "SNR",
    grepl("\\btotal\\b|\\ball\\b|^all\\s*students?$", d)                    ~ "TA",
    TRUE ~ NA_character_
  )
}

# Map subgroup codes, including aliases, to canonical codes.
code_alias_to_canon <- function(code) {
  c <- toupper(code %||% "")
  dplyr::case_when(
    c == "GF" ~ "SF",
    c == "GM" ~ "SM",
    c == "GX" ~ "SNB",
    c == "GZ" ~ "SGZ",
    c == "RD" ~ "SNR",
    c == "SS" ~ "SD",
    c == "SH" ~ "HL",
    grepl("^(SF|SM|SNB|SGZ|SNR|SE|SN|SD|NS|EL|EO|IFEP|RFEP|FY|NF|MG|NM|HL|NH|TA)$", c) ~ c,
    TRUE ~ NA_character_
  )
}

# Human-readable label for a canonical code.
canon_label <- function(code) dplyr::recode(code,
  SF="Female", SM="Male", SNB="Non-Binary", SGZ="Missing Gender", SNR="Not Reported",
  SE="Students with Disabilities", SN="Non-Students with Disabilities",
  SD="Socioeconomically Disadvantaged", NS="Not Socioeconomically Disadvantaged",
  EL="English Learner", EO="English Only", IFEP="Initially Fluent English Proficient",
  RFEP="Reclassified Fluent English Proficient",
  FY="Foster Youth", NF="Not Foster Youth",
  MG="Migrant", NM="Non-Migrant",
  HL="Homeless", NH="Not Homeless",
  TA="All Students",
  .default = NA_character_
)

# Category type for a canonical code.
canon_category <- function(code) dplyr::recode(code,
  SF="Sex", SM="Sex", SNB="Sex", SGZ="Sex", SNR="Sex",
  SE="Special Education", SN="Special Education",
  SD="Socioeconomic", NS="Socioeconomic",
  EL="English Learner", EO="English Learner", IFEP="English Learner", RFEP="English Learner",
  FY="Foster", NF="Foster",
  MG="Migrant", NM="Migrant",
  HL="Homeless", NH="Homeless",
  TA="Total",
  .default = "Other"
)

# Resolve a canonical code from description and/or code.
canon_code <- function(desc, code = NULL) dplyr::coalesce(desc_to_canon(desc), code_alias_to_canon(code))

# Apply canonical labels and category types to a data frame.
canonicalize_demo <- function(df, desc_col = "subgroup", code_col = "subgroup_code") {
  canon <- canon_code(df[[desc_col]], df[[code_col]])
  df$subgroup_code <- canon
  df$subgroup <- canon_label(canon)
  df$category_type <- canon_category(canon)
  df
}

# Convenience wrapper returning canonical labels from free-form descriptors.
canon_demo_label <- function(x) canon_label(desc_to_canon(x))

