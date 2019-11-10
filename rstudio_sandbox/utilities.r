### doesn't work yet

percentify <- function(dat, category_field_name,category_value,numeric_field_name,numeric_field) {
  totals <- dat %>% 
    group_by_at(names(dat)[ names(dat) != category_field_name & names(dat) != numeric_field_name]) %>%
    summarise(sum(numeric_field,na.rm = TRUE))
  return(totals)
}

debug(percentify)
percentify(votes,"choice_party","DEM","abs_votes",abs_votes)

a <- (names(votes)[ names(votes) != "choice_party" & names(votes) != "abs_votes"])
