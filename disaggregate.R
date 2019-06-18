# disaggregate.R
# Written beginning June 12, 2019 by Daniel Philippus for the Los Angeles River Flows project
# at Colorado School of Mines.
#
# This program is designed to disaggregate daily flow data into hourly.  The hourly data must have
# the same daily mean flows as the daily data, and ideally it should smoothly transition.
#
# The original idea was to have the flows at midnight be equal to the average of the daily flows
# for the days preceding and following, with the flows at noon set so that the mean is correct.
# However, this results in a minimum mean daily flow of (1/4)(preceding mean + following mean).
# There is no maximum as the peak at noon can be infinitely high, whereas the minimum at noon
# must be non-negative.
#
# In order to address this, instead the flows at midnight are set equal to those of the day with the
# lower daily flow, with the peak of the higher day adjusted accordingly.
#
# In the actual data, the largest percent difference between the actual mean
# and the mean of the generated hourly data is 3E-14 %.  In random testing, the largest percent
# difference is typically about 1.5E-14%.  This is presumably due to floating-point rounding errors,
# as a problem with the algorithm would be expected to produce far larger errors.  A previous version
# with a minor error produced maximum errors of just under 1%.

library(readr)
library(dplyr)
library(tidyr)
library(lubridate)

# This assumes LARFlows is in the working directory.
dailyData <- as_tibble(read_csv(file.path("LARFlows", "Disaggregate Flows", "F277_87-05_daily.csv"), col_names = TRUE, skip=1))
outPath <- file.path("LARFlows", "Disaggregate Flows", "F277_87-18_hourly.csv")

# Some of the rows are originally NA for Mean flows.  This replaces the NA values with 0.
# dailyData <- dailyDataWithNA %>% rowwise() %>% mutate(Mean = (function(m) {
#  if (is.na(m)) 0 else m
# })(Mean))

findMid <- function(start, end, mean) {
  # Determine what the midpoint value needs to be to achieve the desired mean,
  # given the start and end values.  Since the first half of the day is 13 hours and the
  # second only 11 (13:00-23:00), the second half is weighted less.
  # mean = ((start + mid) / 2 * 13 + (end + mid) / 2 * 11) / 24 = (start * 13/2 + end * 11/2 + mid * 12) / 24
  # Thus mid = (1/12) * (24 * mean - (start * 13/2 + end * 11/2))
  (1/12) * (24 * mean - (start * 13/2 + end * 11/2))
}

makeBounds <- function(yesterday, today, tomorrow) {
  # Given the mean flows for the three days, determine today's flows
  # at midnight (start), noon (mid), and the next midnight (end).
  start <- if (yesterday > today) today else yesterday
  end <- if (tomorrow > today) today else tomorrow
  mid <- findMid(start, end, today)
  c(
    start = start,
    mid = mid,
    end = end
  )
}

makeRange <- function(start, mid, end) {
  # Given the values at midnight, noon, and the next midnight, make the
  # range of hourly values.  To avoid duplication, the next midnight is omitted.
  startMid <- mid - start
  midEnd <- end - mid
  startHours <- (0:12)/12 * startMid + start
  endHours <- (1:11)/12 * midEnd + mid
  c(startHours, endHours)
}

dailyToHourly <- function(yesterday, today, tomorrow) {
  if (is.na(today)) {
    # If today is missing, return NA
    replicate(24, NA)
  } else {
    # If the day before/after is missing, assume the same as today
    yesterday <- if (is.na(yesterday)) today else yesterday
    tomorrow <- if (is.na(tomorrow)) today else tomorrow
    bounds <- makeBounds(yesterday, today, tomorrow)
    makeRange(bounds[1], bounds[2], bounds[3])
  }
}

withYesterdayTomorrow <- function(daily) {
  # Apparently it got grouped somewhere, which messes up lag and lead.  The ungroup() fixes it.
  daily %>% ungroup() %>% mutate(yesterday = lag(Mean), tomorrow=lead(Mean))
}

withHours <- function(dailyYT) {
  # Adds a column for each hour with the appropriate data, then converts those columns into a "Time" column
  # and a "Flow" column, before removing the yesterday and tomorrow columns.
  dailyYT %>% 
    rowwise() %>%
    mutate(`0:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[1],
         `1:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[2],
         `2:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[3],
         `3:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[4],
         `4:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[5],
         `5:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[6],
         `6:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[7],
         `7:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[8],
         `8:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[9],
         `9:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[10],
         `10:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[11],
         `11:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[12],
         `12:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[13],
         `13:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[14],
         `14:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[15],
         `15:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[16],
         `16:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[17],
         `17:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[18],
         `18:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[19],
         `19:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[20],
         `20:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[21],
         `21:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[22],
         `22:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[23],
         `23:00:00` = dailyToHourly(yesterday, Mean, tomorrow)[24]
         ) %>%
      gather(Time, Flow, `0:00:00`:`23:00:00`) %>% # convert columns to rows
      select(Date, Time, Flow, Qual) %>%
      arrange(mdy(Date))
}

prepareData <- function(daily = dailyData) {
  daily %>%
    withYesterdayTomorrow %>%
    withHours
}

compareData <- function(daily = dailyData, hourly = prepareData()) {
  # Data to compare means and ensure they are correct
  means <- hourly %>% group_by(Date) %>% summarize(mean(Flow))
  inner_join(daily, means, by="Date") %>% mutate(pctDiff = (Mean - `mean(Flow)`) / (if (Mean > 0) Mean else 1) * 100)
}

randomTrials <- function(size = 10000) {
  # Generate random daily data to test the error of generated hourly data
  # Returns the maximum percent error
  daily <- replicate(size, sample(3, 100, replace = TRUE))
  # apply dailyToHourly to each column of daily and return the absolute percent difference
  # between the mean of the hourly data and the daily mean
  pctDiff <- apply(daily, 2, function(v) {abs(mean(dailyToHourly(v[1], v[2], v[3])) - v[2]) / v[2] * 100})
  max(pctDiff)
}

output <- function(data = prepareData(), path=outPath, append=TRUE) {
  # Write the data
  # If appending, it may be useful to go sort the spreadsheet so that
  # it is ordered by date.  Otherwise this data will simply be after the old data.
  # In this case it goes 2005-2018 and then 1987-2005 without sorting, though
  # each of those blocks is sorted.
  write_csv(data, path, na="", append=append)
}