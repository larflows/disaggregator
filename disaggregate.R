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

# Default paths.
# This assumes LARFlows is in the working directory.
dataPath <- file.path("LARFlows", "Disaggregate Flows", "F277_87-05_daily.csv")
outPath <- file.path("LARFlows", "Disaggregate Flows", "F277_87-18_hourly.csv")

runCmd <- function() {
  # Run from command line
  # Not fully tested yet.
  args <- commandArgs(TRUE)
  if (length(args) == 0) { # Default: run test
    print(paste("Test percent error with 10,000 trials of random data:", randomTrials(), sep=" "), quote=FALSE)
    print.factor("Refer to README.md for usage information.")
  } else {
    if (args[1] != "--in") { # No input file specified
      if (args[1] != "--test") { # Test not specified either
        print.factor("Error: first argument must be either --in <file path> or --test.  Refer to README.md for usage information.")
      } else print(paste("Test percent error with 10,000 trials of random data:", randomTrials(), sep=" "), quote=FALSE)
    } else { # Input file specified
      if (length(args) == 3 && args[3] == "--test") { # 3: --in <path> --test
        daily <- getDailyData(args[2])
        maxErr <- compareData(daily, prepareData(daily)) %>% ungroup() %>% summarize(max(abs(pctDiff))) # Returning NA currently
        print(paste("Percent error with provided data:", maxErr, sep=" "), quote=FALSE)
      } else if (length(args) >= 4 && args[3] == "--out") { # 4: --in <path> --out <path>, maybe --append <T|F>
          daily <- getDailyData(args[2])
          out <- args[4]
          append <- if (length(args) == 6 && args[5] == "--append") { args[6] == "T" } else TRUE
          output(prepareData(daily), out, append)
        }
    }
  }
}

# Read the data in appropriate format.
getDailyData <- function(path = dataPath, skip = 1) {
  # Skip: number of rows before column headers
  as_tibble(read_csv(path, col_names = TRUE, skip = skip))
}

findMid <- function(start, end, mean, steps = 24, midpoint = 13) {
  # Determine what the midpoint value needs to be to achieve the desired mean,
  # given the start and end values.  Since the first half of the day is 13 hours and the
  # second only 11 (13:00-23:00), the second half is weighted less.
  # mean = ((start + mid) / 2 * midpoint + (end + mid) / 2 * (steps - midpoint)) / steps = (start * midpoint/2 + end * (steps-midpoint)/2 + mid * 12) / steps
  # Thus mid = (2/steps) * (steps * mean - (start * midpoint/2 + end * (steps-midpoint)/2))
  # Steps is the number of blocks (e.g. hours); midpoint is which block is in the middle (noon is the 13th)
  (2/steps) * (steps * mean - (start * midpoint/2 + end * (steps-midpoint)/2))
}

makeBounds <- function(prev, current, nxt) {
  # Given the mean flows for the three days, determine today's flows
  # at midnight (start), noon (mid), and the next midnight (end).
  start <- if (prev > current) current else prev
  end <- if (nxt > current) current else nxt
  mid <- findMid(start, end, current)
  c(
    start = start,
    mid = mid,
    end = end
  )
}

makeRange <- function(start, mid, end, steps=24, midpoint=13) {
  # Given the values at midnight, noon, and the next midnight, make the
  # range of hourly values.  To avoid duplication, the next midnight is omitted.
  startMid <- mid - start
  midEnd <- end - mid
  startSteps <- (0:(steps/2))/(steps/2) * startMid + start
  endSteps <- (1:(steps/2 - 1))/(steps/2) * midEnd + mid
  c(startSteps, endSteps)
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

withPrevNext <- function(daily) {
  # Apparently it got grouped somewhere, which messes up lag and lead.  The ungroup() fixes it.
  daily %>% ungroup() %>% mutate(yesterday = lag("Mean"), tomorrow=lead("Mean"))
}

withHours <- function(originalPN) {
  # Adds a column for each hour with the appropriate data, then converts those columns into a "Time" column
  # and a "Flow" column, before removing the yesterday and tomorrow columns.
  originalPN %>% 
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

prepareData <- function(daily = getDailyData()) {
  daily %>%
    withPrevNext %>%
    withHours
}

compareData <- function(daily = getDailyData(), hourly = prepareData()) {
  # Data to compare means and ensure they are correct
  means <- hourly %>% group_by(Date) %>% summarize(mean(Flow))
  inner_join(daily, means, by="Date") %>% rowwise() %>% mutate(pctDiff = (Mean - `mean(Flow)`) / (if (!is.na(Mean) && Mean > 0) Mean else 1) * 100)
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

runCmd()