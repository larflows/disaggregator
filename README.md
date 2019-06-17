# Disaggregator
Disaggregates data by inventing data at smaller intervals to maintain the same mean.  The original intent is to disaggregate daily mean flow data into hourly, but minor code modifications or, possibly in the future, changes in configuration options could let it be used for other disaggregation tasks.  It is currently set up with the endpoints at midnight and the midpoint at noon.

## Algorithm
The algorithm produces the disaggregated data in linear chunks so that the mean over an interval is equal to the actual mean.  The point at each end of an interval is set to be the lower of the two means on either side, then the midpoint is set such that the mean over the interval is correct, and the remainder of the data is linearly interpolated between the midpoint and the endpoints.  In testing (from converting daily to hourly), the differences between the actual mean and the mean of the generated data have been on the order of 10^-14 of the actual mean, presumably explained by rounding errors.

## Data Format
The program is designed to work with CSVs.  Under the current setup, the input CSV needs to have the column headers Date, Mean, and Qual (the latter an artifact of the spreadsheets it was designed for) on the second line (the first line is ignored, again an artifact).  Date should be in the format m/d/y (or a similar format in the same order) and mean should be the numeric daily mean.

The output has the columns Date, Hour, Flow, and Qual.  Date is in the same format as above.  Hour is in h:mm:ss format.  Flow is the numeric hourly flow (or otherwise hourly) data.  The output can either be a standalone CSV or can be appended to another CSV; in the latter case, the column headers will be omitted.

The input and output can also be R tibbles containing the same data.  A tibble can simply be passed to the `prepareData` function, which will itself produce a tibble.  The input tibble should have dates as strings, not dates.
