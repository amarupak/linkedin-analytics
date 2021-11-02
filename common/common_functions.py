import os
import math
import datetime
from common.auth import read_configuration_file

# Import parameters
params = read_configuration_file()
prev_months = params['previous_months']
output_dir = params['output_dir']


def get_filepath(filename):
    filepath = os.path.join(output_dir, filename)
    return filepath


# Convert datetime to epoch
def epoch_time(date):
    return math.trunc(date.timestamp()) * 1000


# Method to get date relative to the current timestamp based on delta
def start_date(date, delta):
    m, y = (date.month + delta) % 12, date.year + (date.month + delta - 1) // 12
    if not m: m = 12
    d = min(date.day, [31,
                       29 if y % 4 == 0 and (not y % 100 == 0 or y % 400 == 0) else 28,
                       31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date.replace(day=d, month=m, year=y)


# Get start epoch from end date
def epoch_start(EndDate):
    StartDate = start_date(EndDate, prev_months)
    start_epoch = epoch_time(StartDate)
    return start_epoch


# Define time variables for direct use in the program
EndDate = datetime.datetime.now()
start_epoch = epoch_start(EndDate)
end_epoch = epoch_time(EndDate)


# Method to write dataframe to csv for a given path
def write_to_csv(df, path):
    df.to_csv(path, index=False, header=True)


# Method to log console output
def logOutput(filename, log):
    f = open(filename, "a")
    f.write("{0} -- {1}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), log))
    f.close()
