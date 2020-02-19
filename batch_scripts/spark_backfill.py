import datetime
import argparse
import json

def days_after(dt, days_after):
  day = (datetime.datetime.strptime(dt, '%Y-%m-%d') + datetime.timedelta(days=days_after)).date()
  return day.strftime("%Y-%m-%d")

def back_fill(file_name, start_date, jobs_w_days, mode='w', days=90, spark_settings=None):
  with open(file_name, mode) as ff:
    for i in range(0, days):
      _date = days_after(start_date, i)
      for job_name, days in jobs_w_days.items():
        date = days_after(_date, days)
        ff.write("spark-submit ")
        if spark_settings is not None:
          for prop, value in spark_settings.items():
            ff.write(f"--conf {prop}={value} ")
        ff.write(f"{job_name} -e prd -d {date}\n")
  ff.close()
  return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser("")
    parser.add_argument("--file_name", "-fn", help="Your File Name", type=str, required=True)
    parser.add_argument("--start_date", "-st", help="", type=str, required=True)
    parser.add_argument("--mode", "-m", help="", type=str, required=False)
    parser.add_argument("--days", "-ds", help="", type=int, required=False)
    parser.add_argument("--jobs_with_days", "-jwd", help="", type=json.loads, required=True)
    parser.add_argument("--spark_settings", "-ss", help="", type=json.loads, required=False)
    args = parser.parse_args()
    optional_args = {}
    if args.mode:
      optional_args['mode'] = args.mode
    if args.days:
      optional_args['days'] = args.days
    if args.spark_settings:
      optional_args['spark_settings'] = args.spark_settings
    back_fill(args.file_name, args.start_date, args.jobs_with_days, **optional_args)
    print("JOB FINISHED")
