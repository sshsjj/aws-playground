import datetime
import argparse
import json

#Example:
#python testtesttest.py -fn s3-batch-write-eventlog-rv-score-daily.sh -st 2020-02-20 -ib s3-recipient-verification-scores-usw2-tst -ik data/eventlog/dt=2019-08-02/test.json -ob s3-recipient-verification-scores-usw2-tst -ok data/eventlog -d 180

def days_after(dt, days_after):
  day = (datetime.datetime.strptime(dt, '%Y-%m-%d') + datetime.timedelta(days=days_after)).date()
  return day.strftime("%Y-%m-%d")

def s3_batch_write(file_name, start_date, in_bucket, in_key, out_bucket, out_key, mode='w', days=180):
  with open(file_name, mode) as ff:
    for i in range(0, days):
      _date = days_after(start_date, i)
      ff.write(f"aws s3 cp s3://{in_bucket}/{in_key} s3://{out_bucket}/{out_key}/dt={_date}/")
      ff.write("\n")
  ff.close()
  return None

if __name__ == "__main__":
  parser = argparse.ArgumentParser("")
  parser.add_argument("--file_name", "-fn", help="Your File Name", type=str, required=True)
  parser.add_argument("--start_date", "-st", help="", type=str, required=True)
  parser.add_argument("--in_bucket", "-ib", help="", type=str, required=True)
  parser.add_argument("--in_key", "-ik", help="", type=str, required=True)
  parser.add_argument("--out_bucket", "-ob", help="", type=str, required=True)
  parser.add_argument("--out_key", "-ok", help="", type=str, required=True)
  parser.add_argument("--days", "-d", help="", type=int, required=False)
  args = parser.parse_args()
  optional_args = {}
  if args.days:
    optional_args['days'] = args.days
  s3_batch_write(args.file_name, args.start_date, args.in_bucket, args.in_key, args.out_bucket, args.out_key, **optional_args)
  print("Finished Generating Batch Scripts!")
  print(f"Please Find Scripts in {args.file_name}") 
