import os

from crontab import CronTab

cwd = os.getcwd() # Current directory 

cron = CronTab(user=True)
job = cron.new(command=f'python3 {cwd}/cronjob1.py > /{cwd}/logs/cron.log 2>&1')
job.minute.every(1)
cron.write()