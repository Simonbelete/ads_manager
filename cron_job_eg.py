import os

from crontab import CronTab

cwd = os.getcwd() # Current directory 

cron = CronTab(user=True)
job = cron.new(command=f'python3 {cwd}/cronjob1.py > /{cwd}/logs/cron1.log 2>&1')
job.minute.every(1)
job = cron.new(command=f'python3 {cwd}/cronjob2.py > /{cwd}/logs/cron2.log 2>&1')
job.minute.every(1)
job = cron.new(command=f'python3 {cwd}/cronjob3.py > /{cwd}/logs/cron3.log 2>&1')
job.minute.every(1)
job = cron.new(command=f'python3 {cwd}/cronjob4.py > /{cwd}/logs/cron4.log 2>&1')
job.minute.every(1)
cron.write()