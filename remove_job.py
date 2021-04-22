import os

from crontab import CronTab

cron = CronTab(user=True)

cwd = os.getcwd() # Current directory 

cmd = f'python3 {cwd}/cronjob2.py > /{cwd}/logs/cron2.log 2>&1'

job = cron.find_command(cmd)

cron.remove(job)

# Writes content to crontab
cron.write()