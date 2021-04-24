import os
import db
import datetime

from crontab import CronTab
from google_campaign import update_google_campaign
from configuration import CONFIG
from onair import OnAir
from listen import Listen

if __name__ == '__main__':
    cwd = os.getcwd() # Current directory 
    cron = CronTab(user=True)

    connection = db.connect()
    # onair table
    onair = OnAir(connection)
    # On first run start campings and create cronjob
    campaigns = onair.get_campains()
    
    for campaign in campaigns:
        # Since google allow ads to run a minimum of 1 day, set the end date to
        # a minimum of 1 day else it wont work 
        end_second = campaign[3]
        if int(campaign[3]) < 60: # 1 Minute
            end_second = 60

        # Start running the campign
        update_google_campaign(customer_id=campaign[0], campaign_id=campaign[1], campaign_time=end_second, status=CONFIG.get('DEFAULT', 'START_CAMPAIGN'))

        # Set a cron job
        # cmd = f"python{CONFIG.get('DEFAULT', 'PYTHON_VERSION')} {cwd}/cli/stop_google_campaign.py -c={campaign[0]} -i={campaign[1]} -cw={cwd} > {cwd}/logs/{campaign[0]}-{campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1 && python3 {cwd}/cli/remove_cron_job.py -c={campaign[0]} -i={campaign[1]} -cw={cwd}"
        # cmd = f"python{CONFIG.get('DEFAULT', 'PYTHON_VERSION')} {cwd}/cli/stop_google_campaign.py -c={campaign[0]} -i={campaign[1]} -cw={cwd} > {cwd}/logs/{campaign[0]}-{campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1 && python3 {cwd}/cli/remove_cron_job.py -c={campaign[0]} -i={campaign[1]} -cw={cwd}"
        # TODO: only create cronjob if it doen't exist
        cmd = f"python3 {cwd}/cli/stop_google_campaign.py -c={campaign[0]} -i={campaign[1]} -cw={cwd} > {cwd}/logs/{campaign[0]}-{campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1"

        job = cron.new(command=cmd)

        # Calculate cronjob start date
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=int(end_second))

        job.setall(end_time)
        cron.write()

    ##
    ##
    ## Start listning for postgresql notification
    notify_listen = Listen()
    notify_listen.run_forever()