import os
import db

from crontab import CronTab
from google_campaign import update_google_campaign
from configuration import CONFIG
from onair import OnAir

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
        if int(campaign[3]) < 86400: # 86400 = 1 day
            end_second = 86400

        # Start running the campign
        update_google_campaign(customer_id=campaign[0], campaign_id=campaign[1], campaign_time=end_second, status=CONFIG.get('DEFAULT', 'START_CAMPAIGN'))

        # Set a cron job
        cmd = f"python{CONFIG.get('DEFAULT', 'PYTHON_VERSION')} {cwd}/"
        job = cron.new(command=cmd)
