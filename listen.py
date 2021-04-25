import os
import sys
import time
import select
import psycopg2
import socket
import datetime

from logger import logger
from dotenv import load_dotenv
from blender import bcolors
from configuration import CONFIG
from crontab import CronTab
from google_campaign import update_google_campaign

class Listen(object):
    def __init__(self):
        load_dotenv()  # take environment variables from .env.
        self.NOTIFY_NAME = 'table_update' # change this to the postgresql notification name
        self.param = {
            'host': os.getenv('DB_HOST'),                               # type: string, Database Host Name from `.env` file
            'port': os.getenv('DB_PORT'),                               # type: string, Database Host Name from `.env` file                                              
            'user': os.getenv('DB_USER'),                               # type: string, Database Host Name from `.env` file
            'password': os.getenv('DB_PASSWORD'),                       # type: string, Database Host Name from `.env` file
            'database': os.getenv('DB_DATABSE')                         # type: string, Database Host Name from `.env` file
        }
        self.cron = CronTab(user=True)


    def connect(self):
        """ Connect to postgresql.
            On the event of no connection close the script.

            Returns
            -------
                void
        """
        try:
            print(f'{bcolors.OKBLUE}INFO: Connecting to the PostgreSQL databse... {bcolors.ENDC}')
            self.connection =  psycopg2.connect(**self.param)

            if self.connection != None:
                print(f'{bcolors.OKGREEN}SUCESS: Successful connected to PostgreSQL{bcolors.ENDC}')
                self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        except (psycopg2.DatabaseError, psycopg2.OperationalError) as ps_error:
            print(f'{bcolors.FAIL}ERROR: Failed to connect to PostgreSQL{bcolors.ENDC}')
            logger.exception(str(ps_error))
            
            # Wait sometime before re-connecting
            print(f'{bcolors.HEADER}INFO: Retrying...{bcolors.ENDC}')
            time.sleep(10) # Wait 10sec
            self.connect()


    def get_campaign_detail(campaign_id):
        query = f"""
            SELECT
                booster_ads_app.owner_ad_id as owner_ad_id,
                booster_ads_app.google_campaign_id as google_campaign_id,
                booster_ads_app.facebook_campaign_id as facebook_campaign_id,
                booster_ads_app.campaign_time as campaign_time,
                booster_ads_app.is_active as is_active,
                booster_ads_app.expiry_date as expiry_date,
                onair.ad_id as ad_id
                FROM booster_ads_app
                INNER JOIN onair ON booster_ads_app.owner_ad_id = onair.ad_id
                WHERE booster_ads_app.is_active = true
                    AND booster_ads_app.expiry_date >= NOW()
                    AND onair.ad_id = '{campaign_id}'
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result


    def run_forever(self):
        """ """
        try:
            cwd = os.getcwd() # Current directory
            # Connect to postgresql databse
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f'LISTEN {self.NOTIFY_NAME};')

            print(f'{bcolors.OKBLUE}INFO: Waiting for notifications on channel {self.NOTIFY_NAME} {bcolors.ENDC}')

            while True:
                # This is a blocking call that will return if and when any file descriptor in the list has new data
                # We see that there is fresh information on our connection, better poll and read!
                select.select([self.connection], [], [], 10) # Timeout 10sec
                # Needed to get the actual message
                self.connection.poll()
                if self.connection.notifies != []:
                    # Pop notification from list
                    notify = self.connection.notifies.pop(0)
                    j_notify = json.loads(notify.payload)
                    new_campaign = self.get_campaign_detail(j_notify['ad_id'])
                    print(notify)

                    # Create cronjob with the new campaign
                    # Since google allow ads to run a minimum of 1 day, set the end date to
                    # a minimum of 1 day else it wont work 
                    end_second = new_campaign[3]
                    if int(new_campaign[3]) < 60: # 1 Minute
                        end_second = 60

                    # Start running the campign
                    update_google_campaign(customer_id=new_campaign[0], campaign_id=new_campaign[1], campaign_time=end_second, status=CONFIG.get('DEFAULT', 'START_CAMPAIGN'))

                    # Set a cron job
                    # cmd = f"python{CONFIG.get('DEFAULT', 'PYTHON_VERSION')} {cwd}/cli/stop_google_campaign.py -c={campaign[0]} -i={campaign[1]} -cw={cwd} > {cwd}/logs/{campaign[0]}-{campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1 && python3 {cwd}/cli/remove_cron_job.py -c={campaign[0]} -i={campaign[1]} -cw={cwd}"
                    # cmd = f"python{CONFIG.get('DEFAULT', 'PYTHON_VERSION')} {cwd}/cli/stop_google_campaign.py -c={campaign[0]} -i={campaign[1]} -cw={cwd} > {cwd}/logs/{campaign[0]}-{campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1 && python3 {cwd}/cli/remove_cron_job.py -c={campaign[0]} -i={campaign[1]} -cw={cwd}"
                    # TODO: only create cronjob if it doen't exist
                    cmd = f"python3 {cwd}/cli/stop_google_campaign.py -c={new_campaign[0]} -i={new_campaign[1]} -cw={cwd} > {cwd}/logs/{new_campaign[0]}-{new_campaign[1]}.$(date +%Y-%m-%d_%H:%M).log 2>&1"

                    job = self.cron.new(command=cmd)

                    # Calculate cronjob start date
                    start_time = datetime.datetime.now()
                    end_time = start_time + datetime.timedelta(seconds=int(end_second))

                    job.setall(end_time)
                    self.cron.write()

        except (socket.error, select.error) as s_ex:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            logger.exception(str(s_ex))
            print(f'{bcolors.FAIL}ERROR: Error occured in waiting for notifications {bcolors.ENDC}')
            # restart it
            self.run_forever()
        except KeyboardInterrupt as k_ex:
            print('Caught Keyboard Interrupt...')
            logger.exception('Caught keyboard interrupt from postgresql listening')
            sys.exit(1)
        except Exception as ex:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            print(f'{bcolors.FAIL}Error: Unknown error occured{bcolors.ENDC}\n\n')
            print(f'{bcolors.WARNING}*** Restarting Server ***{bcolors.ENDC}\n\n')
            logger.exception(str(ex))
            self.run_forever()
        finally:
            # Inorder to limit the number of open connection to 1
            # close the open connection
            self.connection.close()
            