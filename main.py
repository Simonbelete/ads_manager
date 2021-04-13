import os
import sys
import time
import json
import select
import psycopg2
import argparse
import datetime

from dotenv import load_dotenv
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers


load_dotenv()  # take environment variables from .env.

"""
    Global vars
"""
CONNECTION = None                                                   # type: psycopg2.connection, Holds Postgresql connection 
PARAM = {
        'host': os.getenv('DB_HOST'),                               # type: string, Database Host Name from `.env` file
        'port': os.getenv('DB_PORT'),                               # type: string, Database Host Name from `.env` file                                              
        'user': os.getenv('DB_USER'),                               # type: string, Database Host Name from `.env` file
        'password': os.getenv('DB_PASSWORD'),                       # type: string, Database Host Name from `.env` file
        'database': os.getenv('DB_DATABSE')                         # type: string, Database Host Name from `.env` file
    }
NOTIFY_NAME = 'table_update'                                        # type: string, LISTEN table_update
_DATE_FORMAT = "%Y%m%d"                                             # type: string
INTERVAL = 1                                                        # type: int, interval time to check the campigns
SECONDS = 0                                                         # type: int
RUNNABLE_CAMPAIGNS = []
PAUSE_CAMPAIGN = 1
START_CAMPAIGN = 2

googleads_client = GoogleAdsClient.load_from_storage('./google-ads.yaml')

def connect():
    """ Connect to postgresql.
        On the event of no connection close the script.

        Returns
        -------
            void
    """
    global CONNECTION
    global PARAM
    try:
        print('Connecting to the PostgreSQL databse...')
        CONNECTION = psycopg2.connect(**PARAM)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)


def update_google_campaign(customer_id, campaign_id, campaign_time, status):
    global googleads_client
    client = googleads_client
    campaign_service = client.get_service("CampaignService")
    # Create campaign operation.
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.update

    campaign_status = None

    if status == PAUSE_CAMPAIGN:
        campaign_status = campaign_status_enum = client.get_type(
                            "CampaignStatusEnum"
                          ).CampaignStatus.PAUSED
    elif status == START_CAMPAIGN:
        campaign_status = campaign_status_enum = client.get_type(
                            "CampaignStatusEnum"
                          ).CampaignStatus.ENABLED

    campaign.status = campaign_status

    start_time = datetime.date.today() + datetime.timedelta(days=0)
    campaign.start_date = datetime.date.strftime(start_time, _DATE_FORMAT)
    end_time = start_time + datetime.timedelta(seconds=campaign_time)
    campaign.end_date = datetime.date.strftime(end_time, _DATE_FORMAT)

    campaign.resource_name = campaign_service.campaign_path(
        customer_id, campaign_id
    )
    
    campaign.network_settings.target_search_network = False
    # Retrieve a FieldMask for the fields configured in the campaign.
    client.copy_from(
        campaign_operation.update_mask,
        protobuf_helpers.field_mask(None, campaign._pb),
    )

    campaign_response = campaign_service.mutate_campaigns(
        customer_id=customer_id, operations=[campaign_operation]
    )

    print(f"Updated campaign {campaign_response.results[0].resource_name}.")


def read_onair_campaigns():
    global CONNECTION, RUNNABLE_CAMPAIGNS, SECONDS
    # Select non expired campaigns with status `active`
    query = """
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
        """
    cursor = CONNECTION.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    for row in records:
        RUNNABLE_CAMPAIGNS.append(
            {
                'start_second': SECONDS,
                'details': row
            }
        )
        # Since google allow ads to run a minimum of 1 day, set the end date to
        # a minimum of 1 day else it wont work 
        end_second = row[3]
        if int(row[3]) < 86400: # 86400 = 1 day
            end_second = 86400

        # Start running the campign
        update_google_campaign(customer_id=row[0], campaign_id=row[1], campaign_time=end_second, status=START_CAMPAIGN)
    

def get_campaign_detail(campaign_id):
    global CONNECTION
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
    cursor = CONNECTION.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result


def main():
    global CONNECTION, NOTIFY_NAME, SECONDS, INTERVAL, RUNNABLE_CAMPAIGNS
    CONNECTION.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = CONNECTION.cursor()
    cursor.execute(f'LISTEN {NOTIFY_NAME};')
    print(f"Waiting for notifications on channel '{NOTIFY_NAME}'")
    while True:
        time.sleep(INTERVAL)
        # This is a blocking call that will return if and when any file descriptor in the list has new data
        # We see that there is fresh information on our connection, better poll and read!
        select.select([CONNECTION],[],[],1)
        # Needed to get the actual message
        CONNECTION.poll()
        if CONNECTION.notifies != []:
            # Pop notification from list
            notify = CONNECTION.notifies.pop(0)
            j_notify = json.loads(notify.payload)
            new_campaign = get_campaign_detail(j_notify['ad_id'])
            # Check if the new campaign exists and update it
            for i, row in enumerate(RUNNABLE_CAMPAIGNS):
                print(i)
                if row[0] == new_campaign[0]:
                    del RUNNABLE_CAMPAIGNS[i]
            ## Append the new campaign
            RUNNABLE_CAMPAIGNS.append(
                {
                    'start_second': SECONDS,
                    'details': new_campaign
                }
            )
 
        # Every {SECONDS} check if there is any campign to close
        for i, campaign in enumerate(RUNNABLE_CAMPAIGNS):
            if SECONDS == (campaign['start_second'] + int(campaign['details'][3])):
                # Stop running the campign, 
                # @param campaign_time doesn't matter what the value is
                update_google_campaign(customer_id=campaign['details'][0], campaign_id=campaign['details'][1], campaign_time=86400, status=PAUSE_CAMPAIGN)
                del RUNNABLE_CAMPAIGNS[i]
        print(f'Time {SECONDS}')
        print(RUNNABLE_CAMPAIGNS)
        SECONDS += INTERVAL

if __name__ == '__main__':
    connect()
    read_onair_campaigns()
    main()
    print(RUNNABLE_CAMPAIGNS)