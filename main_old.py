import os
import json
import psycopg2
import select
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

## Const Values
NOTIFY_NAME = 'table_update'
_DATE_FORMAT = "%Y%m%d"


def update_google_campaign(client, customer_id, campaign_id, start_time, end_time):
    campaign_service = client.get_service("CampaignService")
    # Create campaign operation.
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.update

    campaign.name = 'My py campaign'

    start_time = datetime.date.today() + datetime.timedelta(days=4)
    campaign.start_date = datetime.date.strftime(start_time, _DATE_FORMAT)
    end_time = start_time + datetime.timedelta(weeks=6)
    campaign.end_date = datetime.date.strftime(end_time, _DATE_FORMAT)

    campaign.resource_name = campaign_service.campaign_path(
        customer_id, campaign_id
    )

    campaign_status_enum = client.get_type(
        "CampaignStatusEnum"
    ).CampaignStatus.PAUSED

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


def get_google_campaign(client, customer_id, campaign_id):
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          campaign.status,
          campaign.id,
          campaign.name,
          campaign.start_date,
          campaign.end_date,
          campaign.serving_status
        FROM campaign
        WHERE campaign.id = {campaign_id}"""

    # Issues a search request using streaming.
    response = ga_service.search_stream(customer_id=customer_id, query=query)

    all_campaigns = []

    for batch in response:
        for row in batch.results:
            all_campaigns.append(row)

    return all_campaigns

def run_google_campaign_for(customer_id, campaign_id, how_long)
    """Run campaigns for limited time.
    Accoriding to the google ads api the campaign closed automatically
    accoriding to the given end_date, so no need to close the campaign after some time

    Attributes
    ----------
    customer_id : String
        Account found under the managers
        Example:
            - 123456789
        
    campaign_id: String

    how_long: String|int
        Time in seconds
    """
    ## Check if campaign is running or paused, only run paused campaigns
    ## GoogleAdsClient will read the google-ads.yaml configuration file in the
    ## home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage('./google-ads.yaml')

    try:
        campaign = get_google_campaign(googleads_client, customer_id = customer_id, campaign_id = campaign_id)
        
        if campaign != []:
            ## Check the status of the campaing
            ## Run only `PAUSED` campaing
            if campaign[0]['status'] == 'PAUSED':
                start_date = # From now


    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'	Error with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        #sys.exit(1)


if __name__ == '__main__':
    """ Connect to the PostgreSQL database server """
    connection = None
    params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABSE')
    }
    try:
        ## Connect to the PostgreSQL server
        print('Connecting to the PostgreSQL databse...')
        connection = psycopg2.connect(**params)
        ## Open a cursor to perform database operations
        cursor = connection.cursor()
        ## Select all active and non expired campaigns
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
        ## Execute query
        cursor.execute(query)
        records = cursor.fetchall()
        for row in records:
            print(row[5])

        ## Listen to listen_onair changes
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor.execute(f'LISTEN {NOTIFY_NAME};')
        print(f"Waiting for notifications on channel '{NOTIFY_NAME}'")
        while True:
            ## This is a blocking call that will return if and when any file descriptor in the list has new data
            ## We see that there is fresh information on our connection, better poll and read!
            select.select([connection],[],[],1)
            ## Needed to get the actual message
            connection.poll()
            if connection.notifies != []:
                ## Pop notification from list
                notify = connection.notifies.pop(0)
                j_notify = json.loads(notify.payload)
                print(notify)
                ## Get the campaign id and owner id
                n_query = f"""
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
                            AND onair.ad_id = {j_notify['ad_id']}
                """
                ## Since to not affect the notification create new cursore
                n_cursor = connection.cursor()
                n_cursor.execute(n_query)
                c_result = n_cursor.fetchOne()
                ## Run campaign for the given time
                
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed')