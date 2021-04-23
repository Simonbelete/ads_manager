import os
import argparse
import datetime

from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers

from configuration import CONFIG

cwd = os.getcwd() # Current directory 
# TODO: Read from ini file CONFIG.get('DEFAULT', 'DATE_FORMAT')
_DATE_FORMAT = '%Y%m%d'
googleads_client = GoogleAdsClient.load_from_storage(f'{cwd}/google-ads.yaml')

def update_google_campaign(customer_id, campaign_id, campaign_time, status):
    global googleads_client, _DATE_FORMAT
    client = googleads_client
    campaign_service = client.get_service("CampaignService")
    # Create campaign operation.
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.update

    campaign_status = None
    if status == CONFIG.get('DEFAULT', 'PAUSE_CAMPAIGN'):
        campaign_status = campaign_status_enum = client.get_type(
                            "CampaignStatusEnum"
                          ).CampaignStatus.PAUSED
    elif status == CONFIG.get('DEFAULT', 'START_CAMPAIGN'):
        campaign_status = campaign_status_enum = client.get_type(
                            "CampaignStatusEnum"
                          ).CampaignStatus.ENABLED

    campaign.status = campaign_status

    start_time = datetime.date.today() + datetime.timedelta(days=0)
    # campaign.start_date = datetime.date.strftime(start_time, _DATE_FORMAT)
    end_time = start_time + datetime.timedelta(seconds=campaign_time)
    campaign.end_date = None

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