import argparse

import os
import argparse
import datetime

from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers

cwd = os.getcwd() # Current directory 
googleads_client = GoogleAdsClient.load_from_storage(f'{cwd}/google-ads.yaml')

def update_google_campaign(customer_id, campaign_id):
    global googleads_client, _DATE_FORMAT
    client = googleads_client
    campaign_service = client.get_service("CampaignService")
    # Create campaign operation.
    campaign_operation = client.get_type("CampaignOperation")
    campaign = campaign_operation.update

    campaign_status = campaign_status_enum = client.get_type(
                            "CampaignStatusEnum"
                        ).CampaignStatus.PAUSED

    campaign.status = campaign_status

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


parser = argparse.ArgumentParser(
    description="Updates the given campaign for the specified customer."
    )
# The following argument(s) should be provided to run the example.
parser.add_argument(
    "-c",
    "--customer_id",
    type=str,
    required=True,
    help="The Google Ads customer ID.",
)
parser.add_argument(
    "-i", "--campaign_id", type=str, required=True, help="The campaign ID."
)
args = parser.parse_args()


update_google_campaign(args.customer_id, args.campaign_id)
