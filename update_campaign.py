#!/usr/bin/env python
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This example updates a campaign.

To get campaigns, run get_campaigns.py.
"""


import argparse
import datetime
import sys

from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers


_DATE_FORMAT = "%Y%m%d"


def main(client, customer_id, campaign_id):
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


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage('./google-ads.yaml')

    try:
        main(googleads_client, "7969829039", "12737097284")
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
        sys.exit(1)