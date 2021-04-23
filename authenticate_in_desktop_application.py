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
"""This example creates an OAuth 2.0 refresh token for the Google Ads API.

This illustrates how to step through the OAuth 2.0 native / installed
application flow.

It is intended to be run from the command line and requires user input.
"""


import argparse

from google_auth_oauthlib.flow import InstalledAppFlow


SCOPE = "https://www.googleapis.com/auth/adwords"


def main(client_secrets_path):
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_path, scopes = SCOPE
    )

    flow.run_console()

    print("Access token: %s" % flow.credentials.token)
    print("Refresh token: %s" % flow.credentials.refresh_token)


if __name__ == "__main__":
    main('./client_secret.json')