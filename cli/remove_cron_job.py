import os
import argparse

from crontab import CronTab

def remove_cron_job(customer_id, campaign_id, cwd):
    cron = CronTab(user=True)

    cmd = f"python3 {cwd}/cli/stop_google_campaign.py -c={customer_id} -i={campaign_id} -cw={cwd} > {cwd}/logs/{customer_id}-{campaign_id}.$(date +%Y-%m-%d_%H:%M).log 2>&1 && python3 {cwd}/cli/remove_cron_job.py -c={customer_id} -i={campaign_id} -cw={cwd}"
    print(cmd)
    job = cron.find_command(cmd)
    cron.remove(job)
    # Writes content to crontab
    cron.write()


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

parser.add_argument(
    "-cw",
    "--cwd",
    type=str,
    required=True,
    help="Repository directory"
)

args = parser.parse_args()

remove_cron_job(args.customer_id, args.campaign_id, args.cwd)