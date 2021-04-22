
class OnAir(object):
    # Select non expired campaigns with status `active`
    QUERY = """
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

    def __init__(self, connection):
        self.CONNECTION = connection

    def get_campains(self):
        cursor = self.CONNECTION.cursor()
        cursor.execute(self.QUERY)
        return cursor.fetchall()

