DROP TABLE booster_ads_app, onair;

CREATE TABLE booster_ads_app (
    id SERIAL PRIMARY KEY,
    owner_ad_id VARCHAR(20),
    google_campaign_id VARCHAR(20),
    facebook_campaign_id VARCHAR(20),
    campaign_time VARCHAR(20),
    is_active BOOLEAN NOT NULL,
    expiry_date TIMESTAMP
);

INSERT INTO booster_ads_app 
    (owner_ad_id, google_campaign_id, facebook_campaign_id, campaign_time, is_active, expiry_date)
    VALUES 
        ('7969829039', '12737097284', 'f_11', '20', '1', '2021-05-20'),
        ('7969829039', '12737097284', 'f_11', '111', '0', '2020-03-27'),
        ('7969829039', '12737097284', 'f_11', '111', '1', '2020-03-27'),
        ('7969829039', '12737097284', 'f_11', '111', '1', '2020-03-27'),
        ('7969829039a', '12737097284', 'f_11', '10', '1', '2021-08-20');

    

CREATE TABLE onair (
    ad_id VARCHAR(20)
);

INSERT INTO onair
    (ad_id)
    VALUES
        ('7969829039');


CREATE OR REPLACE FUNCTION table_update_notify() RETURNS trigger AS $$
DECLARE
  id bigint;
  l_ad_id VARCHAR;
BEGIN
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    l_ad_id = NEW.ad_id;
  ELSE
    l_ad_id = OLD.ad_id;
  END IF;
  PERFORM pg_notify('table_update', json_build_object('onair', TG_TABLE_NAME, 'ad_id', l_ad_id, 'type', TG_OP)::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER onair_notify_update AFTER UPDATE ON onair FOR EACH ROW EXECUTE PROCEDURE table_update_notify();
CREATE TRIGGER onair_notify_insert AFTER INSERT ON onair FOR EACH ROW EXECUTE PROCEDURE table_update_notify();
CREATE TRIGGER onair_notify_delete AFTER DELETE ON onair FOR EACH ROW EXECUTE PROCEDURE table_update_notify();

DROP TRIGGER onair_notify_update ON onair;
DROP TRIGGER onair_notify_insert ON onair;
DROP TRIGGER onair_notify_delete ON onair;




SELECT * FROM booster_ads_app
    INNER JOIN onair ON booster_ads_app.owner_ad_id = onair.ad_id;


SELECT
    booster_ads_app.owner_ad_id as owner_ad_id,
    booster_ads_app.google_campaign_id as google_campaign_id,
    booster_ads_app.facebook_campaign_id as facebook_campaign_id,
    booster_ads_app.campaign_time as campaign_time,
    booster_ads_app.is_active as is_active,
    booster_ads_app.expiry_date as expiry_date,
    onair.ad_id as ad_id
    FROM booster_ads_appid
        INNER JOIN onair ON booster_ads_app.owner_ad_id = onair.ad_id
    WHERE booster_ads_app.is_active = true
        AND booster_ads_app.expiry_date >= NOW()


UPDATE onair
SET ad_id = '12'
WHERE ad_id = '11';










CREATE OR REPLACE FUNCTION onair_change() RETURNS trigger AS $$
DECLARE payload TEXT;
BEGIN
 	PERFORM pg_notify(
		'onair_changes',
		json_build_object(
			'ad_id', NEW.ad_id
			)::text
	);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER onair_update_trigger AFTER UPDATE ON onair FOR EACH ROW EXECUTE PROCEDURE onair_change();
CREATE TRIGGER onair_insert_trigger AFTER INSERT ON onair FOR EACH ROW EXECUTE PROCEDURE onair_change();
