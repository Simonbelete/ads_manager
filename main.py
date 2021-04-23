import db

from onair import OnAir

if __name__ == '__main__':
    connection = db.connect()
    # onair table
    onair = OnAir(connection)
    # On first run start campings and create cronjob
    campaigns = onair.get_campains()
    
    for campaign in campaigns:
        # Start campign