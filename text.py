import time
import concurrent.futures

INTERVAL = 60 ## A minute
SECONDS = 0

while True:
    time.sleep(INTERVAL)
    SECONDS += INTERVAL
    