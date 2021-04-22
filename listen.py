import sys
import time

from logger import logger

class Listen(object):
    def stop_all_campign():
        """ """
        pass

    def run_forever(self):
        """ """
        try:
            while True:
                pass
        except KeyboardInterrupt as k_ex:
            print('Caught Keyboard Interrupt...')
            logger.exception('Caught keyboard interrupt from postgresql listening')
            sys.exit(0)