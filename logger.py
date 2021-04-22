import logging

from configuration import CONFIG

logging.basicConfig(
    filename=CONFIG.get('DEFAULT', 'LOG_OUTPUT_DIR'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s \n'
)

logger = logging.getLogger(__name__)