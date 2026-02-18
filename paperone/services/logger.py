import logging
import os

log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'youtrack_worker.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
