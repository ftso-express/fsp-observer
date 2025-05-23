import asyncio
import os
import sys

import dotenv

from configuration.config import get_config
from configuration.types import Configuration
from observer.observer import observer_loop

from loguru import logger as LOGGER
LOGGER.remove(0)
LOGGER.add(sys.stderr, level=os.getenv('LOG_LEVEL', 'INFO'))
LOGGER.info("initialised")

def main(config: Configuration):
    LOGGER.info(f"Starting")
    asyncio.run(observer_loop(config))


if __name__ == "__main__":
    dotenv.load_dotenv()
    config = get_config()
    LOGGER.debug(f"{config = }")
    main(config)
