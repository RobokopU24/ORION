import os
from pathlib import Path
from dotenv import dotenv_values

CONFIG = {
    **dotenv_values(Path(__file__).parents[1] / '.env'),  # load config variables from .env
    **os.environ,  # override loaded values with environment variables
}


