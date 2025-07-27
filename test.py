from tools.get_low_di20_by_day import get_low_di20_by_day
from nasdaq_100 import nasdaq_100
import pandas as pd
from pathlib import Path
from tools.refresh_stock_info import refresh_stock_info, refresh_low_di20_stock_list

LOW_DI20_DIR = Path(__file__).parent / "low_di20"

refresh_low_di20_stock_list()