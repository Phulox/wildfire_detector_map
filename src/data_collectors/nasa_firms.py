import requests
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
from io import StringIO
from config.settings import NASA_FIRMS_API_KEY


base_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_FIRMS_API_KEY}/MODIS_NRT/USA/1"

def fetch_modis_global_fires(url):

    response = requests.get(url)
    response.raise_for_status()
    df = pd.read_csv(StringIO(response.text))
    return df
       