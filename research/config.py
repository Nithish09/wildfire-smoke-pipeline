"""Shared settings for the research study (forecasting next-day county PM2.5 during wildfire seasons)."""
import datetime as dt
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.environ.get("RESEARCH_DATA", os.path.join(ROOT, "research_data"))   # raw downloads (git-ignored)
OUT = os.environ.get("RESEARCH_OUT", os.path.join(ROOT, "research_out"))      # features, results, figures

# Fire seasons studied: 1 Jul - 30 Nov. 2018 (Camp, Mendocino Complex), 2019, 2020 (lightning siege), 2021 (Dixie).
SEASONS = [2018, 2019, 2020, 2021]
SEASON_START = (7, 1)
SEASON_END = (11, 30)

# Fires are taken from a box wider than California so smoke from Oregon / Nevada / Baja fires is counted.
FIRE_BBOX = (-126.0, 31.0, -112.0, 44.5)        # west, south, east, north
LOCAL_TZ = "America/Los_Angeles"

PM25_PARAM = "88101"
EXCEEDANCE = 35.0                                # EPA 24-h PM2.5 standard, ug/m3
FIRE_RINGS_KM = [50, 100, 200, 400]
TRANSPORT_MAX_KM = 600.0
TRANSPORT_DECAY_KM = [100.0, 300.0]
NEIGHBOUR_RADIUS_KM = 300.0
N_WEATHER_STATIONS = 3                           # nearest stations averaged per county


def season_days(year):
    d = dt.date(year, *SEASON_START)
    end = dt.date(year, *SEASON_END)
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def path(*parts):
    p = os.path.join(*parts)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p
