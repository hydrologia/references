"""
fields.py
"""
import datetime

import numpy as np


class Fields:

    def __init__(self):
        """
        The constructor
        """

    @staticmethod
    def stations() -> (dict, dict):
        """

        :return:
        """

        keys = ['notation', 'label', 'type', 'easting', 'northing', 'lat', 'long', 'riverName', 'stationGuid',
                'wiskiID', 'RLOIid', 'catchmentArea', 'dateOpened', 'dateClosed', 'nrfaStationID', 'status.label',
                'statusReason', 'measures']

        values = ['station_id', 'station_name', 'type', 'easting', 'northing', 'latitude', 'longitude', 'river_name',
                  'station_guid', 'wiski_id', 'river_level_tool_id', 'catchment_area', 'date_opened', 'date_closed',
                  'rfa_station_id', 'station_status', 'station_status_reason', 'measures']

        types = [str, str, str, np.int64, np.int64, np.float64, np.float64, str,
                 str, str, str, np.float64, datetime.date, datetime.date,
                 np.int64, str, str, str]

        fields = dict(zip(keys, values))
        datatypes = {k: v for k, v in dict(zip(values, types)).items()
                     if k in ['easting', 'northing', 'latitude', 'longitude', 'catchment_area']}

        return fields, datatypes
