"""
points.py
"""
import pandas as pd
import dask.dataframe

import src.hydrometry.patterns
import src.hydrometry.structure


class Points:
    """
    Class Points

    The reference data points.  The references are about measuring stations & measurement types.
    """
    
    def __init__(self, blob: pd.DataFrame):
        """
        
        """

        data = blob.copy()
        self.__blob = src.hydrometry.patterns.Patterns().exc(blob=data)

        # Additionally
        self.__blob.loc[:, 'station_name'] = self.__blob['station_name'].str.title()
        self.__blob.loc[:, 'river_name'] = self.__blob['river_name'].str.title()

    @staticmethod
    def __reshape_measures(blob: pd.DataFrame, fields: list) -> pd.DataFrame:
        """

        :param blob:
        :param fields:
        :return:
        """

        segments = ['is_groundwater', 'is_rainfall_station', 'is_sampling_location', 'is_integrity_station']

        # Foremost, melt by segment, subsequently retain records wherein <is_segment> is True.  Afterwards, reset.
        frame = dask.dataframe.from_pandas(data=blob.copy(), npartitions=12)
        melting = frame.melt(id_vars=list(set(fields) - set(segments)),
                             value_vars=segments, var_name='segment', value_name='is_segment')
        filtering = melting.loc[melting['is_segment'], melting.columns]
        resetting = filtering.reset_index(drop=False)

        # Compute
        sources = resetting.compute()
        sources.drop(columns='is_segment', inplace=True)

        return sources

    def measures(self) -> (pd.DataFrame, pd.DataFrame):
        """

        :return:
        """

        fields = ['station_id', 'station_guid', 'easting', 'northing', 'latitude', 'longitude', 'catchment_area',
                  'is_station', 'is_groundwater', 'is_rainfall_station', 'is_sampling_location', 'is_integrity_station',
                  'date_opened', 'date_closed', 'measure', 'basename', 'variable']

        # Excerpt
        instances = src.hydrometry.structure.Structure().exc(blob=self.__blob.copy())
        instances = instances.copy()[fields]

        # Alternative
        sources = self.__reshape_measures(blob=instances, fields=fields)

        return instances, sources

    def gazetteer(self) -> pd.DataFrame:
        """

        :return:
        """

        fields = ['station_id', 'station_guid', 'easting', 'northing', 'longitude', 'latitude', 'catchment_area',
                  'station_name', 'river_name',  'date_opened', 'date_closed',
                  'wiski_id', 'river_level_tool_id', 'rfa_station_id',
                  'station_status', 'station_status_reason']

        # Excerpt
        data = self.__blob.copy()[fields]

        return data
