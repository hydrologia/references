"""
structure.py
"""
import os

import dask.dataframe
import numpy as np
import pandas as pd


class Structure:
    """
    Class Structure: Structures the type & endpoints fields of stations.json; for
                     application programming interface interactions purposes.
    """

    def __init__(self):
        """

        """

    @staticmethod
    def __endpoints(endpoints: list, station_id: str) -> pd.DataFrame:
        """

        :param endpoints:
        :param station_id:
        :return:
        """

        nodes = [{'station_id': station_id, 'endpoint': measurement['@id']} for measurement in endpoints]

        return pd.DataFrame.from_records(nodes)

    def __decompose_endpoints(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        data = blob.copy()

        # Decompose the grouped endpoints per record
        excerpt = dask.dataframe.from_pandas(data=data[['endpoints', 'station_id']], npartitions=12)
        computation = excerpt.apply(
            lambda x: self.__endpoints(endpoints=x['endpoints'], station_id=x['station_id']), axis=1,
            meta={'name': str, 'station_id': str, 'endpoint': str})
        compute = computation.compute()
        endpoints = pd.concat(compute.values, ignore_index=True)

        # Merge
        data = data.copy().drop(columns='endpoints').merge(endpoints, how='left', on='station_id')

        return data

    @staticmethod
    def __types(nodes: list) -> list:
        """

        :param nodes:
        :return:
        """

        return [os.path.basename(list(node.values())[0]) for node in nodes]

    @staticmethod
    def __decompose_types(blob: pd.DataFrame):
        """

        :param blob:
        :return:
        """

        # Decomposing
        fields = {'Groundwater': 'is_groundwater', 'RainfallStation': 'is_rainfall_station',
                  'SamplingLocation': 'is_sampling_location', 'Station': 'is_station',
                  'WaterQualityStation': 'is_integrity_station'}

        data = blob.copy()
        for key, value in fields.items():
            data.loc[:, value] = data['type'].apply(lambda x: key in x)
        data.drop(columns='type', inplace=True)

        return data

    @staticmethod
    def __replace(basename: str, station_id: str) -> str:
        """

        :param basename:
        :param station_id:
        :return:
        """

        return basename.replace(station_id, '').lstrip('-')

    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        data = blob.copy()

        # The station's measuring tool types
        data.loc[:, 'type'] = data['type'].apply(lambda x: self.__types(x))
        data = self.__decompose_types(blob=data)

        # The endpoints
        data = self.__decompose_endpoints(blob=data)

        # Each distinct measurement, per distinct station, has its own distinct
        # API (application programming interface) endpoint
        basename = np.vectorize(pyfunc=os.path.basename)
        data.loc[:, 'basename'] = basename(data['endpoint'].to_numpy())
        variable = np.vectorize(pyfunc=self.__replace)
        data.loc[:, 'variable'] = variable(
            basename=data['basename'].to_numpy(), station_id=data['station_id'].to_numpy())

        return data
