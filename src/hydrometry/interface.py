"""
interface.py
"""
import collections
import os

import pandas as pd

import src.hydrometry.datatype
import src.functions.directories
import src.functions.objects
import src.functions.streams
import src.hydrometry.anomalies
import src.hydrometry.fields
import src.hydrometry.points


class Interface:
    """
    Class Stations: The hydrometry stations
    """

    ReferenceType = src.hydrometry.datatype.DataType().ReferenceType

    def __init__(self, setting: ReferenceType):
        """

        :param setting:
        """

        # Parameters
        self.__setting = setting
        self.__external = self.__setting.external
        self.__uri = os.path.join(self.__setting.raw, self.__setting.basename)
        self.__storage = self.__setting.storage

        # Data
        self.__objects = src.functions.objects.Objects()
        self.__streams = src.functions.streams.Streams()

    def __read(self) -> dict:
        """

        :return:
        """

        if self.__external:
            return self.__objects.read_external(url=self.__setting.url)
        else:
            return self.__objects.read_internal(uri=self.__uri)

    @staticmethod
    def __normalise(nodes: dict) -> pd.DataFrame:
        """
        The normalisation of the JSON string

        :param nodes:
        :return:
        """

        data = pd.json_normalize(data=nodes, record_path='items')

        return data

    @staticmethod
    def __rename(blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        data = blob.copy()

        # The fields of interest,
        fields, _ = src.hydrometry.fields.Fields().stations()
        data = data.copy()[fields.keys()]

        # ... and their preferred names
        data.rename(columns=fields, inplace=True)

        return data

    def exc(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """

        :return:
        """

        # The data
        nodes = self.__read()
        if self.__external:
            self.__objects.write(nodes=nodes, path=self.__uri)

        # Normalise and address data anomalies
        data = self.__normalise(nodes=nodes)
        data = self.__rename(blob=data)
        excerpt = src.hydrometry.anomalies.Anomalies().exc(blob=data)

        # Gazetteer & Inventory of Measures
        points = src.hydrometry.points.Points(blob=excerpt)
        gazetteer = points.gazetteer()
        instances, sources = points.measures()

        self.__streams.write(blob=gazetteer, path=os.path.join(self.__storage, 'gazetteer.csv'))
        self.__streams.write(blob=sources, path=os.path.join(self.__storage, 'sources.csv'))
        self.__streams.write(blob=instances, path=os.path.join(self.__storage, 'instances.csv'))

        return gazetteer, instances, sources
