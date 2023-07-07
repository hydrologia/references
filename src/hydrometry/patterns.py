"""
patterns.py
"""
import pandas as pd

import src.hydrometry.fields


class Patterns:
    """
    Class Patterns

    Ensures correct casting and/or formatting per applicable field
    """

    def __init__(self):
        """

        """
        # Data Types
        (_, self.__datatypes) = src.hydrometry.fields.Fields().stations()

    def __casting(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        data = blob.copy()
        for k, v in self.__datatypes.items():
            data[k] = data[k].astype(v)

        return data

    @staticmethod
    def __formatting(blob: pd.DataFrame) -> pd.DataFrame:

        data = blob.copy()

        data['date_opened'] = pd.to_datetime(data['date_opened'].copy(), format='%Y-%m-%d')
        data['date_closed'] = pd.to_datetime(data['date_closed'].copy(), format='%Y-%m-%d')

        return data

    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :return:
        """

        data = blob.copy()

        # Casting
        data = self.__casting(blob=data)

        # Date formatting
        data = self.__formatting(blob=data)

        # Outlying problem
        data['river_level_tool_id'] = pd.to_numeric(
            data['river_level_tool_id'].copy(), errors='coerce', downcast='unsigned')

        return data
