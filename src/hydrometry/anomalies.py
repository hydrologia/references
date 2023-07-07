"""
anomalies.py
"""
import pandas as pd


class Anomalies:
    """
    Class Anomalies
    """

    def __init__(self):
        """

        """

    @staticmethod
    def __easting(blob: pd.DataFrame):
        """

        :param blob:
        :return:
        """

        data = blob.copy()

        # Unacceptable records vis-a-vis <easting> values, which must be numbers not lists
        condition = data['easting'].apply(lambda x: type(x) == list)
        data = data.copy().loc[~condition, :]
        data.reset_index(drop=True, inplace=True)

        return data

    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        data = blob.copy()
        data = self.__easting(blob=data)

        return data
