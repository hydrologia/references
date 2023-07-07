"""
configurations.py
"""
import os

import yaml

import src.hydrometry.datatype


class Configurations:
    """
    Class Configurations
    """

    ReferenceType = src.hydrometry.datatype.DataType().ReferenceType
    ReferenceDirectories = src.hydrometry.datatype.DataType().ReferenceDirectories
    ReferenceNode = src.hydrometry.datatype.DataType().ReferenceNode

    def __init__(self):
        """
        Constructor
        """

        # The expected location of the YAML
        self.__path = os.path.join(os.getcwd(), 'src', 'hydrometry', 'configurations.yaml')

        # Get the stream of YAML objects as soon as this class is instantiated
        # A stream of objects wherein each object has the API details for a data set
        self.__stream = self.__get_stream()

    def __get_stream(self) -> dict:
        """
        Reads the YAML file of objects. Each object is a set of application
         programming interface data parameters.

        :return:
        """

        with open(file=self.__path, mode='r') as stream:
            try:
                return yaml.load(stream=stream, Loader=yaml.CLoader)
            except yaml.YAMLError as err:
                raise Exception(err)

    def __directories(self) -> ReferenceDirectories:
        """

        :return:
        """

        # Local directories
        dictionary: dict = {item['code']: os.path.join(os.getcwd(), *item['parts'])
                            for item in self.__stream['directories']}
        
        return self.ReferenceDirectories(**dictionary)

    def __node(self, code: str) -> ReferenceNode:
        """

        :param code: The code of the API object of interest.
        :return:
        """

        # Extracts the details of an API (Application Programming Interface) object in focus.
        dictionary: dict = [item for item in self.__stream['nodes'] if item['code'] == code][0]
        
        return self.ReferenceNode(**dictionary)

    def exc(self, code: str, external: bool) -> ReferenceType:
        """

        :param code:
        :param external:
        :return:
        """

        node = self.__node(code=code)
        directories = self.__directories()
        items = {**node._asdict(), **directories._asdict(), **{'external': external}}

        # the named tuple form of the keys
        return self.ReferenceType(**items)
