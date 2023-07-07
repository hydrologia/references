"""
setting.py
"""
import src.functions.directories
import src.hydrometry.configurations
import src.hydrometry.datatype


class Setting:
    """
    Class Setting
    """

    ReferenceType = src.hydrometry.datatype.DataType().ReferenceType

    def __init__(self, external: bool):
        """

        :param external: Should the raw references data be downloaded from an external source, or
                         is it available locally.
        """

        # The class of arguments for stations references
        self.__setting: src.hydrometry.datatype.DataType().ReferenceType = \
            src.hydrometry.configurations.Configurations().exc(code='stations', external=external)

        # Preparing directories
        self.__directories = src.functions.directories.Directories()
        self.__set_directories()

    def __set_directories(self):
        """

        :return:
        """

        if self.__setting.external:
            self.__directories.cleanup(path=self.__setting.raw)
            self.__directories.create(path=self.__setting.raw)

        self.__directories.cleanup(path=self.__setting.storage)
        self.__directories.create(path=self.__setting.storage)

    def exc(self) -> ReferenceType:
        """

        :return:
        """

        return self.__setting
