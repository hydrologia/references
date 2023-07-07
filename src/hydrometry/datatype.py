import collections


class DataType:

    # Hydrometry reference parameters: vis-à-vis the environment agency's  hydrometry reference
    # data - retrieved via an API (Application Programming Interface)
    ReferenceNode = collections.namedtuple(
        typename='ReferenceNode', field_names=['code', 'url', 'basename'])

    # Local settings
    ReferenceDirectories = collections.namedtuple(
        typename='ReferenceDirectories', field_names=['raw', 'storage'])

    # Retrieve the data online or locally
    ReferenceExternal = collections.namedtuple(
        typename='ReferenceExternal', field_names=['external'])

    # Altogether
    ReferenceType = collections.namedtuple(
        typename='ReferenceType',
        field_names=list(ReferenceNode._fields) + list(ReferenceDirectories._fields) + list(ReferenceExternal._fields))

    def __init__(self):
        """

        """
