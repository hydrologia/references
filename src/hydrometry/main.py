"""
main.py
"""
import logging
import os
import sys

import pandas as pd


def main():
    """
    The entry point.

    :return:
    """

    # This class prepares the data directories, data classes, etc., and
    # if external = True, the latest raw hydrometry references data will be downloaded
    setting = src.hydrometry.setting.Setting(external=True).exc()

    # The Hydrometry References
    gazetteer: pd.DataFrame
    instances: pd.DataFrame
    sources: pd.DataFrame
    gazetteer, instances, sources = src.hydrometry.interface.Interface(setting=setting).exc()

    # Items
    gazetteer.info()
    instances.info()
    sources.info()


if __name__ == '__main__':
    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    # Threads
    os.environ['NUMEXPR_MAX_THREADS'] = '8'

    # Logging
    logging.basicConfig(level=logging.INFO, format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    # Classes
    import src.hydrometry.setting
    import src.hydrometry.interface

    main()
