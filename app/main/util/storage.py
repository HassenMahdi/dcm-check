import os
from pathlib import Path

import pandas as pd
from flask import current_app as app



IMPORT_FOLDER = "imports/"
MAPPING_FOLDER = "mappings/"
RESULTS_FOLDER = "results/"


def get_upload_file_path(filename, folder=""):
    """Creates a string path for a file under UPLOAD FOLDER"""

    return os.path.join(app.config['UPLOAD_FOLDER'], folder, filename.rsplit(".")[0])


def get_path(folder, filename, worksheet, as_folder, create, extension="csv"):
    """Constructs the path for a file under UPLOAD FOLDER"""

    folder_path = Path(get_upload_file_path(filename, folder=folder))
    if create:
        folder_path.mkdir(parents=True, exist_ok=True)
    if as_folder:
        return str(folder_path)
    else:
        return str(folder_path.joinpath(f"{worksheet}.{extension}"))


def get_import_path(filename, worksheet, as_folder=False, create=True):
    return get_path(IMPORT_FOLDER, filename, worksheet, as_folder, create)


def get_mapping_path(filename, worksheet, as_folder=False, create=True):
    return get_path(MAPPING_FOLDER, filename, worksheet, as_folder, create)


def get_results_path(filename, worksheet, as_folder=False, create=True):
    return get_path(RESULTS_FOLDER, filename, worksheet, as_folder, create, extension="csv")


def get_dataframe_from_csv(path, nrows=None, skiprows=None, usecols=None,delimiter=";"):
    """Creates a dataframe from a csv file"""

    df = pd.read_csv(path, engine="c", dtype=str, skipinitialspace=True, skiprows=skiprows, nrows=nrows,
                     usecols=usecols, na_filter=False,delimiter=delimiter)
    return df


def get_check_results_df(filename, worksheet):
    """Creates a dataframe from check result csv file"""

    path = get_results_path(filename, worksheet, as_folder=False, create=True)
    return get_dataframe_from_csv(path,delimiter=";")


def get_mapped_df(filename, worksheet, nrows=None, skiprows=None, usecols=None):
    """Creates a dataframe from mapped data csv file"""

    path = get_mapping_path(filename, worksheet, as_folder=False, create=True)
    return get_dataframe_from_csv(path, nrows, skiprows, usecols)


def get_imported_data_df(filename, worksheet, nrows=None, skiprows=None):
    """Creates a dataframe from imported worksheet csv file"""

    path = get_import_path(filename, worksheet, as_folder=False, create=True)
    return get_dataframe_from_csv(path, nrows, skiprows)


def save_check_results_df(df, filename, worksheet):
    """Saves the check result dataframe into a csv file"""

    path = get_results_path(filename, worksheet, as_folder=False, create=True)
    df.to_csv(path,index=False,sep=';')


def save_mapped_df(df, filename, worksheet):
    """Saves the mapped dataframe into a csv file"""

    path = get_mapping_path(filename, worksheet, as_folder=False, create=True)
    df.to_csv(path, index=False,sep=';')

