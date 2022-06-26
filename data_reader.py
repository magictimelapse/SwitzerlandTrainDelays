import pandas as pd
import configparser
import os
import requests
from clint.textui import progress
import datetime
import zipfile
import data_cleaner as dc

config = configparser.ConfigParser()
config.read('config.ini')

def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def read_csv(date):
    """Reads the istdaten csv file from the configured data directory for a given date

    Parameters
    ----------
    date : datetime.datetime
        The date one wants to read the istdaten csv file


    Returns
    -------
    pd.DataFrame
        The dateframe corresponding to the istdaten csv file
    """

    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten.csv")
    )
    create_directory_if_not_exists(config['data']['directory'])
    df = pd.read_csv(filename, sep=';', dtype=str)
    return df

def save_dataframe_to_parquet( df, date):
    """Save a istdaten dateframe as a parquet file in the configured data directory

    Parameters
    ----------
    df : pd.DataFrame
        The istdaten DataFrame one wants to save
    date : datetime.datetime
        The date one wants to save the istdaten parquet file


    Returns
    -------
    None
    """

    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten.parquet")
    )
    create_directory_if_not_exists(config['data']['directory'])
    df.to_parquet(filename)

def read_dataframe_from_parquet(date):
    """Reads a istdaten DataFrame from a parquet file

    Parameters
    ----------
    date : datetime.datetime
        The date of the parquet file


    Returns
    -------
    pd.DataFrame
        The dateframe corresponding to the istdaten parquet file
    """

    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten.parquet")
        )
    create_directory_if_not_exists(config['data']['directory'])
    df = pd.read_parquet(filename)
    return df

def download_archive(date):
    """Downloading and unzipping a archive file containing the csv files for one month, and stored in the configured
    data directory

    Parameters
    ----------
    date : datetime.datetime
        The date. The archive is downloaded for the date's month and year


    Returns
    -------
    None
    """

    if date > datetime.datetime(2021, 6, 1):
        archive_url = date.strftime(
            'https://opentransportdata.swiss/wp-content/uploads/ist-daten-archive/ist-daten-%Y-%m.zip')
    else:
        archive_url = date.strftime(
            'https://opentransportdata.swiss/wp-content/uploads/ist-daten-archive/%y_%m.zip')
    # download file with progress bar
    r = requests.get(archive_url, stream=True)
    path = os.path.join(
        config['data']['directory'],
        date.strftime('ist-daten-%Y-%m.zip')
    )
    create_directory_if_not_exists(config['data']['directory'])
    with open(path, 'wb') as f:
        total_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length / 1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()

    # unzip file
    with zipfile.ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(config['data']['directory'])

#read the dateframe from local parquet file, in case it exists, otherwise the csv, otherwise download the archive
# and extract the csv, and write the parquet file
def read_data(date):
    """Returns the istdaten DataFrame for a given date. In case the parquet file exists, it will read this file,
    otherwise, it will read the csv file. In case neither exists, it will download the archive file for a month,
    unzip it, read the corresponding csv file and store it as a parquet file.

    Parameters
    ----------
    date : datetime.datetime
        The date where one wishes to read the istdaten DataFrame


    Returns
    -------
     pd.DataFrame
        The dateframe corresponding to the istdaten file
    """

    # first check if the parquet file is available #
    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten.parquet")
    )
    create_directory_if_not_exists(config['data']['directory'])
    if os.path.exists(filename):
        return read_dataframe_from_parquet(date)

    # check if csv file is availabe
    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten.csv")
    )
    if os.path.exists(filename):
        df = read_csv(date)
        # and store the dataframe as parquet for later reuse
        save_dataframe_to_parquet(df, date)
        return df

    # dataset does not exist locally. We need to download the archive
    download_archive(date)
    # read the csv
    df = read_csv(date)
    # and store the dataframe as parquet for later reuse
    save_dataframe_to_parquet(df, date)
    return df



def read_cleaned_data(date):
    """Reads the parquet file for a cleaned istdaten df. In case it does not exist, it reads the uncleaned file,
    cleans it and stores it as a cleand istdaten parquet file.

    Parameters
    ----------
    date : datetime.datetime
        The date where one wishes to read the cleaned istdaten DataFrame


    Returns
    -------
     pd.DataFrame
        The dateframe corresponding to the istdaten cleaned data
    """

    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten_cleaned.parquet")
    )
    create_directory_if_not_exists(config['data']['directory'])
    if os.path.exists(filename):
        return pd.read_parquet(filename)

    df = read_data(date)
    df = dc.clean_data_abfahrt(df)
    df.to_parquet(filename)
    return df

def read_prepared_data(date):
    """Reads the parquet file for a prepared istdaten df. In case it does not exist, it reads the uncleaned file,
    cleans it and stores it as a prepared istdaten parquet file.

    Parameters
    ----------
    date : datetime.datetime
        The date where one wishes to read the prepared istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The dateframe corresponding to the istdaten cleaned data
    """

    filename = os.path.join(
        config['data']['directory'],
        date.strftime("%Y-%m-%d_istdaten_prepared.parquet")
    )
    create_directory_if_not_exists(config['data']['directory'])
    if os.path.exists(filename):
        return pd.read_parquet(filename)
    ## prepared data is cleaned data that is prepared ##


    df = read_cleaned_data(date)
    df = dc.prepare_data(df)
    df.to_parquet(filename)
    return df



def read_cleaned_data_from_daterange(date_start, date_end):
    """Reads the parquet files for a cleaned istdaten df for a given daterange

    Parameters
    ----------
    date_start : datetime.datetime
        The date where one wishes to start reading the cleaned istdaten DataFrame
    date_end : datetime.datetime
        The date where one wishes to stop reading the cleaned istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The dateframe corresponding to the istdaten cleaned data from within the daterange.
    """

    delta = date_end - date_start
    dates = [date_start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
    dfs = []

    for date in dates:
        df = read_cleaned_data(date)
        dfs.append(df)
    return pd.concat(dfs)

def read_prepared_data_from_daterange(date_start, date_end):
    """Reads the parquet files for a prepared istdaten df for a given daterange

    Parameters
    ----------
    date_start : datetime.datetime
        The date where one wishes to start reading the prepared istdaten DataFrame
    date_end : datetime.datetime
        The date where one wishes to stop reading the prepared istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The dateframe corresponding to the istdaten prepared data from within the daterange.
    """

    delta = date_end - date_start
    dates = [date_start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
    dfs = []
    for date in dates:
        df = read_prepared_data(date)
        dfs.append(df)
    return pd.concat(dfs)
