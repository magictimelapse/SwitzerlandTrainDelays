import pandas as pd
from datetime import timedelta



def remove_unnecessary_columns(df):
    """Removes columns from the istdaten df that are not necessary for the further processing

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame without the unnecessary columns
    """

    df.drop(columns= [ 'BETREIBER_ID', 'LINIEN_ID', 'PRODUKT_ID', 'UMLAUF_ID',
                     'VERKEHRSMITTEL_TEXT', 'FAELLT_AUS_TF'], inplace=True)
    return df

# we are only interested in sbb
def select_sbb(df):
    """Only select entries from the istdaten df where the BETREIBER_ABK is SBB

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame for SBB
    """

    df = df[df["BETREIBER_ABK"] == "SBB"]
    return df

def select_trains(df):
    """Only select entries from the istdaten df where a train is involved

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame for trains
    """

    df = df[df['PRODUKT_ID'] == "Zug"]
    return df

# filter data where the an/ab prognose is "REAL"
def real_prognose_filter(df):
    """Only select entries from the istdaten df where the AN_PROGNOSE_STATUS and the AB_PROGNOSE_STATUS is REAL.

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame REAL delays
    """

    df = df[(df['AN_PROGNOSE_STATUS'] == 'REAL') & (df['AB_PROGNOSE_STATUS'] == 'REAL')]
    return df


def convert_to_datetimes(df):
    """Convert string datetimes in the istdaten df to datetimes

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame with datetime.datetime format instead of string.
    """

    #switch off annoying warnings
    pd.options.mode.chained_assignment = None
    df['AN_PROGNOSE'] = df['AN_PROGNOSE'].apply(pd.to_datetime, dayfirst=True)
    df['ANKUNFTSZEIT'] = df['ANKUNFTSZEIT'].apply(pd.to_datetime, dayfirst=True)
    df['AB_PROGNOSE'] = df['AB_PROGNOSE'].apply(pd.to_datetime, dayfirst=True)
    df['ABFAHRTSZEIT'] = df['ABFAHRTSZEIT'].apply(pd.to_datetime, dayfirst=True)
    df['BETRIEBSTAG'] = df['BETRIEBSTAG'].apply(pd.to_datetime, dayfirst=True)
    pd.options.mode.chained_assignment = 'warn'

    return df

# filter bad data. i.e. nans
def bad_data_filter(df):
    """Filter rows where AN_PROGNOSE, AB_PROGNOSE, ABFAHRTSZEIT or ANKUNFTSZEIT is not nan.

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame without NaNs.
    """

    df = df[(df['AN_PROGNOSE'].notna()) & (df['AB_PROGNOSE'].notna()) & (df['ABFAHRTSZEIT'].notna()) & (
        df['ANKUNFTSZEIT'].notna())]
    return df

## calculate the delays. We calculate those in seconds. Reason: timedeltas suck.
def calculate_delay(df):
    """Add a column to the istdaten df corresponding to the arrival and departure delays, measured in seconds.

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame with the new delay columns.
    """

    df['ABFAHRTSVERSPAETUNG_s'] = (df['AB_PROGNOSE'] - df['ABFAHRTSZEIT']) / pd.Timedelta(seconds=1)
    df['ANKUNFTSVERSPAETUNG_s'] = (df['AN_PROGNOSE'] - df['ANKUNFTSZEIT']) / pd.Timedelta(seconds=1)
    return df

# no train in switzerland has a delay longer than 1 hour
def remove_crazy_delays(df):
    """Remove rows with a delay larger than 1 hour. No train in Switzerland has such a large delay!

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The istdaten DataFrame without crazy delays.
    """

    df = df[(df['ANKUNFTSVERSPAETUNG_s'] < timedelta(hours=1) / timedelta(seconds=1)) & (
                df['ABFAHRTSVERSPAETUNG_s'] < timedelta(hours=1) / timedelta(seconds=1))]
    df = df[(df['ANKUNFTSVERSPAETUNG_s'] > timedelta(hours=-1) / timedelta(seconds=1)) & (
                df['ABFAHRTSVERSPAETUNG_s'] > timedelta(hours=-1) / timedelta(seconds=1))]
    return df


def clean_data(df):
    """Perform several cleaning steps, see below

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The cleaned istdaten DataFrame
    """

    df = select_trains(df)
    df = remove_unnecessary_columns(df)
    df = convert_to_datetimes(df)
    df = real_prognose_filter(df)
    df = bad_data_filter(df)
    df = calculate_delay(df)

    return df

def prepare_data(df):
    """Clean the data and select only SBB

    Parameters
    ----------
    df : pd.DataFrame
        the istdaten DataFrame


    Returns
    -------
    pd.DataFrame
        The cleaned istdaten DataFrame for SBB
    """

    df = select_sbb(df)
    df = clean_data(df)
    #df = remove_crazy_delays(df)
    return df