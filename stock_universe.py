from pymongo import MongoClient, ASCENDING
from finsymbols import symbols
from pprint import pprint
import pandas as pd
import datapackage
import datetime
import yaml
import os

__DS_default_loc = './DS'
__ta_default_loc = './models'
__univ_default_loc = './universe'
__dailies_default_loc = './dailies'

def verify_dir(config, key, default_value):
    if key in config:
            __loc = config[key]
    else:
            __loc = default_value

    _absLoc = os.path.join(os.getcwd(), __loc)

    try:
        if not os.path.isdir(_absLoc):
                print(f"Creating Main DataSource Directory {_absLoc}")
                os.mkdir(_absLoc)
    except:
        print(f"Error while creating Main DataSource Folder {_absLoc}")

    return _absLoc

def stripTrails(x):
    return x.strip('\n')

def getSettings(configFile='./settings/settings.yml'):
    """ Get settings file"""

    if not os.path.isfile(configFile):
        print(f"Error - {configFile} cannot be found!")
        return None

    try:
        with open(configFile, 'r') as stream:
             config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print("Error: Cannot read config file in settings/settings.yml")
        return None

    __DS_loc = verify_dir(config, 'ds_location', __DS_default_loc)
    __ta_loc = verify_dir(config, 'ta_location', __ta_default_loc)
    __univ_loc = verify_dir(config, 'universe_location', __univ_default_loc)
    __dailies_loc = verify_dir(config, 'dailies_location', __dailies_default_loc)

    config['ds_abs_location'] = __DS_loc
    config['ta_abs_location'] = __ta_loc
    config['univ_abs_location'] = __univ_loc
    config['dailies_abs_location'] = __dailies_loc

    for entry in ['mongo_server', 'mongo_port', 'mongo_username', 'mongo_password', 'mongo_db_universe']:
        if entry not in config:
            print(f"Error: Cannot find entry: {entry} in Settings file. Aborting.")
            return None

    return config

def getUniverseDB():
    settings = getSettings()
    if settings:
        client = MongoClient( username=settings['mongo_username'], password=settings['mongo_password'], \
                              host=settings['mongo_server'], port=settings['mongo_port'], \
                              authSource=settings['mongo_db_universe'])
        return client[settings['mongo_db_universe']]
    else:
        print("Error in retrieving Settings!")
        return None

def getCollections(db):
    return db.list_collection_names()

def getDefaultDB():
    return getUniverseDB()

def refresh_sp500():
    db = getDefaultDB()
    if not db:
        print("Error - Cannot get default db from MongoDB Server")
        return None

    existingCollections = getCollections(db)

    try:
        sp500 = symbols.get_sp500_symbols()
    except Exception as e:
        print("Error while retrieving new SP500 Symbols - Aborting")
        return None

    df500 = pd.DataFrame(sp500)
    df500['symbol'] = df500['symbol'].apply(stripTrails)
    df500['sector'] = df500['sector'].apply(stripTrails)

    if 'sp500' in existingCollections:
        print("SP500 was found in exisitng MongoDB - deleting it.")
        db.sp500.drop()

    sp500Collection = db.sp500

    df500.reset_index(inplace=True)
    data_dict = df500.to_dict("records")

    print("Inserting new SP500 List.")
    sp500Collection.insert_many(data_dict)

    return df500

def refresh_nyse():
    db = getDefaultDB()
    if not db:
        print("Error - Cannot get default db from MongoDB Server")
        return None

    existingCollections = getCollections(db)

    try:
        data_url = 'https://datahub.io/core/nyse-other-listings/datapackage.json'
        # to load Data Package into storage
        package = datapackage.Package(data_url)
        # to load only tabular data
        resources = package.resources
        i=0
        pds = []
        for resource in resources:
            if resource.tabular:
                pds.append(pd.read_csv(resource.descriptor['path']))
                i+=1
        # pd2 has full list of nyse stocks
        pd2 = pds[1]
    except:
        print("Error while retrieving new NYSE Symbols - Aborting")
        return None

    if 'nyse' in existingCollections:
        print("NYSE was found in exisitng MongoDB - deleting it.")
        db.nyse.drop()

    nyseCollection = db.nyse
    data_dict = pd2.to_dict("records")
    print("Inserting new NYSE List.")
    nyseCollection.insert_many(data_dict)

    return pd2

def refresh_all():
    refresh_sp500()
    refresh_nyse()


def getSP500_df(force_refresh = False):

    db = getDefaultDB()
    if not db:
        print("Error - Cannot get default db from MongoDB Server")
        return None

    if 'sp500' in getCollections(db):
        #print('SP500 Collection is found in MongoDB')
        sp500Collection = db.sp500
        sp500_stocks = [x for x in sp500Collection.find()]
        mongo_sp500 = pd.DataFrame(sp500_stocks)

        return mongo_sp500
    else:
        print('Error sp500 Collection cannot be found in MongoDB')
        return None

def getNYSE_df(force_refresh = False):


    db = getDefaultDB()
    if not db:
        print("Error - Cannot get default db from MongoDB Server")
        return None
    if 'nyse' in getCollections(db):
        #print('NYSE Collection is found in MongoDB')
        nyseCollection = db.nyse
        nyse_stocks = [x for x in nyseCollection.find()]
        mongo_nyse = pd.DataFrame(nyse_stocks)

        return mongo_nyse
    else:
        print('Error NYSE(nyse) Collection cannot be found in MongoDB')
        return None

def get_su_etfs_list():
    nyse = getNYSE_df()
    etfs = nyse[nyse['ETF']=='Y']
    return list(etfs['NASDAQ Symbol'])

def get_key_etfs_list():
    return [ 'FBND',
             'FCOM',
             'FCOR',
             'FDIS',
             'FENY',
             'FHLC',
             'FIDU',
             'FLTB',
             'FMAT',
             'FNCL',
             'FSTA',
             'FTEC',
             'FUTY',
             'FZROX',
             'GLD',
             'IWM',
             'QQQ',
             'SCHD',
             'SPY',
             'VEA',
             'VOO']

def get_su_stocks_list():
    ## get list of NYSE - Non-ETFs
    nyse =  getNYSE_df()
    nyse_stocks_df = nyse[nyse['ETF']=='N']
    _filter1 = ~ nyse_stocks_df['NASDAQ Symbol'].str.contains('-')
    nyse_stocks = nyse_stocks_df[_filter1]
    nyse_stocks_list = list(nyse_stocks['NASDAQ Symbol'])

    ## get list of SP500
    sp500 = getSP500_df()
    sp500_stocks = list(sp500['symbol'])

    su_set = set(sp500_stocks + nyse_stocks_list)

    return list(su_set)

def su_describe(_stock):
    ## Returns Object description of a stock
    nyse =  getNYSE_df()
    sp500 = getSP500_df()

    if type(_stock) == type([]):
        match_stock_list = []

        for _stockItem in _stock:
            stock = _stockItem.upper()

            nyse_match = nyse[nyse['NASDAQ Symbol']==stock]
            nyse_count = len(nyse_match)

            sp500_match = sp500[sp500['symbol']==stock]
            sp500_count = len(sp500_match)

            if nyse_count == 0 and sp500_count ==0:
                break

            match_stock = {}

            if nyse_count == 1:
                nyse_match_filtered = nyse_match.drop('_id', axis=1)
                match_stock = nyse_match_filtered.to_dict('records')[0]

            if sp500_count == 1:
                sp500_match_filtered = sp500_match.drop('_id', axis=1)
                if match_stock == {}:
                    match_stock = sp500_match_filtered.to_dict('records')[0]
                else:
                    for elm in ['headquarters', 'industry', 'sector']:
                        match_stock[elm] = sp500_match_filtered[elm][0]

            match_stock_list.append(match_stock)

        return match_stock_list

    elif type(_stock) == type(""):
        stock = _stock.upper()
        nyse_match = nyse[nyse['NASDAQ Symbol']==stock]
        nyse_count = len(nyse_match)

        sp500_match = sp500[sp500['symbol']==stock]
        sp500_count = len(sp500_match)

        if nyse_count == 0 and sp500_count ==0:
            return {}

        match_stock = {}

        if nyse_count == 1:
            nyse_match_filtered = nyse_match.drop('_id', axis=1)
            match_stock = nyse_match_filtered.to_dict('records')[0]

        if sp500_count == 1:
            sp500_match_filtered = sp500_match.drop('_id', axis=1)
            if match_stock == {}:
                match_stock = sp500_match_filtered.to_dict('records')[0]
            else:
                for elm in ['headquarters', 'industry', 'sector']:
                    match_stock[elm] = sp500_match_filtered[elm][0]

        return match_stock

def get_su_latest(stockList, write = False, days=33, prefix='universe'):
    """ By Default:
     write: (False) => write to dailies <prefix>_adj_close.pkl , <prefix>_volume.pkl, <prefix>_open.pkl,
                                        <prefix>_high.pkl, <prefix>_low.pkl, <prefix>_close.pkl
     days: by default days=42 - to get (at least the last 30 trading days)
     prefix (universe) by default - prefix to rename the pkl output in dailies. """

    date_N_days_ago = datetime.now() - timedelta(days=days)

    start = str(date_N_days_ago.date())
    end = str(datetime.now().date())

    data = yf.download(stockList, start = start, end = end)

    filtered_data = data.dropna(axis=1)

    if not write:
        return filtered_data

    settings = getSettings()
    dailies_abs = settings['dailies_abs_location']

    for entry in list(filtered_data.columns.levels[0]):
        newEntry = "_".join(entry.lower().split())
        filename = "_".join([prefix, newEntry])+".pkl"
        targe_file = os.path.join(dailies_abs, filename)
        print(f"writing {targe_file}")

        sel_pd = filtered_data[entry]
        sel_pd.to_pickle(targe_file)

    return filtered_data
