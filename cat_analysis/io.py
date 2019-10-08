'''
io

Input output functions for CATS data

Data are stored within individual trips files
'''

import os
import pandas as pd 
import numpy as np 


def read_trip_file_names(filepath):
    """
    Read in the names of ALL .csv files within the subdirectories contained within the 
    filepath 
    Returns a `List` of filenames and paths
    

    Keyword arguments:
    filepath -- the filepath you want to search
    
    """
    files = []
    ext = '.csv'
    for dirpath, dirnames, filenames in os.walk(filepath):
        for filename in [f for f in filenames if f.endswith(ext)]:
            files.append(os.path.join(dirpath, filename))
            
    return files

class CleanTrip(object):
    '''
    Encapsulates the data cleaning logic for a
    CATS Trip.

    Assumptioms:

    1. The raw data are in not in a clean time series format
    2. The data have 33 usable columns
    3. The file contains a column 'timestamp'
    4. There data contain multiple entries per timestamp that must be combined.
    
    '''
    def __init__(self, filepath):
        self._filepath = filepath
        self._data = None
    
    def _get_data(self):
        '''
        Returns:
        --------
        pandas.DataFrame containing the cleaned
        time series data
        '''
        return self._data

    def resample(self, rule, interp=False):
        '''
        Convenience function to convert the frequency 
        of observation in the timeseries

        e.g. 1s -> 30s

        Parameters:
        --------
        rule - str, see pd.DataFrame.resample()
        interp - bool.  True = linear interpolation (default=False)

        Returns:
        ---------
        DataFrame - in the new obervation period
        '''
        data = self._data.resample(rule=rule).mean()

        #dosen't work with spline or polynomial 
        if interp:
            data = data.interpolate(method='linear')

        return data

    def clean(self):
        '''
        Process the raw data into cleaner time series.
        '''
        df = self._read_trip()
        self._format_headers(df)
        df = self._drop_invalid_dates(df)
        self._format_as_timeseries(df)
        self._format_wave_form_data(df)
        df = self._replace_missing_values_with_nan(df)
        self._data = self._aggregate_rows(df)


    def _read_trip(self):
        return pd.read_csv(self._filepath, usecols=[i for i in range(33)])
        

    def _format_headers(self, df):
        '''
        strip out whitesapce from dataframe column headers

        Parameters:
        --------
        df -- pandas.DataFrame, containing raw time series
        '''
        #reformat column header names
        df.columns = (df.columns.str.strip()
                     .str.lower()
                     .str.replace(' ', '_')
                     .str.replace('(', '')
                     .str.replace(')', '')
                     )


    def _drop_invalid_dates(self, df):
        '''
        Removes any invalid rows from the dataset
        These are flagged as 'Invalid Date' in the
        timestamp column.

        Returns:
        --------
        Dataframe -- containing data minus invalid dates

        Parameters:
        --------
        df -- pandas.DataFrame, containing raw time series
        '''
        return df[~df['timestamp'].str.contains("Invalid Date")]

    def _format_as_timeseries(self, df):
        '''
        Convert timestamp column to pandas timeseries data

        Parameters:
        --------
        df -- pandas.DataFrame, containing raw time series
        '''
        df['timestamp'] = pd.to_datetime(df['timestamp'], )

    def _replace_missing_values_with_nan(self, df):
        missing_value = 8388607.0
        return df.replace(missing_value, None)
    

    def _format_wave_form_data(self, df):
        '''
        Columns 24+ contain multiple observations per second.
        This is stored in string format.  This procedure
        converts to numpy array and then produces new features based
        on moments of the distribution and other measures.

        Parameters:
        ---------
        df - pandas - DataFrame containing the waveform data
        '''
        cols_to_drop = []
        from_column = 24

        for col in df.columns[from_column:]:
            
            np_label = col + '_np'
            
            df[np_label] = df[col].apply(str).apply(lambda x: np.fromstring(x.replace(' \"',' ')
                                                            .replace('nan','')
                                                            ,sep=' '))
            
            df[col+'_avg'] = df[np_label].map(lambda x: x.mean() if x.shape[0] > 0 else None)
            df[col+'_std'] = df[np_label].map(lambda x: x.std() if x.shape[0] > 0 else None)
            df[col+'_min'] = df[np_label].map(lambda x: x.min() if x.shape[0] > 0 else None)
            df[col+'_max'] = df[np_label].map(lambda x: x.max() if x.shape[0] > 0 else None)
        
            cols_to_drop.append(col)
            cols_to_drop.append(np_label)
            

        df.drop(cols_to_drop, inplace=True, axis=1)

    def _aggregate_rows(self, df):
        '''
        Aggregate rows so that each second has a single entry
        in dataframe 
        '''

        #lambdas to drop any NaNs from a set, revert to scaler
        drop_nan = lambda a: {x for x in a if x==x}
        revert_to_scalar = lambda a: a.values[len(a)-1] if len(a) > 0 else None
        drop_missing = lambda a: a if a < 1000 else None

        drop_nan.__name__ = 'drop_na'
        revert_to_scalar.__name__ = 'scalar'
        drop_missing.__name__ = 'drop_mis'

        dict_apply = {}

        for col in df.columns[3:]:
            dict_apply[str(col)] = (set, drop_nan, revert_to_scalar)

        df_agg = pd.concat([df.groupby(by='timestamp')['catsid'].count(),
                            df.groupby(by='timestamp')['type'].apply(set),
                            df.groupby(by='timestamp').agg(dict_apply)],
                            axis=1)

        
        df_agg.rename(columns={'catsid':'merged_n'}, inplace=True)

        to_drop = [col for col in df_agg.columns[2:] if 'set' in col]
        df_agg = df_agg.drop(columns=to_drop)

        to_drop = [col for col in df_agg.columns[2:] if 'drop_na' in col]
        df_agg = df_agg.drop(columns=to_drop)

        dict_cols = {}
        for col in df_agg.columns[2:]:
            dict_cols[col] = col[0]
        
        df_agg = df_agg.rename(columns=dict_cols)
        
        return df_agg

    time_series = property(_get_data)

    