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


class DatasetOverview(object):
    pass

class TripSummaryStatistics(object):
    '''
    Summary statistics for a cleaned trip
    '''
    def __init__(self, clean_trip):
        '''
        Create an instance of TripSummaryStatistics for an individual
        trip.

        Parameters:
        -------
        clean_trip, cats_analysis.io.CleanTrip.  Cleaned Trip Data
        '''
        self._clean_trip = clean_trip
        self._summary = None
        self._duration = -1.0


    def _get_duration(self):
        '''
        Return trip duration in HH:MM:SS
        '''
        return self._get_duration

    def _get_summary_table(self):
        '''
        Summary statistics for the trip
        '''
        return self._summary

    def calculate(self, resample='30s', interp_missing=False):
        '''
        Calculate basic summary statistics for trip.

        1. Trip Duration
        2. Completely empty fields
        3. For every field:
            3.1 Mean,
            3.2 Stdev,
            3.3 Histogram.... think about that one. (numpy.hist?)
            3.4 

        Parameters:
        ----------
        resample -- str, interval to aggregate values over (defaul=30s) 
        interp_missing -- bool, linear interpolation between missing values
                          (default = False)
        '''
        
        df = self._clean_trip.resample(resample, interp_missing)

        self.duration = df.index.max() - df.index.min()

        results = {}
        results['per_missing'] = (1 - df.count()/df.shape[0])*100
        results['mean'] = df.mean()
        results['std'] = df.std()
        results['min'] = df.min()
        results['max'] = df.max()
        results['median'] = df.quantile(q=0.5)
        results['iqr'] = df.quantile(q=0.75) - df.quantile(q=0.25)
        self._summary = pd.DataFrame(results)
        
    summary_table = property(_get_summary_table)
    trip_duration = property(_get_duration)

    


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
    def __init__(self, filepath, wave_features=None):
        '''
        Constructor method

        Parameters:
        ---------
        filepath - str, the path of the raw data file for trip (.csv)
        wave_features - List, a list of features to engineer. Specified
                        by a string with the same name as the 
                        equivalent numpy array method
                        e.g. 1 ['mean', 'std', 'min', 'max']
                        e.g. 2 ['mean']

        '''
        self._filepath = filepath
        self._data = None
        if wave_features == None:
            wave_features = []
        self._wave_features = wave_features
    
    def _get_data(self):
        '''
        Returns:
        --------
        pandas.DataFrame containing the cleaned
        time series data
        '''
        return self._data

    def resample(self, rule, interp_missing=False):
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
        if interp_missing:
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
            for func in self._wave_features:
                feature_name = col + '_' + func
                df[feature_name] = df[np_label].map(lambda x: 
                                                    getattr(np, func)(x, axis=0)
                                                    if x.shape[0] > 0 else np.nan)
        
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

    