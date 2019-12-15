'''
summary

Classes and functions for summarising Trips
Tightly coupled to CleanTrip
'''

import numpy as numpy
import pandas as pd

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

    def calculate(self, resample='30s', smooth=False, interp_missing=False):
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
        resample -- str, interval to aggregate values over (default=30s) 

        interp_missing -- bool, linear interpolation between missing values
                          (default=False)
        '''
        
        df = self._clean_trip.resample(resample, smooth, interp_missing)

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