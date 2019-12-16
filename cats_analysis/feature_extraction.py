
import pandas as pd
import numpy as np

def featurize_trip(summary_frame):
    '''
    Unstack/flatten a data frame
    and return a dataframe of features
    
    Parameters:
    ------
    summary_frame, pandas.DataFrame.  A table of features/variables
    in 2D format.
    
    Returns:
    -------
    pandas.DataFrame
    '''
    df = pd.DataFrame(summary_frame.unstack())
    df['feature'] = df.index
    df.index = df['feature']
    df = df.drop(['feature'], axis=1)
    del df.index.name
    return df.T

def featurize_trips(summary_frames):
    df_trips = None
    
    for df in summary_frames:
        row = featurize_trip(df.copy(deep=True))
        if isinstance(df_trips, (pd.DataFrame)):
            df_trips = pd.concat([df_trips, row])
        else:
            df_trips = pd.DataFrame(row)
            
    idx = np.arange(len(summary_frames))
    df_trips.index = idx
    return df_trips