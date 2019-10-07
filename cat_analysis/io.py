'''
io

Input output functions for CATS data

Data are stored within individual trips files
'''

import os

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



