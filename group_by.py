
# coding: utf-8

# In[73]:

import pandas as pd 
import os
from os import listdir
from os import listdir
from os.path import isfile, join
import tqdm
from urllib.parse import quote, unquote
import sys, time
import gc

def get_files(path):
    """ Returns a list of files from a directory
        Input parameter: path to directory
    """
    mypath = path
    complete = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    file_paths = [mypath+file for file in complete]
    return file_paths

def load_files_to_lst(file_paths):
    """ Loads all files into a pandas dataframe, appends dataframes to 
        a list which is returned
    """
    df_lst = []
    for file in tqdm.tqdm(file_paths):
        try:
            df = pd.read_csv(file, sep=" ")
        except:
            print("Skiped: ", file)
            pass
        df.columns = ['name', 'views']
        df_lst.append(df)
    return df_lst


def concate_to_df(lst):
    """ Concats all dataframes from the given list to one dataframe
    """
    df = pd.concat(lst,ignore_index=True)
    print(df.info())
    return df

def save_aggregation(agr, path):
    """ Saves a csv file on the specified location 
    """
    name_q = agr.index
    views = agr.values
    name_u =[unquote(i) for i in agr.index]
    df = pd.DataFrame({
        "name_q":name_q,
        "name_u":name_u,
        "views":views,
    })
    print("File saved at {}".format(path))
    df.to_csv(path, encoding="utf-8", index=False)

def main():

    file_path = sys.argv[1]
    output_path = sys.argv[2]

    start = time.time()
    files = get_files(file_path)
    print("Loading files.. \n")
    lst = load_files_to_lst(files)
    print("Concatinating {} dataframes.. \n".format(len(lst)))
    df = concate_to_df(lst)
    lst = None
    gc.collect()
    print("Aggregating.. \n")
    agr = df.groupby(by="name")["views"].sum()
    save_aggregation(agr, output_path)
    print("Time used: ", (time.time() - start)/60)


if __name__ == '__main__':
    main()