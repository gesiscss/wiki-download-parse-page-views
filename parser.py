
import threading
from queue import Queue
import sys, time
import pandas as pd
import requests
import time
import os
from os import listdir
from os import listdir
from os.path import isfile, join
import gc

def get_files(path):
    """ Returns a list of files in a directory
        Input parameter: path to directory
    """
    mypath = path
    complete = [join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
    return complete

def load_names_df(path):
    """ Loads pandas df with names from a csv file
    """    
    df = pd.read_csv(path, encoding="utf-8")
    df = df.drop(["names_u"], axis=1)
    # print(df)
    df.columns = ["name"]
    return df 


def parse(path_old, path_new, names_df):
    """ Reads file, eliminates unneeded data, filters for project "en" and sspecified names
    """
    global bad_files

    f_name = path_old.split("/")[-1]

    try:
        df = pd.read_csv(path_old, sep=" ")
        df.columns = ["project", "name", "views", "size"]
        df = df[df["project"] == "en"]
        df = df.drop(["size","project"], axis=1)
        df = df.merge(names_df, on=["name"])
        path_new = path_new + f_name
        df.to_csv(path_new, sep=" ",compression="gzip", index=False, header=False)
        print("{} > {}, DONE! ".format(path_old, path_new))
    except:
        try:
            df = pd.read_csv(path_old, sep=" ", encoding="latin_1")
            df.columns = ["project", "name", "views", "size"]
            df = df[df["project"] == "en"]
            df = df.drop(["size","project"], axis=1)
            df = df.merge(names_df, on=["name"])
            path_new = path_new + f_name
            df.to_csv(path_new, sep=" ",compression="gzip", index=False, header=False)
            print("{} > {}, DONE! ".format(path_old, path_new))
        except:
            print("SKIP")


def threader(names_df, save_dir):
    global q
    while q.empty() != True:
        # gets a worker from the queue
        worker = q.get()

        # Run the example job with the avail worker in queue (thread)
        parse(worker, save_dir, names_df) 

        # completed with the job
        q.task_done()


def start_threads(num_threads, names_df, save_dir):

    for x in range(num_threads):
        time.sleep(0.05)
        t = threading.Thread(target=threader, args=(names_df, save_dir,))

         # classifying as a daemon, so they will die when the main dies
        t.daemon = True
        # print("Thread started")
        # begins, must come after daemon definition
        t.start()


def main():
    global q
    # bad_files = []
    start = time.time()
    names_file = sys.argv[1]
    files_dir = sys.argv[2]
    save_dir = sys.argv[3]
    num_threads = int(sys.argv[4])

    df = load_names_df(names_file)
  
    files = get_files(files_dir)

    q = Queue()

    for worker in files:
        q.put(worker)

    start_threads(num_threads, df, save_dir)

    q.join()

    print("Time used: ", (time.time() - start)/3600)

    # pd.DataFrame(bad_files).to_csv("bad_"+names_file.split("/")[-1])

if __name__ == '__main__':
    main()