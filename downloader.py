

import threading
from queue import Queue
import sys, time
import pandas as pd
import requests
import cProfile
import time
def download(url, save_path, lib="requests"):
    global q
    """ Downloads file to specified path, if server returns status codes other than 200 url is saved
    """
    # the speed diference is insignificant between 'requests' and 'wget'

    f_name = url.split("/")[-1]
    save = save_path+"/"+f_name

    if lib == "requests":
        
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            # next_run.append(url)
            q.put(url)
            print("Server did not response correctly, url added to the end of queue")
            # print("Added {} to NEW ROUND (bach size: {})".format(f_name, len(next_run)))
        
        print("Downloading {}".format(url))

        with open(save, 'wb') as f:
            for chunk in r.iter_content(1024):
                if chunk:
                    f.write(chunk)

    # if lib == "wget":
    #     try:
    #         wget.download(url, save)
    #     except:
    #         next_run.append(url)
    #         print("Added {} to NEW ROUND (bach size: {})".format(f_name, len(next_run)))


def threader(save_path):
    global q
    while q.empty() != True:
        # gets an worker from the queue
        worker = q.get()

        # Run the example job with the avail worker in queue (thread)
        download(worker, save_path)

        # completed with the job
        q.task_done()


def start_threads(num_threads, save_path):

    for x in range(num_threads):
        time.sleep(0.5)
        t = threading.Thread(target=threader, args=(save_path,))

         # classifying as a daemon, so they will die when the main dies
        t.daemon = True
        # print("Thread started")
        # begins, must come after daemon definition
        t.start()


def main():
    global q
    start = time.time()
    year_file = sys.argv[1]
    save_path = sys.argv[2]
    num_threads = int(sys.argv[3])

    df = pd.read_csv(year_file)
    urls = list(df["url"].values)

    q = Queue()

    for worker in urls:
        q.put(worker)

    start_threads(num_threads, save_path)

    q.join()

    print("Time used: ", (time.time() - start)/3600)

if __name__ == '__main__':
    main()