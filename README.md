# wiki-download-parse-page-views
Pipeline for downloading, parsing and aggregating static page view dumps from Wikipedia.

# How it works?

In case you need an anual number of pageviews for specific pages on Wikipedia before 2015. you will unfortunately not be able to rely on the API (at least not at time of writing this doc) as it gives access to new records (post 2015). However, a collection of [static dumps](https://dumps.wikimedia.org/other/pagecounts-raw/) is available. 

This pipline was made in order to: 

1. Fetch names of all files to be downloaded
2. Download the needed files (paralelized)
3. Parse them after downloading (paralelized)
4. Aggregate files for each year in order to get the anual number of views for selected pages

The following scripts need to be ran respectively:

1. [fetch_file_names.py](https://github.com/gesiscss/wiki-download-parse-page-views/blob/master/fetch_file_names.py)
2. [downloader.py](https://github.com/gesiscss/wiki-download-parse-page-views/blob/master/downloader.py)
3. [parser.py](https://github.com/gesiscss/wiki-download-parse-page-views/blob/master/parser.py)
4. [group_by.py](https://github.com/gesiscss/wiki-download-parse-page-views/blob/master/group_by.py)

# Fetching file names and URLs

First, we need to get the names of files we want to download. For every year, there is a set of files available, so it is also good to specify about which years we are interested in. 

### fetch_file_names.py 

The script generates a csv file containing file names, file sizes and URLs from which the files should be downloaded. Script parameters:
* year_start - first year to be downloaded
* year_end - last year to be downloaded (all years in between are  downloaded)
* output_dir - directory where files for each year will be stored

```{r, engine='bash', count_lines}
python fetch_file_names.py  [year_start] [year_end] [output_dir]
```
### Output file

file | size |url |
--- |--- |--- |
pagecounts-20140101-000000.gz |82| https://.. |
pagecounts-20140201-000000.gz |81|https://.. |
... | ... | ... |

# Downloading files

Now, when we have downloaded the file names and URLs, we can download them! 

### downloader.py 

This script concurently downloads [Wikipedia pagecount dumps](https://dumps.wikimedia.org/other/pagecounts-raw/) [qzip]. The file previously generated **file.csv** contains a list of urls for the files mentioned. The **path_save** refers to directory where files should be downloaded. 

```{r, engine='bash', count_lines}
python downloader.py [file.csv] [path_save] [thread_number]
```
**THE SERVER IS CURRENTLY BLOCKING IN CASE OF USING MORE THEN 3 THREADS**

# Parsing files

As the files have information on every page on Wikipedia which was accessed within the hour specified in the file name, we should remove page names that we do not need.

### Input file

For parsing, a csv file containing wikipedia page names has to be provided in the following format:

names_u | names_q |
--- |--- |
Barack_Obama |Barack_Obama| 
René_Konen |Ren%C3%A9_Konen| 
Zoran_Đinđić |Zoran_%C4%90in%C4%91i%C4%87|
... | ... | 

The column **names_u** is standard utf-8 encoding (the unquated representation), however in the files a nother type of encoding is used, so we need a **names_q** which is the 'qouated' representation. Both [quote and unquote](https://stackoverflow.com/questions/300445/how-to-unquote-a-urlencoded-unicode-string-in-python) can be done with [urllib](https://docs.python.org/2/library/urllib.html).

### parser.py

Opens specified list of files in **files_dir**, filters them per names in **page_names_file**, saves filtered files in **save_dir** using

**num_threads** 
```{r, engine='bash', count_lines}
python parser.py [page_names_file] [files_dir] [save_dir] [num_threads]
```

# Getting the aggregated pageviews

After parsing the files, it is time to aggregate the page views! 

Loads files from **file_dir** as pandas dataframes, concatinates them, performs aggregation and saves them as csv on **save_path**. 

```{r, engine='bash', count_lines}
python groupby.py [file_dir] [save_path] 
```

### Output file

names_u | names_q | views |
--- |--- | --- |
Barack_Obama |Barack_Obama| 3562998 | 
René_Konen |Ren%C3%A9_Konen| 156456 |
Zoran_Đinđić |Zoran_%C4%90in%C4%91i%C4%87| 96846 |
... | ... | ... |

# Dependencies

```{r, engine='bash', count_lines}
#todo requirements.txt
```
