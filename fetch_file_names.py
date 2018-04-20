import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
from os.path import join

def form_url(host_path, year, month,file_name=""):
	""" Returns url for specified year and month
	"""
	return host_path+str(year)+"/"+str(year)+"-"+str(month)+"/"+file_name


def get_files(url):
	""" Returns all files from given url as a list
	"""
	resp = requests.get(url)
	soup = BeautifulSoup(resp.content,"html.parser")
	list_elements = soup.find_all("li")
	size = [li.text.split(" ")[-1].replace("M","") for li in list_elements]
#     print(size)
	list_elements = [li.a.get("href") for li in list_elements]
	return list_elements, size

def get_url(host_path, file_name):
    """ Goes to lambda function
    """
    date = file_name.split("-")[1]
    year = date[:4]
    month = date[4:6]
    url = form_url(host_path,year, month, file_name)
    return url

def main():

	host_path = "https://dumps.wikimedia.org/other/pagecounts-raw/"

	year_start = int(sys.argv[1])
	year_end = int(sys.argv[2])
	output_dir = sys.argv[3]

	if year_start < 2007:
		print("There are no dumps prior to 2007")
		return
	if year_start > 2015:
		print("For years after 2015 use the API") 
		return

	#list of years to loop over: 2008-2014
	years = range(year_start,2015)
	#list of months to loop over: 01 - 12
	months = ["%.2d" % i for i in range(1, 13)]

	# urls and files (with information on size) for each year are fetched
	final_size = 0
	for year in years:
		year_lst = []
		memory_lst = []
		for month in months:
			url = form_url(host_path, year, month)
			files, sizes = get_files(url)
			year_lst.append(files)
			memory_lst.append(sizes)
		year_lst = sum(year_lst, [])
		memory_lst = sum(memory_lst, [])
	#     year_lst = [file for file in year_lst if file.startswith("pagecounts")] #!!!!
	
		df = pd.DataFrame({
			"file": year_lst,
			"size": memory_lst,
		})

		df["filter"] = df.file.apply(lambda x: "yes" if x.startswith("pagecounts") else "no")
		df = df[df["filter"] == "yes"]
		df = df.drop("filter", axis=1)
		df["size"] = pd.to_numeric(df["size"], errors='coerce')
		df["url"] = df["file"].apply(lambda x: get_url(host_path, x))

		path = join(output_dir, str(year)+".csv")
		df.to_csv(path,index=False)
		print("File saved:"+path)
		total = df["size"].sum()/1024
		print("Cumulative file size: {} in GB".format(total))
		print("Number of files:", df.shape[0])
		final_size = final_size + total
		if final_size > 1024:
			print("Total size to be downloaded {} in {}".format(final_size/1024, "TB"))
		else:
			print("Total size to be downloaded {} in {}".format(final_size, "GB"))

if __name__ == '__main__':
	main()
