from io import BytesIO
from zipfile import ZipFile
import urllib.request
from bs4 import BeautifulSoup
import requests
import re
import csv
import psycopg2

# this lambda should read from the a database table to get the filename
# of the most recent, previously downloaded file.  Then it should
# query the SEC page containing the list of files.  It will then
# compare the filename of the first entry in the SEC list with the
# filename of the most recently downloaded file in the database.
# If the filenames are different, then it will download the new
# SEC file and store that information within the database.


conn = psycopg2.connect(
   database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432'
)

def append_data(file_name, list_of_elem):

    with open(file_name, 'a+', newline='') as write_obj:
        writer = csv.writer(write_obj)
        writer.writerows(list_of_elem)

def download_csv():
        DOMAIN = "https://www.sec.gov"
        FAILS_TO_DELIVER_URL = "https://www.sec.gov/data/foiadocsfailsdatahtm"
        req = requests.get(FAILS_TO_DELIVER_URL)
        soup = BeautifulSoup(req.content, 'html.parser')
        a_list = soup.find_all('a', href=re.compile("^/files/data/"))

        file_link = a_list[0].get('href', None)
        file_url = DOMAIN + file_link
        url = urllib.request.urlopen(file_url)

        with ZipFile(BytesIO(url.read())) as my_zip_file:
            for contained_file in my_zip_file.namelist():
                temp_file_name = contained_file.split('.')
                file_name = temp_file_name[0] + ".csv"
                data = []

                for line in my_zip_file.open(contained_file).readlines():
                    line = line.decode('ISO-8859-1')
                    row = line.split('|')
                    data.append(row)

                del data[-1]
                del data[-1]

                append_data(file_name, data)




if __name__ == '__main__':
    download_csv()
