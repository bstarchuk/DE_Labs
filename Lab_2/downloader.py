import os
import requests
import zipfile


class Downloader:
    @staticmethod
    def download(folder_name, url):
        filename = url.split("/")[-1]

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        response = requests.get(url)
        with open(os.path.join(folder_name, filename), "wb") as file:
            file.write(response.content)

    @staticmethod
    def download_many(folder_name, urls):
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        for url in urls:
            filename = url.split("/")[-1]
            response = requests.get(url)

            with open(os.path.join(folder_name, filename), "wb") as file:
                file.write(response.content)

    @staticmethod
    def unarchiver(folder_name, destination_folder_name):
        for _, _, files in os.walk(folder_name):
            for file_name in files:
                with zipfile.ZipFile(os.path.join(folder_name, file_name), "r") as zip_ref:
                    csv_file = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')][0]
                    zip_ref.extract(csv_file, path=folder_name)

                os.remove(os.path.join(folder_name, file_name))

                with zipfile.ZipFile("{}.zip".format(destination_folder_name), "a") as archive:
                    archive.write(os.path.join(folder_name, csv_file), arcname=csv_file)
