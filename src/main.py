import os
import pandas as pd
import pysftp
import boto3
import tqdm

def make_folder_empty(folder_path):
    """
    Очищает указанную папку, удаляя все файлы и подпапки.
    """
    items = os.listdir(folder_path)
    for item in items:
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            make_folder_empty(item_path)
    os.rmdir(folder_path)

def download_files_from_sftp(sftp_host, sftp_user, sftp_pass, sftp_directory, local_directory):
    """
    Загружает файлы с SFTP сервера в локальную папку.
    """
    with pysftp.Connection(host=sftp_host, username=sftp_user, password=sftp_pass) as sftp:
        sftp.chdir(sftp_directory)
        files = sftp.listdir()
        csv_files = [file for file in files if file.endswith(".csv")]
        
        if csv_files:
            make_folder_empty(local_directory)
            for csv_file in tqdm.tqdm(csv_files):
                local_path = os.path.join(local_directory, csv_file)
                sftp.get(csv_file, local_path)

def upload_files_to_s3(local_directory, s3_client, bucket_name):
    """
    Загружает локальные файлы в S3.
    """
    for load_file in os.listdir(local_directory):
        file_path = os.path.join(local_directory, load_file)
        s3_client.upload_file(file_path, bucket_name, load_file)

if __name__ == "__main__":
    # Параметры подключения и настройки
    sftp_host = "Адрес SFTP"
    sftp_user = "Имя пользователя"
    sftp_pass = "Пароль пользователя"
    sftp_directory = "Нужная/Директория/Для/Загрузки/Файлов"
    local_directory = "Локальная директория (куда загружать)/"
    
    access_key = 'Ключ от S3'
    secret_key = 'Секретный ключ от S3'
    s3_endpoint_url = 'Адрес хранилища'
    bucket_name = "Имя бакета"
    
    # Создаем клиента S3
    s3_client = boto3.client(
        's3',
        endpoint_url=s3_endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    # Процесс загрузки
    download_files_from_sftp(sftp_host, sftp_user, sftp_pass, sftp_directory, local_directory)
    upload_files_to_s3(local_directory, s3_client, bucket_name)

