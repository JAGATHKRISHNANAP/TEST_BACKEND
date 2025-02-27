

import os

# Move UP one level and create the uploads folder there
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads')
SUB_FOLDERS = ['audio', 'csv', 'excel']
ALLOWED_EXTENSIONS = {'mp3', 'wav', 'ogg', 'flac','m4a'}

# Ensure the upload folder and subfolders exist
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Create the subdirectories
for sub_folder in SUB_FOLDERS:
    folder_path = os.path.join(UPLOAD_FOLDER, sub_folder)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


DB_NAME = 'datasource'
USER_NAME = 'postgres'
PASSWORD = 'jaTHU@12'
HOST = 'localhost'
# HOST = '43.204.149.125'
PORT = '5432'
AUDIO_DATABASE_NAME = 'audio_database'
