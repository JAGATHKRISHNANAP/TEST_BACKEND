
import os
import whisper
import psycopg2
import json
from config import AUDIO_DATABASE_NAME,PASSWORD,USER_NAME,HOST,PORT

def transcribe_audio_with_timestamps(audio_file_path):
    model = whisper.load_model("base")
    result = model.transcribe(audio_file_path, language="en", verbose=True, task="transcribe")

    transcribed_text = ""
    for segment in result["segments"]:
        text = segment["text"]
        transcribed_text += text + " " 
    return transcribed_text.strip()  

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

False


def save_file_to_db(filename, file_data):
    try:
        # Ensure file_data is valid JSON
        file_data_json = json.dumps(file_data)

        connection = psycopg2.connect(
            # dbname="audio_database",
            # user="postgres",
            # password="jaTHU@12",
            # host="localhost",
            # port="5432"
            dbname=AUDIO_DATABASE_NAME,
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port= PORT
        )
        cursor = connection.cursor()
        cursor.execute("INSERT INTO audio_transcript (filename, file_data) VALUES (%s, %s)", (filename, file_data_json))
        connection.commit()
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error saving file to database: {e}")
        return False


