import dateparser
import pymysql
import time
import re
import os
import pandas as pd
from tqdm import tqdm
from langdetect import detect
from textblob import TextBlob
from mtranslate import translate
from urllib.parse import urlparse
from googleapiclient.discovery import build
import googleapiclient


# get playlist id from channel id
def get_playlist_id(channel_id):
    cil = list(channel_id)
    cil[1] = 'U'
    playlist_id = ''.join(cil)
    return playlist_id

# normalize date
def normalize_metadate(datetime):
    dateObj = dateparser.parse(datetime)
    date = dateObj.strftime('%Y-%m-%d %H:%M:%S') if dateObj is not None else ''
    return date

# get category from id
def get_category(id):
    category_list = {
                        '1'  : 'Film & Animation', 
                        '2'  : 'Autos & Vehicles', 
                        '10' : 'Music',  
                        '15' : 'Pets & Animals', 
                        '17' : 'Sports', 
                        '18' : 'Short Movies', 
                        '19' : 'Travel & Events', 
                        '20' : 'Gaming', 
                        '21' : 'Videoblogging', 
                        '22' : 'People & Blogs', 
                        '23' : 'Comedy', 
                        '24' : 'Entertainment', 
                        '25' : 'News & Politics', 
                        '26' : 'Howto & Style', 
                        '27' : 'Education', 
                        '28' : 'Science & Technology', 
                        '29' : 'Nonprofits & Activism', 
                        '30' : 'Movies', 
                        '31' : 'Anime/Animation', 
                        '32' : 'Action/Adventure', 
                        '33' : 'Classics', 
                        '34' : 'Comedy', 
                        '35' : 'Documentary', 
                        '36' : 'Drama', 
                        '37' : 'Family', 
                        '38' : 'Foreign', 
                        '39' : 'Horror', 
                        '40' : 'Sci-Fi/Fantasy', 
                        '41' : 'Thriller', 
                        '42' : 'Shorts', 
                        '43' : 'Shows', 
                        '44' : 'Trailers'   
                    }
    return category_list[id]

# detect language of channel description
def get_language(description):
    try:
        post_lang = detect(description)
    except:
        post_lang = 'N/A'
    return post_lang

# Cleaning text before sentiment / toxicity
def clean_text(text):
    if type(text) == bytes: #Decoding byte strings
        text = text.decode('utf-8') 
    text = translate(text) #Translating
    text = re.sub(r"http\S+", "", text) #Removing URLS
    text = re.sub(r"\\b\w+(?:\.\w+)+\s*", '',  text.replace('-', '')) #Remvoing .coms
    if not text:  #Skipping empty strings
        return "" 
    return text

# get sentiment score
def get_sentiment_score(text):
    #Cleaning the text
    text = clean_text(text)
    if not text: return 0 #Returning empty strings
    #Getting Score
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    sentiment_score_rounded = round(sentiment_score, 6)
    return sentiment_score_rounded

# get toxicity score
def get_toxicity_score(text):
    #Cleaning the text
    text = clean_text(text)
    if not text: return 0 #Returning empty strings
    #Sending Request
    API_KEY = 'AIzaSyCdOGjynFqrd5A-gkKKeYjqs0UIMP7FGjc'
    service = build('commentanalyzer', 'v1alpha1', developerKey = API_KEY)
    analyze_request = {
                'comment': { 'text': text },
                'requestedAttributes': {'TOXICITY': {}}}
    try: 
        probability = service.comments().analyze(body=analyze_request).execute()
        result = probability['attributeScores']['TOXICITY']['summaryScore']['value']
        return result
    #Catching Exceptions
    except googleapiclient.errors.HttpError as e:
        if 'language' in e.content.decode('utf-8'): 
            #Some  examples of what gets caught here: 
            #Puke.....we are fkd
            #Whooo waaaaants maaaan tiiiitties
            # print("Unsupported Language in Toxicity Request: {}".format(text))
            return 0
        else: 
            print("Uncaught Http error on Toxicity Score: {}".format(e))
    except Exception as e: 
        print("Uncaught error on Toxicity Score: {}".format(e))

# get video id from url
def get_video_id(video_url):
    query = urlparse(video_url)
    if query.hostname == 'youtu.be':
        return query.path[1:]
    if query.hostname in ('www.youtube.com', 'youtube.com', 'm.youtube.com'):
        if query.path == '/watch':
            p = query.query
            return p[2:]
        if query.path[:7] == '/embed/':
            return query.path.split('/')[2]
        if query.path[:3] == '/v/':
            return query.path.split('/')[2]
    return None

def get_admin_api_key(user_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("select `youtube_data_api_key_value` from `youtube_data_api_key` where user_id = %s", user_id)
            api_key = cursor.fetchone()['youtube_data_api_key_value']
        connection.close()
        return(api_key)    
    except:
        api_key = None
        connection.close()
        # print("User API key missing from DB! Using defulat key...")
    return api_key

def get_app_api_key(tracker_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("select `tracker_creater` from `tracker` where tracker_id = %s", tracker_id)
            user_id = cursor.fetchone()['tracker_creater']
        with connection.cursor() as cursor:
            cursor.execute("select `youtube_data_api_key_value` from `youtube_data_api_key` where user_id = %s", user_id)
            api_key = cursor.fetchone()['youtube_data_api_key_value']
        connection.close()
        return(api_key)    
    except:
        api_key = None
        connection.close()
        # print("User API key missing from DB! Using defulat key...")
    return api_key

def get_connection():
    count = 0
    while True:
        count += 1
        try:
            connection = pymysql.connect(host='localhost',
                                 user='yttrackers',
                                 password='YTSummer2018',
                                 db='youtube_tracker',
                                 charset='utf8mb4',
                                 use_unicode=True,
                                 cursorclass=pymysql.cursors.DictCursor)
            return connection
        #Error handeling
        except Exception as e:
            if isinstance(e, pymysql.err.OperationalError): 
                # Unable to access port (Windows Error), trying again
                # See https://docs.microsoft.com/en-us/biztalk/technical-guides/settings-that-can-be-modified-to-improve-network-performance
                # print("Socket error uploading to db. Trying again... {}".format(count))
                time.sleep(3)
                count += 1
                if count > 10: print("Failed to connect to db {} times in a row".format(count))
            else: 
                # Uncaught errors
                raise Exception("We aren't catching this mySql get_connection Error: {}".format(e))


def commit_to_db(query, data, yt_api, id, request_type):
    # while True: 
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
                cursor.execute(query, data)
                connection.commit()
                connection.close()
                yt_api.success_content(id, request_type)
                return            
    #Error handeling
    except Exception as e:
        if isinstance(e, pymysql.err.IntegrityError) and e.args[0]==1062:
            # Duplicate Entry, already in DB
            yt_api.failed_content(id, request_type, "Good API request, but data is already duplicate entry in the DB")
            connection.close() 
            return
        elif e.args[0] == 1406:
            # Data too long for column
            print(e)
            yt_api.failed_content(id, request_type, "Good API request, but data is Too Long for DB Column")
            connection.close()
            return 
        else: 
            # Uncaught errors
            raise Exception("We aren't catching this mySql commit_to_db Error: {}".format(e))

def save_csv(data, fname, loc="Validation_data"):
    df = pd.DataFrame(data)
    dest = os.path.join(os.getcwd(), loc)
    if not os.path.exists(dest):
        os.makedirs(dest)
    path = os.path.join(os.getcwd(), loc+"/"+fname+".csv")
    update_csv(df, path)
    return fname

def update_csv(df, path):
    if not os.path.exists(path):
        df.to_csv(path, header=True, mode='w', index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    else:
        df.to_csv(path, header=False, mode='a', index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    del df

def clean_mp_files(dir_name):
    #Cleaning the multi_processing files
    for root, _, files in os.walk(dir_name, topdown=False):
        for file in files:
            if '_id' in file and file.endswith('csv'):
                new_name = file.split('_id', 1)[0]
                df = pd.read_csv(os.path.join(root,file))
                path = os.path.join(os.getcwd(), dir_name+"/"+new_name+".csv")
                update_csv(df, path)
                #deleting CSV
                os.remove(os.path.join(root,file))
                del df

def validation_check(run_name):
    #Goes through all the CSV files in Vallidation and compares them. 
    #It will leave behind rows that processed differently on Single Threaded / Parallel Process
    print("\nValidating Run")
    fname = run_name
    #Getting files
    path = os.path.join(os.getcwd(), "Validation_data")
    for root,_,files in os.walk(path):
        for file in files:
            if run_name in file: 
                #Opening Files
                main_file = os.path.join(root, file)
                if "_parallel" in file: 
                    compare_file = os.path.join(root, file.replace("_parallel", "_single"))
                    if "Failure" in file: 
                        export_name = "Validation_" +fname + "_failure"
                        run_name = "Success_"+run_name
                    else:
                        export_name = "Validation_" +fname + "_success"
                        run_name = "Failure_"+run_name
                elif "_single" in file:
                    compare_file = os.path.join(root, file.replace("_single", "_parallel"))
                    if "Failure" in file: 
                        export_name = "Validation_" +fname + "_failure"
                        run_name = "Success_"+run_name
                    else:
                        export_name = "Validation_" +fname + "_success"
                        run_name = "Failure_"+run_name
                df1 = pd.read_csv(main_file)
                df2 = pd.read_csv(compare_file)
                #Checking Validation and removing Rows
                df_diff = pd.concat([df1,df2])
                df_diff = df_diff.drop_duplicates(subset=["Content_id","Crawler","Request_type"], keep=False)
                export_path = os.path.join(os.getcwd(), "Validation_data/"+export_name+".csv")
                df_diff.to_csv(export_path, header=True)
    print("Validation Finished! Check folder for more info: {}".format(export_path))
