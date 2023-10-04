from APIInterface import * 
import os
import json
from tqdm import tqdm
import glob
import csv
import pandas as pd
from pprint import pprint

#ouput directory name
output_directory = "China/"
output_tsv_filename = "China_Narrative.tsv"
id_filename = "Tweet_ids/China_Narrative_(dehydrated).xlsx"

output_directory = "Uyghur/"
output_tsv_filename = "Uyghur_Narrative.tsv"
id_filename = "Tweet_ids/Uyghur_Narrative_(dehydrated).xlsx"

if not os.path.exists(output_directory):
    os.makedirs(output_directory)
    print("Directory created:", output_directory)
else:
    print("Directory already exists:", output_directory)


def get_tweet_ids_from_xlsx(fname):
    dfs = pd.read_excel(fname, sheet_name=None)
    full_id_list = []
    for sheet in dfs.keys():
        id_strs = map(str, list(dfs[sheet]["id"]))
        full_id_list+=id_strs
    return full_id_list

def done_ids(output_directory):
    files = glob.glob(output_directory+"*.json")
    ids = []
    for f in files:
        ids.append(f.split("/")[1].replace(".json",""))
    return ids

def get_tweet_ids_from_dir(f_type,input_directory):
    files = glob.glob(input_directory+"*"+f_type)
    full_id_list = []
    for f in files:
        print(f)
        print(pd.read_csv(f))
        #full_id_list.append(get_tweet_ids(f))
    return full_id_list


#input file
#ids = get_tweet_ids_from_dir(f_type="csv",input_directory="Uyghur_Tweet_ids/")
#id_filename = "ids.txt"
#ids = get_tweet_ids(filename=id_filename)



#connect to tweepy client
twAPI = auth1()

def jsons_to_tsv(jsons,csv_file_path):

    #header = set()
    #for row in jsons:
    #    header.update(row.keys())

    header = ['id','full_text','lang','url',"favorite_count","retweet_count"]    


    # Open the CSV file for writing
    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header,delimiter='\t')
        writer.writeheader()

        # Write each dictionary to a new row in the CSV file
        for row in jsons:
            if not "user" in row.keys():
                row["user"] = {"screen_name":row["full_text"]}

            to_write={"id":row["id_str"],
                      "full_text":row["full_text"],
                      "lang":row["lang"],
                      "url":"https://twitter.com/"+row["user"]["screen_name"]+"/status/"+str(row["id_str"]),
                      "favorite_count":row["favorite_count"],
                      "retweet_count":row["retweet_count"]
                      }
            writer.writerow(to_write)

def reduce_tweet_json(tweet):
    if not "user" in tweet.keys():
        #This Case is only for SUSPENDED/FORBIDDEN/Not Found to replace "user" with that
        tweet["user"] = tweet["full_text"]
    else:
        tweet["user"] = tweet["user"]["screen_name"]

    reduced_tweet={"id":tweet["id_str"],
                "full_text":tweet["full_text"],
                "lang":tweet["lang"],
                "url":"https://twitter.com/"+tweet["user"]+"/status/"+str(tweet["id_str"]),
                "favorite_count":tweet["favorite_count"],
                "retweet_count":tweet["retweet_count"]
                }
    return reduced_tweet

def load_tweets_in_dir(directory):
    files = glob.glob(directory+"*.json")
    tweets = []
    for file_path in files:
        with open(file_path, 'r') as f:
            tweet = json.load(f)
        tweets.append(tweet)
    return tweets

def load_tweet_by_id(directory,_id):
    #Load a single json from directory based on _id
    file_path = directory+_id+".json"
    with open(file_path, 'r') as f:
        tweet = json.load(f)
    return tweet


def fix_annotation_ids(annotations):

    for idx,row in annotations.iterrows():
        new_id = row["url"].split("/")[-1] 
        annotations.iloc[[idx],[0]] = str(new_id) 
    return annotations


def add_annotations_to_tweet(tweet,annotations):

    #Find annotation record
    row = annotations.loc[annotations['id'] == str(tweet["id"])]
    tweet["Relevant to China/ Uighur?"] = row["Relevant to China/ Uighur?"].values[0]       
    tweet["Relevant to Uighur/country?"] = row["Relevant to Uighur/country?"].values[0]       
    tweet["Relevant to country and/or other topic?"] = row["Relevant to country and/or other topic?"].values[0]       
    tweet["Note1"] = row["Note1"].values[0]       
    tweet["Note2"] = row["Note2"].values[0]  

    return tweet

def process_tweets_by_sheets_with_annotations(input_fname,output_directory,output_tsv_filename,annotation_file):
    dfs = pd.read_excel(input_fname, sheet_name=None)

    annotations = pd.read_excel(annotation_file)
    annotations = fix_annotation_ids(annotations)
    #Removes NaN from output
    annotations = annotations.fillna("") 
    for sheet in dfs.keys():
        print("Starting ",sheet)
        tsv_file_path = output_tsv_filename.split(".")[0]+sheet.replace(" ","")+"."+output_tsv_filename.split(".")[1]
        print("output...",tsv_file_path)
        id_strs = map(str, list(dfs[sheet]["id"]))
        header = ['id','full_text','lang','url',"favorite_count","retweet_count","Relevant to China/ Uighur?","Relevant to Uighur/country?","Relevant to country and/or other topic?","Note1","Note2"]
        with open(tsv_file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header,delimiter='\t')
            writer.writeheader()
            for _id in tqdm(id_strs):
                tweet = load_tweet_by_id(output_directory,_id)
                tweet = reduce_tweet_json(tweet)
                tweet = add_annotations_to_tweet(tweet,annotations)
                writer.writerow(tweet)


def process_tweets_by_sheets(input_fname,output_directory,output_tsv_filename):
    dfs = pd.read_excel(input_fname, sheet_name=None)
    for sheet in dfs.keys():
        print("Starting ",sheet)
        tsv_file_path = output_tsv_filename.split(".")[0]+sheet.replace(" ","")+"."+output_tsv_filename.split(".")[1]
        print("output...",tsv_file_path)
        id_strs = map(str, list(dfs[sheet]["id"]))
        header = ['id','full_text','lang','url',"favorite_count","retweet_count"]
        with open(tsv_file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header,delimiter='\t')
            writer.writeheader()
            for _id in tqdm(id_strs):
                tweet = load_tweet_by_id(output_directory,_id)
                tweet = reduce_tweet_json(tweet)
                writer.writerow(tweet)

def collect_and_dump_tweets(ids,twAPI,output_directory):
    for _id in tqdm(ids):
        try:
            print("Collecting...",_id)
            tweet = twAPI.get_status(_id, tweet_mode="extended")
            with open(output_directory+tweet._json["id_str"]+".json", 'w') as f:
                json.dump(tweet._json, f)
        except Exception as e:

            if "suspended" in e.args[0]:
                print("***************")
                print("suspended")
                print("***************")
                bad_tweet={"id_str":_id,
                      "full_text":"SUSPENDED",
                      "lang":"SUSPENDED",
                      "url":"SUSPENDED",
                      "user":{"screen_name":"SUSPENDED"},
                      "favorite_count":"SUSPENDED",
                      "retweet_count":"SUSPENDED"
                      }
                with open(output_directory+str(_id)+".json", 'w') as f:
                    json.dump(bad_tweet, f)
            elif "404 Not Found" in e.args[0]:
                print("***************")
                print("404 Not Found")
                print("***************")
                bad_tweet={"id_str":_id,
                      "full_text":"Not Found",
                      "lang":"Not Found",
                      "url":"Not Found",
                      "user":{"screen_name":"Not Found"},
                      "favorite_count":"Not Found",
                      "retweet_count":"Not Found"
                      }
                with open(output_directory+str(_id)+".json", 'w') as f:
                    json.dump(bad_tweet, f)
            elif "Forbidden" in e.args[0]:
                print("***************")
                print("Forbidden")
                print("***************")
                bad_tweet={"id_str":_id,
                      "full_text":"Forbidden",
                      "lang":"Forbidden",
                      "url":"Forbidden",
                      "user":{"screen_name":"Forbidden"},
                      "favorite_count":"Forbidden",
                      "retweet_count":"Forbidden"
                      }
                with open(output_directory+str(_id)+".json", 'w') as f:
                    json.dump(bad_tweet, f)
            else:
                print(e)

ids = set(get_tweet_ids_from_xlsx(id_filename)) - set(done_ids(output_directory))
collect_and_dump_tweets(ids=ids,twAPI=twAPI,output_directory=output_directory)

tweets = load_tweets_in_dir(directory=output_directory)

jsons_to_tsv(jsons = tweets,csv_file_path = output_directory+output_tsv_filename)
