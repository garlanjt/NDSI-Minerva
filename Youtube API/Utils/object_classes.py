from datetime import datetime, timedelta 
import multiprocessing as mp
from tqdm import tqdm
import pandas as pd
import requests
import pymysql
import random
import math
import time
import sys

from Utils.functions import get_admin_api_key, get_app_api_key, save_csv

class ApiKeys:  
    def __init__(self, crawler_name, parallel_process, run_name, print_debug=False, dev_key=False):
        ###-Managing Api Requests-###
        self.request = ""
        self.response = None
        self.print_debug = print_debug
        if parallel_process:
            self.process_type = "parallel"
        else:
            self.process_type = "single"
        self.run_name=run_name
        self.failure_fname="Failure_"+self.run_name+"_"+self.process_type
        self.success_fname="Success_"+self.run_name+"_"+self.process_type
        ###-Managing Api Keys-###
        self.current_key = ""
        self.user_id_key = ""
        self.master_keys = []
        self.all_keys = []
        self.expired_keys = []
        self.dev_key = dev_key
        self.crawler_name = crawler_name
        self.num_keys = 0
        ###-Api Key Locations-###
        self.expired_key_loc = "Utils/API_Expired_Keys.txt"
        self.master_key_loc = "Utils/API_Master_Keys.txt"
        ###-Initalize Functions-###
        self.initalize_api_keys()


    def load_api_keys(self): 
        self.load_exp_keys()
        self.all_keys = [key for key in self.master_keys if not key in self.expired_keys]
        random.shuffle(self.all_keys) #Shuffling keys to make sure all get used, and thus don't get deleted. 
        self.num_keys = len(self.all_keys)
        try: 
            self.current_key = self.all_keys[0]
        except:
            print("\nLooks like all keys are expired... Going to sleep till 12:05am PT to refresh keys\n")
            dt = datetime.now() - timedelta(hours=2, minutes=5) #Cac PT based on CT
            minutes_left = math.ceil((((24 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second))/60)
            wait_list = list(range(0, minutes_left))
            for _ in tqdm(wait_list, desc="Time Remaining", ascii=True, file=sys.stdout):
                time.sleep(60) #sleeping for 1 minute, X times till 12:05am PT
            print("\n\nBack Online! Reloading Keys")
            self.initalize_api_keys()

    def load_exp_keys(self):
        with open(self.expired_key_loc) as f:
            lines = [line.rstrip() for line in f]
        self.expired_keys = list(set(lines))

    def load_user_key(self, user_id_key):
        self.user_id_key = user_id_key
        if not self.user_id_key: #Checking if string is empty
            raise Exception("API Error! You've turned off Developement Keys, but didn't not provide a user id (so we can pull their key)")
        if self.crawler_name == 'admin':
            key = get_admin_api_key(self.user_id_key)
        elif self.crawler_name == 'app':
            key = get_app_api_key(self.user_id_key)
        if key != None: #If we can't get the current user key, use dev key
            self.current_key = key  

    def initalize_api_keys(self): 
        with open(self.master_key_loc) as f:
            lines = [line.rstrip() for line in f]
        self.all_keys = lines
        self.master_keys = lines
        #Resetting failed keys
        open(self.expired_key_loc, 'w').close() 
        self.load_api_keys()

    def remove_current_key(self):
        #Removing a dev key
        if self.current_key in self.master_keys: 
            with open(self.expired_key_loc, "a") as f1:
                f1.write(self.current_key +'\n')
            self.all_keys.remove(self.current_key)
            self.current_key = self.all_keys[0]
        #Removing a user key
        else: 
            with open(self.expired_key_loc, "a") as f1:
                f1.write(self.current_key +'\n')
            self.load_api_keys()


    def get_response(self, id, request_type, params=None):
        error_counter=0
        #Resetting keys, won't run if using a user's key
        if self.current_key in self.master_keys: self.load_api_keys()
        else: self.load_exp_keys()

        while True:
            #Making sure we are using most up to date key
            if params: params['key'] = self.current_key
            #Making Request
            try:
                response = requests.get(self.request, params=params)
                response = response.json()
                return self.handle_response(response, id, request_type)
            #Error Handling
            except:
                #Might be a timeout on the response, wait and try again
                error_counter+=1
                time.sleep(2)
                if error_counter > 16: raise Exception("Trouble getting response: {}".format(self.request))
        

    def handle_response(self, response, id, request_type):
        #Error Handling
        if 'error' in response.keys():
            #Api Errors
            if response['error']['code']:
                code = response['error']['code']
                message = response['error']['message']
               
                #400 Bad Request
                if code == 400:
                    try: 
                        if "usageLimits" in response['error']['errors'][0]['domain']:
                            self.remove_current_key()
                    except:
                        print("Bad request, trying again")

                #403 Forbidden
                elif code == 403:
                    #API key is not setup properly
                    if "accessNotConfigured" in message or "Access Not Configured" in message:
                        # print("API {} access key is not configured...".format(self.current_key))
                        self.remove_current_key()
                    #API Key is exceeded it's daily limit
                    elif "dailyLimitExceeded" in message or "quotaExceeded" in message or "quota" in message:
                        # print("API {} exceeded it's daily limit of requests".format(self.current_key))
                        if self.user_id_key != "" and self.current_key not in self.master_keys:
                            self.user_id_key=""
                            # print(" User key hit it's limit. Switching over to Developer Keys")
                        self.remove_current_key()
                    #Comments disabled
                    elif "disabled comments" in message:
                        return self.failed_content(id, request_type, "Disabled comments")

                #404 not found error (Might be an issue with video Id)
                elif code == 404:
                    return self.failed_content(id, request_type, "Video not found")
                
                #10060 Timeout
                elif code == 10060:
                    print("No response from API server... Timeout")

        #Unabliable Content (Videos can have no comments)
        if len(response['items']) == 0 and request_type != "get_youtube_comments":
            return self.failed_content(id, request_type, "Content is marked Unavailable")

        #Response passes checks, GOOD!
        else: 
            self.response = response
            self.success_content(id, request_type)
            return response
  
    def failed_content(self, id, request_type, reason):
        if self.process_type == 'parallel': pid = "_id" + str(mp.current_process().pid)
        else: pid = ""
        data = {
            "Crawler": self.crawler_name,
            "Request_type": request_type,
            "Processing_type": self.process_type,
            "Content_id": id,
            "Failure": reason,
            "Cost": self.api_cost(self.request),
            "Request_url":self.request
        }
        save_csv([data], self.failure_fname + pid)
        return None

    def success_content(self, id, request_type):
        if self.process_type == 'parallel': pid = "_id" + str(mp.current_process().pid)
        else: pid = ""
        data = {
            "Crawler": self.crawler_name,
            "Request_type": request_type,
            "Processing_type": self.process_type,
            "Content_id": id,
            "Cost": self.api_cost(self.request)
        }
        save_csv([data], self.success_fname + pid)

    def api_cost(self, url):
        cost = 0
        if 'videos' in url:
            if 'snippet' and 'contentDetails' in url: cost = 5
            elif 'snippet' in url: cost = 3
            elif 'contentDetails' in url: cost = 3
            elif 'statistics' in url: cost = 3
        elif 'search' in url: cost = 100
        elif 'playlistItems' in url:
            if 'snippet' and 'contentDetails' in url: cost = 5
            elif 'snippet' in url: cost = 3
            elif 'contentDetails' in url: cost = 3
        elif 'channels' in url:
            if 'snippet' and 'statistics' in url: cost = 5
            elif 'snippet' in url: cost = 3
            elif 'statistics' in url: cost = 3
        elif 'commentThreads' in url: 
            if 'snippet' and 'replies' in url: cost = 5
            elif 'snippet' in url: cost = 3
            elif 'replies' in url: cost = 3 
        elif url == '':
            cost = 0
        else:
            raise Exception("There is an API request we're not processing correctly...")
        return cost


    def handle_exceptions(self, response, id=None, print_debug=False):
        raise Exception("Something broke, but we aren't catching it...")


class Time():
    def __init__(self):
        self.start = time.time()
        self.end = None
        self.runtime_mins = None
        self.runtime_secs = None

    def finished(self):
        self.end = time.time()
        self.runtime_mins, self.runtime_secs = divmod(self.end - self.start, 60)
        self.runtime_mins = round(self.runtime_mins, 0)
        self.runtime_secs = round(self.runtime_secs, 0)
        print("Time to complete: {} Mins {} Secs".format(self.runtime_mins, self.runtime_secs))

class Video():
    def __init__(self, video_id, api_key, sid="", bad_key=False):
        self.video_id = video_id
        self.api_key = api_key
        self.sid = sid

class Smlink():
    def __init__(self, smlink, channel_id):
        self.smlink = smlink
        self.channel_id = channel_id
