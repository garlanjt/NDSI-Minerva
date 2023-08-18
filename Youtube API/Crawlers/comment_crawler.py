from pathos.multiprocessing import ProcessPool as Pool
from datetime import datetime
from tqdm import tqdm
import sys

from Data_collection.video_functions import process_youtube_comments, process_youtube_replies, get_youtube_comments

class CommentCrawler():
    def __init__(self, api_key, parallel_process, export_to_csv=True):
        self.crawler_name = "comments"
        self.dev_keys = api_key.dev_key
        self.parallel_process = parallel_process
        self.api_key = api_key
        self.export_to_csv = export_to_csv
        if parallel_process:
            self.process_type = "_parallel"
        else:
            self.process_type = "_single"

    def crawl_comments(self, video_ids, num_processes, process_pool=None):

        def run():
            ###-RUN-###
            if self.dev_keys: self.api_key.load_api_keys()
            if self.parallel_process: parallel_process_yt_comments(video_ids, num_processes, process_pool)
            else: single_process_yt_comments(video_ids) 
        
        def parallel_process_yt_comments(video_ids, num_processes, process_pool):
            yt_data_list = []
            replies_data_list = []
            close_at_end = False
            if process_pool is None: 
                process_pool = Pool(num_processes)
                close_at_end = True

            #Getting API requests
            for yt_data in tqdm(process_pool.uimap(parallel_process_wrapper, video_ids), desc="API Requests for Comments", total=len(video_ids), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                if 'Failed' in yt_data[0].keys(): #Failed API Requests
                    pass
                else:   
                    for data in yt_data:
                        yt_data_list += (data['items'])
            #Processing the comments
            for replies in tqdm(process_pool.uimap(process_youtube_comments_wrapper, yt_data_list), desc="Parsing Comments", total=len(yt_data_list), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                if replies != None:
                    replies_data_list += replies
            #Processing the replies
            for _ in tqdm(process_pool.uimap(process_youtube_reply_wrapper, replies_data_list), desc="Parsing Replies", total=len(replies_data_list), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                pass

            #Finished          
            # if close_at_end:
            #     process_pool.close()
            #     #process_pool.join()
            #     process_pool.clear()
                
        def process_youtube_reply_wrapper(reply):
            return process_youtube_replies(reply, self.api_key, self.export_to_csv, self.process_type)

        def process_youtube_comments_wrapper(yt_data):
            return process_youtube_comments(yt_data, self.api_key, self.export_to_csv, self.process_type)

        def parallel_process_wrapper(video):
            return get_youtube_comments(video, '', self.api_key)

        def single_process_yt_comments(video_ids):
            yt_data_list=[]
            replies_data_list = []

            #Getting data and processing
            for video in tqdm(video_ids, desc="API Requests for Comments", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                #Getting API requests
                for yt_data in get_youtube_comments(video, '', self.api_key):
                    if 'Failed' in yt_data.keys(): #Failed API Requests
                        pass
                    else:   
                        if len(yt_data['items']) != 0: 
                            yt_data_list += (yt_data['items']) 
            #Processing the comments
            for yt_data in tqdm(yt_data_list, desc="Parsing Comments", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):    
                replies = process_youtube_comments(yt_data, self.api_key, self.export_to_csv, self.process_type)
                if replies != None:
                    replies_data_list += replies 
            #Processing the replies
            for reply in tqdm(replies_data_list, desc="Parsing Replies", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):    
                process_youtube_replies(reply, self.api_key, self.export_to_csv, self.process_type)
                
            #Finished
        
        ###-RUN-###
        run()