from pathos.multiprocessing import ProcessPool as Pool
from datetime import datetime
from tqdm import tqdm
import sys

from Crawlers.base_crawler import Crawler
from Data_collection.channel_functions import channels, channels_daily, get_video_list_ch
from Data_collection.video_functions import videos, videos_daily, get_related_videos, get_video_list_kw
from Utils.functions import get_playlist_id

class InstantCrawler(Crawler):
    def __init__(self, num_processes, run_name, dev_keys=False, parallel_process=False, export_to_csv=True, get_videos=True, get_videos_daily=False, get_channels=True, get_channels_daily=False, get_related_videos=False, get_comments=False):
        self.crawler_name = 'instant'
        Crawler.__init__(self, num_processes, self.crawler_name, run_name, dev_keys, parallel_process, export_to_csv)
        if parallel_process:
            self.process_type = "_parallel"
        else:
            self.process_type = "_single"
        self.get_videos = get_videos
        self.get_videos_daily = get_videos_daily
        self.get_channels = get_channels
        self.get_channels_daily = get_channels_daily
        self.get_related_videos = get_related_videos
        self.get_comments = get_comments
        self.only_channels = False if self.get_videos or self.get_videos_daily or \
            self.get_related_videos or self.get_comments else True

        
    def instant_crawl(self, channel_ids, keyword_ids, video_ids):
        
        ###-RUN-###
        def run():
            print("\n\n----- Running the YT Instant Crawler -----")
            if self.parallel_process: parallel_process()
            else: single_process()
            

        def single_process():
            # Content type: Channel
            print("\nProcessing {} Channel Content Type...".format(len(channel_ids)))
            if len(channel_ids) > 0:
                for channel_id in tqdm(channel_ids, desc="Channels", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    videos = process_channel(channel_id)
                    videos = list(set(videos))
                    for video in tqdm(videos, desc="Videos", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}): 
                        process_video(video)
                    if self.get_comments and videos: self.crawl_comments(videos, self.num_processes)
            # Content type: Keyword
            print("\nProcessing {} Keyword Content Type...".format(len(keyword_ids)))
            if len(keyword_ids) > 0:
                for keyword in tqdm(keyword_ids, desc="Keywords", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    videos = process_keyword_videos(keyword)
                    videos = list(set(videos))
                    for video in tqdm(videos, desc="Videos", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}): 
                        process_video(video)
                    if self.get_comments and videos: self.crawl_comments(videos, self.num_processes)
            # Content type: Video
            print("\nProcessing {} Video Content Type...".format(len(video_ids)))
            if len(video_ids) > 0:
                for video_id in tqdm(video_ids, desc="Videos", ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    process_video(video_id)
                if self.get_comments and video_ids: self.crawl_comments(video_ids, self.num_processes)
                   
               
        def parallel_process():            
            process_pool = Pool(self.num_processes)
            
            #Parallel Processing
            #Channels
            print("\nProcessing {} Channel Content Type...".format(len(channel_ids)))
            if len(channel_ids) > 0: 
                channel_videoList =[]
                for video in tqdm(process_pool.uimap(process_channel, channel_ids), desc="Channels", total=len(channel_ids), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    channel_videoList += video
                channel_videoList = list(set(channel_videoList))
                for _ in tqdm(process_pool.uimap(process_video, channel_videoList), desc="Videos", total=len(channel_videoList), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    pass
                if self.get_comments and channel_videoList: self.crawl_comments(channel_videoList, self.num_processes, process_pool)
            
            #Keywords
            print("\nProcessing {} Keyword Content Type...".format(len(keyword_ids)))
            if len(keyword_ids) > 0: 
                keyword_videoList = []
                for video in tqdm(process_pool.uimap(process_keyword_videos, keyword_ids), desc="Keywords", total=len(keyword_ids), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    keyword_videoList += video
                keyword_videoList = list(set(keyword_videoList))
                for _ in tqdm(process_pool.uimap(process_video, keyword_videoList), desc="Videos", total=len(keyword_videoList), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    pass
                if self.get_comments and keyword_videoList: self.crawl_comments(keyword_videoList, self.num_processes, process_pool)

            #Video
            print("\nProcessing {} Video Content Type...".format(len(video_ids)))
            if len(video_ids) > 0: 
                for _ in tqdm(process_pool.uimap(process_video, video_ids), desc="Video", total=len(video_ids), ascii=True,  file=sys.stdout, postfix={"Start_Time:":datetime.now().strftime("%H:%M:%S")}):
                    pass
                if self.get_comments and video_ids: self.crawl_comments(video_ids, self.num_processes, process_pool)

            #Closeing Pool
            # process_pool.close()
            #process_pool.join()
            # process_pool.clear()

        def process_channel(channel_id): 
            if self.get_channels: channels(channel_id, self.api_key, self.export_to_csv, self.process_type)
            if self.get_channels_daily: channels_daily(channel_id, self.api_key, self.export_to_csv, self.process_type)
            if not self.only_channels: 
                playlist_id = get_playlist_id(channel_id)
                videos_list = get_video_list_ch('', playlist_id, self.api_key)
                return videos_list
            return []
                
        def process_video(video_id):
            if self.get_videos: videos(video_id, self.api_key, self.export_to_csv, self.process_type)
            if self.get_videos_daily: videos_daily(video_id, self.api_key, self.export_to_csv, self.process_type)
            if self.get_related_videos: get_related_videos(video_id, self.api_key, self.export_to_csv, self.process_type)

        def process_keyword_videos(keyword):
            video_ids = get_video_list_kw(keyword, self.api_key)
            return video_ids

        ###-RUN-###
        run()


