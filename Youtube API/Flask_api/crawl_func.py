from Crawlers.instant_crawler import InstantCrawler
from Utils.object_classes import Time
from Utils.functions import save_csv, clean_mp_files

from pathos.multiprocessing import ProcessPool as Pool
from datetime import datetime
import shutil
import os


def crawl_fuc(channel_ids, keyword_ids,  video_ids , get_videos=True,  get_videos_daily=False,  get_channels=True,  get_channels_daily=False,
            get_related_videos=False,  get_comments=False,  run_name="",  parallel_process=True): 
    #Initalizing
    if not run_name: run_name= "{}".format(datetime.today().strftime('%m-%d-%Y--%H-%M')) 
    if parallel_process:
       process_type = "parallel"
    else:
        process_type = "single"
    #Stats for multiprocessing!!
    num_processes = 8
    #Data initliztion
    if channel_ids is None:
        channel_ids=[]
    else: channel_ids = list(set(channel_ids))
    if keyword_ids is None:
        keyword_ids= []
    else: keyword_ids = list(set(keyword_ids))
    if video_ids is None:
        video_ids= []
    else: video_ids = list(set(video_ids))
    #Crawling
    timer = Time()
    crawler = InstantCrawler(num_processes, run_name, dev_keys=True, get_videos=get_videos, get_videos_daily=get_videos_daily, get_channels=get_channels, get_channels_daily=get_channels_daily, get_related_videos=get_related_videos, get_comments=get_comments, parallel_process=parallel_process, export_to_csv=True)
    crawler.instant_crawl(channel_ids, keyword_ids, video_ids)
    timer.finished()

    #Showing crawler times
    show_stats(run_name, {"Instant Crawler":timer}, process_type)

    #Cleaning multiprocessing files
    clean_mp_files("Export")
    #Moving the files to be uploaded
    # for root, _, files in os.walk("Export", topdown=False):
    #     for file in files: 
    #         if file.endswith(".csv"):
    #             shutil.copy(os.path.join(root,file), "G:\\Shared drives\\COSMOS\\YouTubeTracker\\Instant_Crawler_Export")
    # #Cleaining validation files
    # clean_mp_files("Validation_data")


    #Creating pool and closing (closes and nested pools)
    if parallel_process:
        process_pool = Pool(num_processes)
        print("\nClosing pool")
        process_pool.close()
        # process_pool.join()
        # process_pool.clear()
        print("Pool closed. Finished!")

    return "Crawling Done"

def show_stats(run_name, timers, process_type):
    print("\nStats for {} running in {}".format(run_name, process_type))
    for key, value in timers.items():
        print("-{}: {} Mins {} Secs".format(key, value.runtime_mins, value.runtime_secs))