from Flask_api.crawl_func import crawl_fuc
from Utils.functions import clean_mp_files

import shutil
import os

def main():
    crawl_fuc(
        channel_ids=[],
        keyword_ids=[], 
        video_ids=['vjNV2e11Klg' ], 
        get_channels=True,
        get_videos=True,
        get_videos_daily=True,
        get_channels_daily=True,
        get_related_videos=True,
        get_comments=True,
        run_name="Test123",
        parallel_process=True
)

 #Cleaning multiprocessing files
    # clean_mp_files("Export")
    # #Moving the files to be uploaded
    # for root, _, files in os.walk("Export", topdown=False):
    #     for file in files: 
    #         if file.endswith(".csv"):
    #             shutil.copy(os.path.join(root,file), "G:\\Shared drives\\COSMOS\\YT_Crawler_Export")
    # #Cleaining validation files
    # clean_mp_files("Validation_data")


if __name__ == "__main__":
    main()