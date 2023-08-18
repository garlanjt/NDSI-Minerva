# EasyInstantCrawler

This repo is a simplified version of the [YTInstantCrawler](https://github.com/COSMOS-UALR/YTInstantCrawler/) aiming to take in a simple text based input and output a JSON or XLSX file in response.

# Set Up:

1. Install the script's requirements by running requirements.bat (you may need to [install conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)).
2. Add [API keys](https://developers.google.com/youtube/v3/getting-started) to 'Utils/API_Master_Keys.txt'. Each API Key should be a new line in the .txt file. E.G 
```
    firstkey
    secondkey
    thirdkey
```
3. Add the list of video_ids to crawl to 'Input/video_ids.txt'

4. Edit 'yt_crawler.py' to change the crawler task parameters
```python 
    channel_ids = None          # list of channel ids
    keyword_ids=None        # 
    get_videos= True        # Boolean to crawl videos or not 
    get_channels=True           # Boolean to crawl channel information
    get_related_videos = False      # Boolean to get related videos from crawled videos
    get_comments = True             # Boolean to get comments on videos
    run_name = 'Test'           
    parallel_process = False         # Boolean to run parallel or single process job
```

5. Execute yt_crawler.py to start the crawler
```
    python yt_crawler.py
```


# Result
The crawler outputs the result to 'Export' folder as a .csv file


&copy; [COSMOS](https://cosmos.ualr.edu/) 2022
