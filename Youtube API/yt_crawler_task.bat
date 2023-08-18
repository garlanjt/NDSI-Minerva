call conda activate instant_crawler
REM call E:
cd C:\Users\CosmosAdmin\Documents\GitHub\YTInstantCrawler

call git pull origin api_crawler

set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%

call python yt_crawler.py | tee Validation_data\Log_%SUBFILENAME%.log

