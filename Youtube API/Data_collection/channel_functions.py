import multiprocessing as mp
import datetime

from Utils.functions import get_language, normalize_metadate, commit_to_db, get_connection, save_csv, get_sentiment_score, get_toxicity_score

def channels(channel_id, yt_api, export_to_csv=True, process_type=None):                          
    # get api request url
    params = {'id': channel_id, 'key': yt_api.current_key, 'part': 'snippet'}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/channels'                    

    # get response from api                           
    data = yt_api.get_response(channel_id, "channels", params)
    if data is None: 
        #API contains no items sometimes
        return 

    # get channel info
    channel = {}
    channel["channel_id"] = data['items'][0]['id']                                                 # get channel id
    channel['channel_title'] = data['items'][0]['snippet']['title']                                # get the name displayed in channel
    # channel['title_sentiment']  = get_sentiment_score(channel['channel_title'])
    # channel['title_toxicity']  = get_toxicity_score(channel['channel_title'])
    channel['thumbnails_medium_url'] = data['items'][0]['snippet']['thumbnails']['medium']['url']  # get medium thumbnail url
    channel['description'] = data['items'][0]['snippet']['description']				            # get the description of channel
    # channel['description_sentiment']  = get_sentiment_score(channel['description'])
    # channel['description_toxicity']  = get_toxicity_score(channel['description'])
    channel['joined_date'] = normalize_metadate(data['items'][0]['snippet']['publishedAt'])        # get joined date
    channel['language'] = get_language(channel['description'])
    channel['crawled_time'] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())          # get added to db time
    # playlist_id = get_playlist_id(channel_id)                                           # get playlist id from channel id
    try:
        channel['location'] = data['items'][0]['snippet']['country']                               # get location/country
    except:
        channel['location'] = 'N/A'

    #Adding to db
    if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
    else: pid = ""
    if export_to_csv:
        save_csv([channel], yt_api.run_name + "_Channels_id"+ pid  , "Export")


def channels_daily(channel_id, yt_api, export_to_csv=True, process_type=None):                         
    # get api request url
    params = {'id': channel_id, 'key': yt_api.current_key, 'part': 'statistics'}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/channels'
                        
    # get response from api
    data = yt_api.get_response(channel_id, "channels_daily", params)
    if data is None: 
        #API contains no items sometimes
        return 

    channel = {}
    channel['channel_id'] = data['items'][0]['id']                                                             # get channel id
    try: channel['total_views'] = data['items'][0]['statistics']['viewCount']                                   # get the number of views
    except: channel['total_views'] = 0
    
    try: channel['total_subscribers'] = data['items'][0]['statistics']['subscriberCount']                       # get the number of subscribers
    except: channel['total_subscribers'] = 0
    
    try: channel['total_videos'] = data['items'][0]['statistics']['videoCount']                                 # get the number of videos
    except: channel['total_videos'] = 0
        
    channel['crawled_time'] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())                        # get extracted date

    
    #Adding to DB
    if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
    else: pid = ""
    if export_to_csv:
        save_csv([channel], yt_api.run_name + "_Channels_stats_id" + pid, "Export")
         

def get_video_list_ch(nextToken, playlist_id, yt_api):
    # get the list of videos
    params = {'playlistId': playlist_id, 'key': yt_api.current_key, 'part': 'snippet', 'maxResults':50}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/playlistItems'
    dataset = []
    videoIdList_ch = []

    #Getting response from api (Multi-Page)
    while True:
        params['pageToken']= nextToken
        response = yt_api.get_response(playlist_id, "get_video_list_ch", params)
        if response is not None: 
            dataset.append(response)
            if 'nextPageToken' in yt_api.response.keys():
                nextToken = yt_api.response['nextPageToken']
            else: break
        else: break
    #Processing data
    for data in dataset: 
        items = data['items']
        for item in items:
            video_id = item['snippet']['resourceId']['videoId']
            videoIdList_ch.append(video_id)
    videoIdList_ch = list(set(videoIdList_ch))
    return videoIdList_ch
    



