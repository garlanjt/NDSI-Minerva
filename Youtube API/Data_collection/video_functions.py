import multiprocessing as mp
import datetime
import time

from Utils.functions import normalize_metadate, get_category, get_sentiment_score, get_toxicity_score, commit_to_db, get_connection, save_csv


def videos(video_id, yt_api, export_to_csv=True, process_type=None):
    # get api request url
    params = {'id': video_id, 'key':yt_api.current_key, 'part':'snippet,contentDetails'}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/videos'

    # get response from api
    data = yt_api.get_response(video_id, "videos", params)
    if data is None:
        return 

    video = {}
    video['video_id'] = data['items'][0]['id']                                                       # get video id    
    video['video_title'] = data['items'][0]['snippet']['title']##.encode('ascii', 'replace')           # get the title of the video
    # video['title_sentiment']  = get_sentiment_score(video['video_title'])
    # video['title_toxicity']  = get_toxicity_score(video['video_title'])
    video['categoryId'] = data['items'][0]['snippet']['categoryId']                                  # get category ID
    video['category'] = get_category(video['categoryId'])                                                     # get category
    try:
        video['tags'] = ",".join(item['snippet']['tags'])                                # get the tags
    except:
        video['tags'] = 'Not found'
    video['published_date'] = normalize_metadate(data['items'][0]['snippet']['publishedAt'])         # get date published
    video['thumbnails_medium_url'] = data['items'][0]['snippet']['thumbnails']['medium']['url']      # get medium thumbnail url
    video['description'] = data['items'][0]['snippet']['description']##.encode('ascii', 'replace')     # get description
    # video['description_sentiment']  = get_sentiment_score(video['description'])
    # video['description_toxicity']  = get_toxicity_score(video['description'])
    video['channel_id'] = data['items'][0]['snippet']['channelId']                                   # get channel ID
    video['crawled_time'] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())              # get added to db time
    # duration = data['items'][0]['contentDetails']["duration"]                               # get duration
    # definition = data['items'][0]['contentDetails']["definition"]                           # get definition
    # caption = data['items'][0]['contentDetails']["caption"]                                 # get caption
    # licensedContent = data['items'][0]['contentDetails']["licensedContent"]                 # get licensedContent

    #Adding to db
    if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
    else: pid = ""
    if export_to_csv:
        save_csv([video], yt_api.run_name + "_Videos_id" + pid, "Export")
                               
def videos_daily(video_id, yt_api, export_to_csv=True, process_type=None):                           
        # get api request url
        params = {'id': video_id, 'key':yt_api.current_key, 'part':'statistics'}
        yt_api.request = 'https://www.googleapis.com/youtube/v3/videos'

        # get response from api
        data = yt_api.get_response(video_id, "videos_daily", params)
        if data is None:
            return 
        
        video = {}
        video['video_id'] = data['items'][0]['id']                                                   # get video id
        try:    
            video['total_views'] = data['items'][0]['statistics']['viewCount']                       # get the number of views
        except:
            video['total_views'] = 0
        try:
            video['total_likes'] = data['items'][0]['statistics']['likeCount']                       # get the number of likes
        except:
            video['total_likes'] = 0
        try:
            video['total_dislikes'] = data['items'][0]['statistics']['dislikeCount']                 # get the number of dislikes
        except:
            video['total_dislikes'] = 0
        try:
            video['total_comments'] = data['items'][0]['statistics']['commentCount']                 # get the total number of comments
        except:
            video['total_comments'] = 0
        video['crawled_time'] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())            # get extracted date

        if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
        else: pid = ""
        if export_to_csv:
            save_csv([video], yt_api.run_name + "_Videos_stats_id" + pid, "Export")
           

def get_related_videos(videoId, yt_api, export_to_csv=True, process_type=None):
    params = {'part': 'snippet', 'relatedToVideoId': videoId, 'type':'video', 'key':yt_api.current_key, 'maxResults':50}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/search'

    # get response from api
    data = yt_api.get_response(videoId, 'get_related_videos', params)
    if data is None:
        return 

    related_video_list = []
    items = data['items']
    for item in items:
        video = {}
        video['video_id'] = item['id']['videoId']                                            # get video id
        video['title'] = item['snippet']['title']##.encode('ascii', 'replace')                 # get title
        # video['title_sentiment']  = get_sentiment_score(video['title'])
        # video['title_toxicity']  = get_toxicity_score(video['title'])
        video['thumbnails_medium_url'] = item['snippet']['thumbnails']['medium']['url']      # thumbnails medium url
        video['published_date'] = normalize_metadate(item['snippet']['publishedAt'])         # get joined date
        video['channelId'] = item['snippet']['channelId']                                    # get channelId
        video['channel_title'] = item['snippet']['channelTitle']##.encode('ascii', 'replace')  # get channel title
        video['parent_video'] = videoId                                                      # get parent_id
        video['crawled_time'] = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())  # get added to db time
        related_video_list.append(video)

        if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
        else: pid = ""
        if export_to_csv:
            save_csv(related_video_list, yt_api.run_name + "_Related_videos_id" + pid, "Export")



def get_youtube_comments(video_id, nextToken, yt_api):
    #Getting the API Data
    dataset = []

    #Checking for ran out api keys
    if yt_api.dev_key and yt_api.num_keys == 0:
        raise Exception("Looks like all the keys are used up! Try again tomorrow...")
    params = {'key': yt_api.current_key, 'textFormat': 'plainText', 'part': 'snippet,replies', 'videoId': video_id, 'maxResults':50}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/commentThreads'

    while True:       
        #Sending API Request (Multi-Page)
        params['pageToken'] = nextToken
        data = yt_api.get_response(video_id, "get_youtube_comments", params)
        if data is None: #Video was unavailable 
            return [{"Failed":video_id}]
        dataset.append(data)

        if 'nextPageToken' in yt_api.response:
            nextToken = yt_api.response['nextPageToken']
        else: break
        
    return dataset
            
    
def process_youtube_comments(data, yt_api, export_to_csv=True, process_type=None):
    #Processing Comment Data
    item = data 
    comment = {}
    comment['comment_id'] = item['id']
    comment['commenter_name'] = item['snippet']['topLevelComment']['snippet']['authorDisplayName']#.encode('ascii', 'replace')
    try:
        comment['commenter_id'] = item['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
    except: #Looks like commenter names can be removed, but not their comments. 
        comment['commenter_id']=None
    comment['comment_displayed'] = item['snippet']['topLevelComment']['snippet']['textDisplay']#.encode('ascii', 'replace')
    comment['comment_original'] = item['snippet']['topLevelComment']['snippet']['textOriginal']#.encode('ascii', 'replace')
    comment['likes'] = item['snippet']['topLevelComment']['snippet']['likeCount']
    comment['total_replies'] = item['snippet']['totalReplyCount']
    comment['published_date'] = item['snippet']['topLevelComment']['snippet']['publishedAt']
    comment['updated_date'] = item['snippet']['topLevelComment']['snippet']['updatedAt']
    comment['reply_to'] = None
    comment['video_id'] = item['snippet']['topLevelComment']['snippet']['videoId']
    # comment['sentiment'] = get_sentiment_score(comment['comment_displayed'])
    # comment['toxicity'] = get_toxicity_score(comment['comment_displayed'])
    
    #Adding to db
    if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
    else: pid = ""
    if export_to_csv:
        save_csv([comment], yt_api.run_name + "_Comments_id" + pid, "Export")

    #Gettign replies for further processing. 
    if comment['total_replies'] > 0:
        try: #Sometimes YT api shows replies, but no replies in the dataset. 
            replies = item['replies']['comments']
            return replies
        except: 
            return None
    else: 
        return None


def process_youtube_replies(reply, yt_api, export_to_csv=True, process_type=None):
    #Processing Replies Data
    reply_comment = {}
    reply_comment['comment_id'] = reply['id']
    reply_comment['commenter_name'] = reply['snippet']['authorDisplayName']#.encode('ascii', 'replace')
    reply_comment['commenter_id'] = reply['snippet']['authorChannelId']['value']
    reply_comment['comment_displayed'] = reply['snippet']['textDisplay']#.encode('ascii', 'replace')
    reply_comment['comment_original'] = reply['snippet']['textOriginal']#.encode('ascii', 'replace')
    reply_comment['likes'] = reply['snippet']['likeCount']
    reply_comment['total_replies'] = 0
    reply_comment['published_date'] = reply['snippet']['publishedAt']
    reply_comment['updated_date'] = reply['snippet']['updatedAt']
    reply_comment['reply_to'] = reply['snippet']['parentId']
    reply_comment['video_id'] = reply['snippet']['videoId']
    # reply_comment['sentiment'] = get_sentiment_score(reply_comment['comment_displayed'])
    # reply_comment['toxicity'] = get_toxicity_score(reply_comment['comment_displayed'])

    #Adding to db
    if yt_api.process_type == 'parallel': pid = str(mp.current_process().pid)
    else: pid = ""
    if export_to_csv:
        save_csv([reply_comment], yt_api.run_name + "_Comments_id" + pid, "Export")
                   

def get_video_list_kw(keyword, yt_api):
    videoIdList_kw = []
    params = {"q": keyword, "part": "snippet", "type":"video", "maxResults":50, "key": yt_api.current_key}
    yt_api.request = 'https://www.googleapis.com/youtube/v3/search'
 
    # get response from api
    data = yt_api.get_response(keyword, "get_video_list_kw", params)
    items = data['items']
    for item in items:
        try:
            video_id = item['id']['videoId']
            videoIdList_kw.append(video_id)
        except:
            raise Exception("Why does this error??")
    return videoIdList_kw
