#%%
import requests
import os,sys
from ratelimiter import RateLimiter
import json
import time
import tweepy



def auth2():
    #need to run this before hand where BEARER_TOKEN is collected from one of the hidden files
    #export BEARER_TOKEN='xxxxxxxxxxxxxxxxxxx'
    return os.environ.get("BEARER_TOKEN")

def auth1():
    keys = []
    with open("consumer.keys") as fp:
        lines = fp.readlines()
    for line in lines:
        keys.append(line.strip())
    print(keys)

    #CONSUMER_KEY = "<consumer key>"
    #CONSUMER_SECRET = "<consumer secret>"
    CONSUMER_KEY = keys[0]
    CONSUMER_SECRET = keys[1]
    keys = []
    with open("access_token.keys") as fp:
        lines = fp.readlines()
    for line in lines:
        keys.append(line.strip())
    print(keys)
    #OAUTH_TOKEN = "<application key>"
    #OAUTH_TOKEN_SECRET = "<application secret"
    OAUTH_TOKEN = keys[0]
    OAUTH_TOKEN_SECRET = keys[1]
    #twitter = Twython(CONSUMER_KEY, CONSUMER_SECRET,
    #    OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    auth = tweepy.OAuth1UserHandler(CONSUMER_KEY, CONSUMER_SECRET,
                    OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth,wait_on_rate_limit=True)
    return api

    

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    """
    returns a json with two fields a list of tweet objects and meta data
    json_response["data"] is a list of tweet objects
    in each tweet object if x['type']== 'replied_to' x["referenced_tweets"][0]["id"] will give you the id that the tweet is responding to
    
    """

    response = requests.request("GET", url, headers=headers)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def create_conversation_search_url(conversation_id,max_results=None,next_token=None,recent_only=False):
    #query = "from:twitterdev -is:retweet"
    query="conversation_id:"+str(conversation_id)
    #query = "from:tagesschau -is:retweet"
    #tweet_fields = "tweet.fields=author_id"
    #in the "referenced_tweets"
    """

    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld

    """
    #does not return root tweet however. So I think that the name of the game will be to scrape the user tweets then for each user tweet
    # scrape the conversation. it could be I need to use one of these fields to iterate very large conversations. in the morning I should look for
    # large conversations and then make sure it all works. Should also try and use the new api to get timelines and see how that goes.
    # [query,start_time,end_time,since_id,until_id,
    # max_results,next_token,expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields]"}]  
    """
    max_results \in [10,500] with academic search API
    """
    if max_results:
        if max_results>=10 and max_results<=500:
            max_results_str = "&max_results="+str(max_results)
        else: 
            print("Max Results needs to be in [10,500]. Not returning a url.")
            return None
    else:
        max_results_str = ""

    if next_token:
        next_token_str = "&next_token="+str(next_token)
    else:
        next_token_str = ""

    tweet_fields="tweet.fields=in_reply_to_user_id,author_id,created_at,conversation_id,public_metrics,referenced_tweets"
    
    if recent_only:
        url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}".format(
                query, tweet_fields 
                )+ max_results_str +next_token_str +"&expansions=author_id"
    else:
        url = "https://api.twitter.com/2/tweets/search/all?query={}&{}".format(
            query, tweet_fields 
            )+ max_results_str +next_token_str +"&expansions=author_id"
    print(url)
    return url



def create_user_url(ids=["TwitterDev"]):
    # Specify the ids that you want to lookup below
    # You can enter up to 100 comma-separated values.
    username_str = "ids="
    for user in ids:
        username_str=username_str+user+","
    username_str = username_str[:-1]

    #username_str = "usernames=TwitterDev,TwitterAPI"
    user_fields = "user.fields=description,created_at,withheld,verified,protected"
    # User fields are adjustable, options include:
    # created_at, description, entities, id, location, name,
    # pinned_tweet_id, profile_image_url, protected,
    # public_metrics, url, username, verified, and withheld
    #curl https://api.twitter.com/2/users/2244994945 -H "Authorization: Bearer $BEARER_TOKEN"

    url = "https://api.twitter.com/2/users/by?{}&{}".format(username_str, user_fields)
    return url


def tweet_lookup_url(tweet_id,max_results=None,next_token=None):
    #query = "from:twitterdev -is:retweet"
    query="?ids="+str(tweet_id)
    #query = "from:tagesschau -is:retweet"
    """

    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld

    """
    #does not return root tweet however. So I think that the name of the game will be to scrape the user tweets then for each user tweet
    # scrape the conversation. it could be I need to use one of these fields to iterate very large conversations. in the morning I should look for
    # large conversations and then make sure it all works. Should also try and use the new api to get timelines and see how that goes.
    # [query,start_time,end_time,since_id,until_id,
    # max_results,next_token,expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields]"}]  
    """
    max_results \in [10,500] with search API
    """
    if max_results:
        if max_results>=10 and max_results<=500:
            max_results_str = "&max_results="+str(max_results)
        else: 
            print("Max Results needs to be in [10,500]. Not returning a url.")
            return None
    else:
        max_results_str = ""

    if next_token:
        next_token_str = "&next_token="+str(next_token)
    else:
        next_token_str = ""

    tweet_fields="tweet.fields=in_reply_to_user_id,author_id,created_at,conversation_id,public_metrics,referenced_tweets"
    #tweet_fields="tweet.fields="
    url = "https://api.twitter.com/2/tweets{}&{}".format(
        query, tweet_fields 
    )+ max_results_str +next_token_str +"&expansions=author_id"
    return url



def collect_conversation_list(conversation_id,max_results=500):
    """
    Given a conversation id this returns a list of each tweet in the conversation with the exception of the root node
    which is not returned.
    """
    conversation = []
    bearer_token = auth()
    headers = create_headers(bearer_token)
    url = create_conversation_search_url(conversation_id=conversation_id,max_results=max_results) 
    json_response = connect_to_endpoint(url, headers) 
    print(json_response)
    if json_response["meta"]["result_count"]>0:
        conversation +=json_response["data"] 
        while "next_token" in json_response["meta"]: 
            next_token=json_response["meta"]["next_token"] 
            url = create_conversation_search_url(conversation_id=conversation_id,next_token=next_token,max_results=max_results)
            json_response = connect_to_endpoint(url, headers) 
            time.sleep(1)
            conversation +=json_response["data"] 
    return conversation

def collect_root_node(conversation_id,translate_to_rt_format = False):
    bearer_token = auth()
    headers = create_headers(bearer_token)
    lookup_url=tweet_lookup_url(conversation_id)   
    json_response  = connect_to_endpoint(lookup_url, headers)  
    node = json_response["data"][0] 
    if translate_to_rt_format:
        root = {"_id":node["id"],
                "conversation_id":node["conversation_id"],
                "created_at":node["created_at"],
                "screen_name":json_response["includes"]["users"][0]["name"],
                "user_name":json_response["includes"]["users"][0]["username"],
                "user_twitter_id":node["author_id"],
                "full_text":node["text"],
                'num_likes': node["public_metrics"]["like_count"],
                'num_retweets': node["public_metrics"]["retweet_count"] ,
                'num_replies':node["public_metrics"]["reply_count"],
                "num_quotes":node["public_metrics"]["quote_count"],
                "replies":[]
                }
    else:
        root = node
        root["screen_name"] = json_response["includes"]["users"][0]["name"],
        root["user_name"] = json_response["includes"]["users"][0]["username"]
    return root


def conversation_list_to_reply_tree(node,conversation_list):
    #for testing tree_id= 1000000791241986049
    c_node = {"_id":node["id"],
                "conversation_id":node["conversation_id"],
                "created_at":node["created_at"],
                #"screen_name":node[""], #<TODO> 
                #"user_name":node[""],
                "user_twitter_id":node["author_id"],
                "full_text":node["text"],
                'num_likes': node["public_metrics"]["like_count"],
                'num_retweets': node["public_metrics"]["retweet_count"] ,
                'num_replies':node["public_metrics"]["reply_count"],
                "num_quotes":node["public_metrics"]["quote_count"],
                "replies":[]
               }
    for tweet in conversation_list:
        if 'replied_to'==tweet["referenced_tweets"][0]["type"]: #not sure about the 0 here, or how more tweet ref could occur
            if tweet["referenced_tweets"][0]["id"]==node["id"]:
                c_node["replies"].append(conversation_list_to_reply_tree(tweet,conversation_list))

    return c_node

        
@RateLimiter(max_calls=300, period=960)
def scrape_conversation(conversation_id):
    """
    This uses the academic api to reconstruct a reply tree from a conversation ID. 
    Currently only some trees are returned as it seems not all tweets have conversation ids
    especially going back in time. In this case it is returning an empty conversation. 
    """
    conversation_list = collect_conversation_list(conversation_id,max_results=500)   
    if len(conversation_list)>0:
        root_node = collect_root_node(conversation_id)
        tree = conversation_list_to_reply_tree(root_node,conversation_list)
        return tree
    else: 
        return {} 

def scrape_users(usernames = []):
    url = create_user_url(usernames)
    bearer_token = auth()
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url,headers)
    print(json.dumps(json_response, indent=4, sort_keys=True))


def get_tweet_ids(filename="ids.txt"):
    ids = []
    with open(filename) as fp:
        lines = fp.readlines()
    for line in lines:
        ids.append(line.strip())
    return ids
def main():
    #connect to tweepy client
    twAPI = auth1()
    ids = get_tweet_ids(filename="ids.txt")
    #ids = ['1409935014725177344', '1409931481552543749', '1441054496931541004']
    for _id in ids:
        tweet = twAPI.get_status(_id, tweet_mode="extended")
        print(tweet._json)

if __name__ == "__main__":
    main()
# %%
