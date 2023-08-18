
from Flask_api.crawl_func import crawl_fuc


def main():
    id_file = 'Input/hadith_video_id.txt'
    channel_ids = None
    keyword_ids=None  
    get_videos= True  
    get_channels=True
    get_related_videos = False
    get_comments = True
    run_name = 'hadith'
    parallel_process = False

    with open(id_file) as f:
        video_ids = [line.rstrip() for line in f]

    crawl_fuc(channel_ids = channel_ids, keyword_ids=keyword_ids,  video_ids = video_ids , get_videos = get_videos,
                 get_channels = get_channels, get_related_videos =get_related_videos,  get_comments = get_comments,
                   run_name = run_name,  parallel_process = parallel_process)



if __name__ == '__main__':
    main()