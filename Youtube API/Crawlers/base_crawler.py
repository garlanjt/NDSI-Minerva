from Utils.object_classes import ApiKeys
from Crawlers.comment_crawler import CommentCrawler


class Crawler(CommentCrawler):
    def __init__(self, num_processes, crawler_name, run_name, dev_keys=False, parallel_process=False, add_to_db=True):
        self.parallel_process = parallel_process
        self.dev_keys = dev_keys
        self.num_processes = num_processes
        self.add_to_db = add_to_db
        self.api_key = ApiKeys(crawler_name=crawler_name, parallel_process=parallel_process, dev_key=dev_keys, run_name=run_name) #Loading temp key
        CommentCrawler.__init__(self, self.api_key, parallel_process, add_to_db)




########---Crawler Template---########
# from pathos.multiprocessing import ProcessPool as Pool

# from Crawlers.base_crawler import Crawler
# from Data_collection.channel_functions import *
# from Data_collection.video_functions import *


# class __Name__(Crawler):
#     def __init__(self, num_processes, dev_keys=False, parallel_process=False, add_to_db=True):
#         self.crawler_name = '__Name__'
#         Crawler.__init__(self, num_processes, self.crawler_name, dev_keys, parallel_process, add_to_db)
#         if parallel_process:
#             self.process_type = "_parallel"
#         else:
#             self.process_type = "_single"




#     def crawl___Name__(self):

#         ###-RUN-###
#         def run():
#             print("\n\nRunning the __Name__ Crawler")
            # if self.parallel_process: parallel_process()
            # else: single_process()

#         def single_process():
#             pass

#         def parallel_process():
#             pass


#         ###-RUN-###
#         run()