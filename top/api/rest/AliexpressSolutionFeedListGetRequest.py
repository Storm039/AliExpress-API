'''
Created by auto_sdk on 2020.12.02
'''
from top.api.base import RestApi
class AliexpressSolutionFeedListGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.current_page = None
		self.feed_type = None
		self.page_size = None
		self.status = None
		self.submitted_time_end = None
		self.submitted_time_start = None

	def getapiname(self):
		return 'aliexpress.solution.feed.list.get'
