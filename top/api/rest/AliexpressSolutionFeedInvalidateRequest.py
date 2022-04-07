'''
Created by auto_sdk on 2020.12.02
'''
from top.api.base import RestApi
class AliexpressSolutionFeedInvalidateRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.job_id_list = None

	def getapiname(self):
		return 'aliexpress.solution.feed.invalidate'
