'''
Created by auto_sdk on 2020.12.02
'''
from top.api.base import RestApi
class AliexpressSolutionFeedSubmitRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.item_list = None
		self.operation_type = None

	def getapiname(self):
		return 'aliexpress.solution.feed.submit'
