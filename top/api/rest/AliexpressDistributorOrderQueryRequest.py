'''
Created by auto_sdk on 2020.12.01
'''
from top.api.base import RestApi
class AliexpressDistributorOrderQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.query_request = None

	def getapiname(self):
		return 'aliexpress.distributor.order.query'
