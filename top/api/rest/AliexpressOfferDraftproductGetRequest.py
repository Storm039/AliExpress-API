'''
Created by auto_sdk on 2019.01.10
'''
from top.api.base import RestApi
class AliexpressOfferDraftproductGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.product_id = None

	def getapiname(self):
		return 'aliexpress.offer.draftproduct.get'
