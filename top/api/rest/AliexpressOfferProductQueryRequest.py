'''
Created by auto_sdk on 2019.03.27
'''
from top.api.base import RestApi
class AliexpressOfferProductQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.product_id = None

	def getapiname(self):
		return 'aliexpress.offer.product.query'
