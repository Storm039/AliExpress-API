'''
Created by auto_sdk on 2020.01.03
'''
from top.api.base import RestApi
class AliexpressOfferProductPostRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.aeop_a_e_product = None

	def getapiname(self):
		return 'aliexpress.offer.product.post'
