'''
Created by auto_sdk on 2020.11.11
'''
from top.api.base import RestApi
class AliexpressTradeSellerOrderlistGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.param_aeop_order_query = None

	def getapiname(self):
		return 'aliexpress.trade.seller.orderlist.get'
