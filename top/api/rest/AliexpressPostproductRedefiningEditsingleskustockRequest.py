'''
Created by auto_sdk on 2020.11.05
'''
from top.api.base import RestApi
class AliexpressPostproductRedefiningEditsingleskustockRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.ipm_sku_stock = None
		self.product_id = None
		self.sku_id = None

	def getapiname(self):
		return 'aliexpress.postproduct.redefining.editsingleskustock'
