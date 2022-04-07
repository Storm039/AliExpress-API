'''
Created by auto_sdk on 2021.01.19
'''
from top.api.base import RestApi
class AliexpressSolutionSellerCategoryTreeQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.category_id = None
		self.filter_no_permission = None

	def getapiname(self):
		return 'aliexpress.solution.seller.category.tree.query'
