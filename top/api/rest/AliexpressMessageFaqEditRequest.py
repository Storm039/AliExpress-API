'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AliexpressMessageFaqEditRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.param_message_faq_subject_dto = None

	def getapiname(self):
		return 'aliexpress.message.faq.edit'