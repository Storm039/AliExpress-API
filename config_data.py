class ConfigData:
    # Данные для подключения к AliExpress API
    # Установка appkey (ключ приложения AE) и appsecret (специальный ключ для приложения AE)
    # Эта информация берется из кабинета разработчика AE
    ae_appkey = '31457722'
    ae_appsecret = 'bb73a87a220fb8d15f6ab100f3d33fc1'
    # ae_oauth_token = '50000901917gx8kspdXfEqApTyekh10ff897bKozjyUlfxCvqBvPZQtpyW5J5FgdJdow'
    ae_oauth_token = '50000900728dq8Ver7iDRwtuBd0kxdptfzQEZcew12482edd5LTUGGlMjWeLM8iuYjMc'
    # Токен обновления (в текущей реализации не используется)
    ae_oauth_refresh_token = '50001901128z28kZzgugLqgwQhYgeqBox1EHgKk217ea6877fRUkGXGrrBKJPorkLqcq'
    ae_domain = "api.taobao.com"
    ae_port = 443
    # Данные для подключения к 1С
    one_c_user = 'webuser'
    one_c_password = 'mDvYp9z4Cd1pBxTwsmCF'
    # Методы HTTP-сервиса 1С
    # временно
    one_c_link_get_info_products = 'http://gws.gkk.guru:8085/UT11/hs/GetInfoProductsAli/get_data'
    # one_c_link_get_info_products = 'http://127.0.0.1/UT11/hs/GetInfoProductsAli/get_data'
    # one_c_link_register_products_updates = 'http://gws.gkk.guru:8085/UT11/hs/GetInfoProductsAli/registration'
    token_1c = "r82sjn3L09srypVmubaf"
    # Флаги для класса AELogger:
    # Нужно ли писать сообщения лога операций в файл?
    writeLogMessageToFile = True
    # Нужно ли отправлять сообщения лога операций на удаленный сервер?
    sendLogMessageToServer = False
    # Нужно ли выводить сообщения лога операций в консоли отладчика?
    printLogMessageToConsole = True
    # Имя папки с логами
    log_folder = 'ae_update_logs'

