import json
import requests
import top.api
import pandas as pd
from multiprocessing import Pool
from dotenv import load_dotenv
import time
import csv
import datetime
import os
import traceback


class Utils:

    @staticmethod
    def load_env():
        return load_dotenv('.env')


class AELogger:
    """
    Инициализация настроек логгера.
        log_folder - Строка, локальная папка для логов. Находится на одном уровне с файлом скрипта.
        write_log_message_to_file - Булево, определяет необходимость писать сообщения в локальный файл.
        send_log_message_to_server - Булево, определяет необходимость отправлять сообщения на удаленный сервер
        print_log_message_to_console - Булево, определяет, нужно ли выводить сообщение в консоль отладчика.
    """

    def __init__(self, config):
        self.log_folder = config.get("LOG_FOLDER")
        self.write_log_message_to_file = config.get("WRITE_LOG_MESSAGE_TO_FILE")
        self.send_log_message_to_server = config.get("SEND_LOG_MESSAGE_TO_SERVER")
        self.print_log_message_to_console = config.get("PRINT_LOG_MESSAGE_TO_CONSOLE")

    def process_log_message(self, message):
        """
        Процдура выводит логируемое сообщение в файл, консоль или отправляет сообщение на удаленный сервер
        в зависимости от настроек, хранящихся в файле config_data.py
        """

        # Запись в файл
        if self.write_log_message_to_file:
            self.write_message_to_file(message)
        else:
            pass

        # Отправка на удаленный сервер
        if self.send_log_message_to_server:
            self.send_message_to_server(message)
        else:
            pass

        # Вывод в консоль отладчика
        if self.print_log_message_to_console:
            print(message)
        else:
            pass

    def write_message_to_file(self, message='Неизвестная ошибка!'):
        """
        Процедура записывает сообщение в локальный файл лога.
        """
        current_datetime = str(datetime.datetime.now())
        current_date = str(datetime.date.today())
        target_file = self.log_folder + '/ae_product_update_log_' + current_date + '.csv'
        open_type = 'w'
        # Проверяем путь до папки с логами, создаем если папки не существует
        if not os.path.isdir(self.log_folder):
            os.mkdir(self.log_folder)
        # Проверяем наличие файла логов за текущую дату. Выставляем флаг для открытия файла в зависимости от этого
        if os.path.isfile(target_file):
            open_type = 'a'
        # Пишем сообщение об ошибке в файл
        with open(target_file, open_type, newline='', encoding='UTF-8') as csvFile:
            fieldnames = ['Date', 'Log_message']
            writer = csv.DictWriter(csvFile, fieldnames=fieldnames, dialect='excel')
            writer.writerow({'Date': current_datetime, 'Log_message': message})
            csvFile.close()

    @staticmethod
    def send_message_to_server(message):
        """
        Процедура отправляет сообщение на удаленный сервер.
        """
        x = 1


class AEGeneralProductUpdater:
    # Словарь для статусов товаров
    product_statuses = {
        "onSelling": "onSelling",  # в продаже
        "offline": "offline",  # снят с продажи
        "auditing": "auditing",  # на рассмотрении
        "editingRequired": "editingRequired"  # требует внимания и изменения
    }
    path_id_info_file = 'ids_info.csv'
    path_product_info_file = 'product_info.csv'
    # Строки для обращения к структуре ответов/запросов AE
    resp_get_p_list = 'aliexpress_solution_product_list_get_response'
    resp_get_p_info = 'aliexpress_solution_product_info_get_response'
    # Размер страницы при получении списка идентификаторов AE
    id_page_size = 50
    # Коды для составления сообщения об ошибках подключения (скопировано из base.js в TOP API)
    p_code = 'code'
    p_sub_code = 'sub_code'
    p_msg = 'msg'
    p_sub_msg = 'sub_msg'
    # Общее количество страниц с товарами
    total_page_count = 0
    # В result помещаем данные, полученные в процессе работы
    result_list_ali_ids = []
    result_products_info = {}
    data_from_1c = None
    time_sleep = 2

    def __init__(self, config_f):
        # Логгер для записи ошибок
        self.logger = AELogger(config_f)
        self.check_for_file_ids_info_availability()
        self.config = config_f

    def check_for_file_ids_info_availability(self):
        if os.path.isfile(self.path_id_info_file):
            df = pd.read_csv(self.path_id_info_file, sep=";", encoding='utf_8_sig')
        else:
            df = pd.DataFrame({'product_id': list()})
            df.to_csv(self.path_id_info_file, sep=";", encoding='utf_8_sig', index=False)
        if df.shape[0] > 0:
            for id_ in df['product_id'].values:
                self.result_list_ali_ids.append(id_)

    def read_file_product_info(self, return_data=False):
        if os.path.isfile(self.path_product_info_file):
            with open(self.path_product_info_file, 'r') as outfile:
                data = json.load(outfile)
                self.result_products_info = data
                if return_data:
                    return json.load(data)

    def save_file_product_info(self, data=None):
        """
        Функция записывает словарь со списком идентификаторов AliExpress и связанными с ними SKU в csv.
        """
        if data is None:
            data = self.result_products_info
        else:
            self.result_products_info = data

        with open(self.path_product_info_file, 'w') as outfile:
            if not self.result_products_info == dict():
                json.dump(data, outfile)

    def make_request_with_retry(self, request, add_info=None):
        """
        Функция выполняет запрос по API получения информации о товаре AliExpress.
        -  возвращает или ответ API-сервера (словарь), или None.
        """
        # Для запроса по одному товару обрабатывается ошибка, когда список SKU пуст
        response = None
        time.sleep(self.time_sleep)
        if add_info is not None:
            log_message = f'Обработка товара с ID {add_info}'
            self.logger.process_log_message(log_message)

        for j in range(3):
            log_message = "ok"
            response = request.getResponse(self.config.get("AE_OAUTH_TOKEN"))
            if ("error_response" in response
                    and j < 3):
                time.sleep(self.time_sleep)
            elif (request is top.api.AliexpressSolutionProductInfoGetRequest
                  and 'aeop_ae_product_s_k_us' not in response[self.resp_get_p_info]['result']
                  and j < 3):
                time.sleep(self.time_sleep)
            else:  # НЕ ОШИБКА!!!
                try:
                    if not response.get('aliexpress_solution_product_info_get_response') is None:
                        res = response['aliexpress_solution_product_info_get_response']['result']["subject"]
                    elif not response.get('aliexpress_solution_product_list_get_response') is None:
                        res = response['aliexpress_solution_product_list_get_response']['result']["success"]
                    else:
                        res = "Структура ответа от сервера - не валидная."
                    log_message = f"Структура ответа от сервера - валидная."
                except KeyError as er:
                    log_message = er
                    pass
                finally:
                    self.logger.process_log_message(log_message)
                    return response

        log_message = '---> error_response'
        self.logger.process_log_message(log_message)
        # Если после 3 попыток не получен положительный ответ от сервера,
        # формируем сообщение об ошибке и пишем его в лог, после чего возвращаем None
        self.log_final_error_message(request=request, response=response)
        return None

    def log_final_error_message(self, request, response):
        """
        Процедура формирует сообщение об ошибке, возникшей при выполнении запроса к API AliExpress.
            request - объект http-запроса к API AliExpress.
            response - объект http-ответа от API AliExpress.
        """
        log_message = ""
        error_code = ""
        error_code_message = ""
        error_sub_code = ""
        error_sub_code_message = ""

        if "error_response" in response:
            if self.p_code in response["error_response"]:
                if "product_info_response" in response["error_response"]:
                    # Если ошибка связана с товаром
                    error_code = str(response["product_info_response"][self.p_code])
                else:
                    # Иначе это общая ошибка
                    error_code = str(response["error_response"][self.p_code])
            if self.p_msg in response["error_response"]:
                error_code_message = str(response["error_response"][self.p_msg])
            if self.p_sub_code in response["error_response"]:
                error_sub_code = str(response["error_response"][self.p_sub_code])
            if self.p_sub_msg in response["error_response"]:
                error_sub_code_message = str(response["error_response"][self.p_sub_msg])

            log_message = f"Ошибка исполнения запроса.\n" \
                          f"Код ошибки: {error_code}.\n" \
                          f"Описание: {error_code_message}.\n" \
                          f"Дополнительный код ошибки: {error_sub_code}.\n" \
                          f"Описание: {error_sub_code_message}.\n" \
                          f"{'---' * 20}"
        elif (request is top.api.AliexpressSolutionProductInfoGetRequest
              and 'aeop_ae_product_s_k_us' not in response[self.resp_get_p_info]['result']):
            # При выполнении запроса на получение данных одного товара иногда возвращается пустой список SKU
            # (даже для товаров без вариаций должен быть хотя бы один SKU)
            log_message = f"Ошибка исполнения запроса: API не вернул список SKU.\n" \
                          f"Проблеманый ID товара: {str(request.product_id)}.\n" \
                          f"{'---' * 20}"
        else:
            log_message = f"Неизвестная ошибка исполнения запроса.\n" \
                          f"Проблеманый ID товара: {str(request.product_id)}.\n" \
                          f"{'---' * 20}"
        self.logger.process_log_message(log_message)

    def process_get_list_ids(self, page_num=None):
        result = None
        # Создание подключения к зарубежному серверу AE по HTTPS
        cur_product_list_request = top.api.AliexpressSolutionProductListGetRequest(
            self.config.get("AE_DOMAIN"),
            self.config.get("AE_PORT")
        )
        cur_product_list_request.set_app_info(top.appinfo(
            self.config.get("AE_APPKEY"),
            self.config.get("AE_APPSECRET"))
        )
        cur_product_list_request.aeop_a_e_product_list_query = {
            "current_page": page_num,
            "page_size": self.id_page_size,
            "product_status_type": self.product_statuses["onSelling"]
        }

        # Получаем одну страницу на 50 товаров. Осуществляем 3 попытки получить данные т.к. AE иногда глючит
        items_list = list()
        try:
            cur_product_list_response = self.make_request_with_retry(cur_product_list_request)
            if cur_product_list_response is None:
                log_message = f"Ошибка запроса списка товаров в продаже - запрос не вернул результат.\n" \
                              f"Не обработана страница № {str(page_num)}"
                self.logger.process_log_message(log_message)
            else:
                # Получаем массив словарей. В словарях хранятся идентификаторы на AliExpress
                response = cur_product_list_response[self.resp_get_p_list]['result']
                list_products_info = response['aeop_a_e_product_display_d_t_o_list']['item_display_dto']
                for i in list_products_info:
                    items_list.append(i['product_id'])
                result = items_list
        except Exception as e:
            log_message = f"Ошибка запроса к API для полученя страницы списка товаров.\n" \
                          f"Не обработана страница № {str(page_num)}.\n" \
                          f"Текст ошибки с цепочкой вызовов:\n" \
                          f"{traceback.format_exc()}"

            self.logger.process_log_message(log_message)

        return result

    def get_num_page_count(self):
        log_message = f"Начало операции по получению количества страниц с товарами.\n" \
                      f"{'-' * 20}"
        self.logger.process_log_message(log_message)

        # Если id прочитаны при инициализации объекта класса, то нет смысла выполнять данную функцию
        if len(self.result_list_ali_ids) > 0:
            return

        req = top.api.AliexpressSolutionProductListGetRequest(
            self.config.get("AE_DOMAIN"),
            self.config.get("AE_PORT")
        )
        req.set_app_info(top.appinfo(
            self.config.get("AE_APPKEY"),
            self.config.get("AE_APPSECRET"))
        )
        # Данные запроса (получения общего количества товаров)
        req.aeop_a_e_product_list_query = {
            "current_page": 1,
            "page_size": 1,
            "product_status_type": self.product_statuses["onSelling"]
        }

        # Запрос на один товар. Необходим для получения общего числа опубликованных товаров
        resp = req.getResponse(self.config.get("AE_OAUTH_TOKEN"))
        product_count = resp[self.resp_get_p_list]['result']['product_count']
        self.total_page_count = int(product_count // self.id_page_size)
        if product_count % self.id_page_size > 0:
            self.total_page_count += 1

    def get_list_ids_ali(self):
        # Инициализация переменных
        response_data = []

        log_message = f"Начало операции по получению списка Идентификаторов AliExpress.\n" \
                      f"{'-' * 20}"
        self.logger.process_log_message(log_message)

        # Если id прочитаны при инициализации объекта класса, то нет смысла выполнятьт данную функцию
        if len(self.result_list_ali_ids) > 0:
            return
        # Для каждой страницы создаем свой процесс получения данных от AE
        # Полученные результаты преобразуем в датафреймы и объединяем
        # Так как каждая страница содержит уникальные данные, настраивать слияние не надо
        # В случае, если при получении списка данных возникла ошибка с той или иной страницей, такая страница
        # В списке будет представлена пустым словарем, а в логах будет сказано, какая страница не была выгружена.
        try:
            # Сформируем список страниц, для параллельного обхода в pool
            page_list = list(range(1, self.total_page_count + 1))
            pool = Pool(processes=10)
            pool_map = pool.map_async(self.process_get_list_ids, page_list)
            response_data = pool_map.get()
        except TimeoutError:
            log_message = "Ошибка исполнения кода процесса получения страницы списка товаров Ali:\n" \
                          "Превышено время ожидания ответа от сервера AliExpress."
            self.logger.process_log_message(log_message)
        except Exception:
            log_message = f"Неизвестная ошибка исполнения кода процесса получения страницы списка товаров Ali.\n" \
                          f"Описание ошибки с цепочкой вызовов:\n" \
                          f"{traceback.format_exc()}"
            self.logger.process_log_message(log_message)

        list_to_save = []
        for i_list in response_data:
            self.result_list_ali_ids.extend(i_list)
            list_to_save.extend(i_list)

        if len(list_to_save) > 0:
            df = pd.DataFrame({'product_id': list_to_save})
            df.to_csv(self.path_id_info_file, sep=";", encoding='utf_8_sig', index=False)

    def process_get_products_info(self, product_id):
        """
        Функция вызывает другую функцию получения данных по одному идентификатору AliExpress.
        Если ответ по API вернулся без ошибок, функция записывает в словарь result_dict список полученных SKU,
        и возвращает словарь; иначе возвращает пустой result_dict.
        """
        # Отдельный запрос для получения информации об одном товаре
        cur_product_info_request = top.api.AliexpressSolutionProductInfoGetRequest(
            self.config.get("AE_DOMAIN"),
            self.config.get("AE_PORT")
        )
        cur_product_info_request.set_app_info(top.appinfo(
            self.config.get("AE_APPKEY"),
            self.config.get("AE_APPSECRET"))
        )

        items_list = self.result_list_ali_ids
        result_dict = {'SKU': None, 'product_id': product_id}
        list_SKU = []

        # # Задержка в 1 сек чтобы не перегружать API слишком частыми запросами
        # time.sleep(1)
        # Выполнение запроса по товару
        cur_product_info_request.product_id = product_id

        # Повторяем запрос до 3х раз, если возникают ошибки
        # Если в конце получаем назад None - проскакиваем данный товар
        # Если все успешно - получаем результат запроса
        product_info_response = self.make_request_with_retry(cur_product_info_request, product_id)
        if product_info_response is None:
            return result_dict

        # Если структура правильная - обрабатываем список SKU
        if 'aeop_ae_product_s_k_us' in product_info_response[self.resp_get_p_info]['result']:
            response_result = product_info_response[self.resp_get_p_info]['result']
            current_sku_list = response_result['aeop_ae_product_s_k_us']['global_aeop_ae_product_sku']
            # Собираем список SKU
            for sku_row in current_sku_list:
                list_SKU.append(sku_row['sku_code'])
            result_dict['SKU'] = list_SKU
        else:  # иначе пишем ошибку
            log_message = f"Ошибка исполнения запроса: сервер не вернул список SKU для товара с ID {str(product_id)}"
            self.logger.process_log_message(log_message)

        return result_dict

    def control_full_product_info(self):
        """
        Функция проверяет прочитанный словарь со списком идентификаторов AliExpress и SKU,
        проверяет список на наличие идентификаторов, для которых еще не был получен набор SKU.

        Если в списке есть ключи (идентификаторы AliExpress), для которых еще не получен список SKU (значение по ключу
        равно None), то функция возвращает True.
        Если SKU получены для всех идентификаторов AliExpress, то функция возвращает False
        """
        result = False
        list_with_data = []
        for key, values in self.result_products_info.items():
            if not values is None:
                list_with_data.append(key)
        if len(list_with_data) != len(self.result_list_ali_ids):
            result = True
        else:
            if None in self.result_products_info.values():
                result = True

        return result

    def get_products_info_ali(self):
        """
        Функция вызывает функцию получения списка SKU для каждого идентификатора AliExpress, для которго список SKU
        еще не получен.
        Полученные по API данные сохраняются в свойстве result_products_info, полсле чего вызывается другая функция,
        которая записывает полученные данные в csv-файл.
        """

        log_message = f"Начало операции по формированию списка SKU товаров, размещенных на ALiExpress.\n" \
                      f"{'-' * 20}"
        self.logger.process_log_message(log_message)

        # получаем данные только по тем id, по которым данных нет
        not_info_list = []
        full_id_list = self.result_list_ali_ids
        for i in full_id_list:
            values = self.result_products_info.get(str(i))
            if values is None:
                not_info_list.append(i)
            else:
                if isinstance(values, list):
                    if len(values) == 0:
                        not_info_list.append(i)

        try:
            # Сформируем список страниц, для параллельного обхода в pool
            ids_list = not_info_list
            pool = Pool(processes=1)
            pool_map = pool.map_async(self.process_get_products_info, ids_list)
            response_data = pool_map.get()
        except TimeoutError:
            log_message = "Ошибка исполнения кода процесса получения списка SKU для товара, размещенного на AE:\n" \
                          + "Превышено ожидание ответа сервера."
            self.logger.process_log_message(log_message)
        except Exception:
            log_message = f"Неизвестная ошибка исполнения кода процесса получения списка SKU для товара, " \
                          f"размещенного на AE.\n" \
                          f"Описание ошибки с цепочкой вызовов:\n" \
                          f"{traceback.format_exc()}"
            self.logger.process_log_message(log_message)

        for i in response_data:
            self.result_products_info[str(i['product_id'])] = i['SKU']
        # return None
        self.save_file_product_info()

    def collecting_product_data(self):
        """
        Функция проверяет, нужно ли продолжать получение списка SKU для имеющихся идентификаторов AliExpress,
        и если надо, то запускает функцию получения списка SKU.
        Вызов производится в цикле, который закончится только:
        1) Если по всем идентификаторам AliExpress получены SKU.
        2) Если число итераций цикла не достигло 5000.
        """
        index = 0
        while self.control_full_product_info():
            if index == 5000:
                break
            self.get_products_info_ali()
            index += 1

    def get_data_from_1c(self):
        """
        Функция вызывает HTTP-сервис 1С. Дополнительно передается токен доступа.
        Ответ HTTP-сервиса - словарь с ключами:
        1) error - код ошибки. 0 - если нет, 1 - если есть
        2) description - описание ошибки
        3) data - словарь с массивами в значениях. Все индексы в данных массивах связаны как строки таблицы значений.
            - SKU
            - product_id
            - price
            - inventory
        """

        # Записываем в лог два сообщения - одно начале операции обновления, другое по получению данных от 1С
        # log_message = f"Начало операции обновления цен и остатков товаров на AliExpress.\n" \
        #               f"{'---' * 20}"
        # self.logger.process_log_message(log_message)

        # log_message = f"Получение данных от 1С.\n" \
        #               f"{'---' * 20}"
        # self.logger.process_log_message(log_message)

        json_df = {"token": self.config.get("TOKEN_1C")}
        config_data = {
            "json_df": json_df,
            "link": self.config.get("ONE_C_LINK_GET_INFO_PRODUCTS"),
            "one_c_auth": (self.config.get("ONE_C_USER"),
                           self.config.get("ONE_C_PASSWORD")),
            "logger": self.logger
        }
        one_c_response = AEGeneralProductUpdater.get_1c_response(config_data)

        if one_c_response['error'] == 0:
            # log_message = f"Данные успешно получены из 1С"
            # self.logger.process_log_message(log_message)
            self.data_from_1c = pd.DataFrame(one_c_response['data'])
        else:
            log_message = f"Не удалось получить данные из 1С. Ошибка:\n" \
                          f"{one_c_response['description']}"
            self.logger.process_log_message(log_message)

    @staticmethod
    def get_1c_response(config_data):
        # Запрос к 1С
        one_c_response = requests.post(config_data.get("link"),
                                       json=config_data.get("json_df"),
                                       auth=config_data.get("one_c_auth"))
        # Если вернется ошибка - пишем в лог и прерываем исполнение
        if one_c_response.status_code != 200:
            log_message = f"Ошибка при получении данных от 1С.\n" \
                          f"Код ответа сервера: {str(one_c_response.status_code)}\n" \
                          f"Описание ошибки: {str(one_c_response.reason)}"
            config_data.get("logger").process_log_message(log_message)
            exit(-3)
        # Возвращаем тело ответа, конвертированное в JSON
        return json.loads(one_c_response.text)


class AEProductBatchUpdater:
    # Коды для составления сообщения об ошибках подключения (скопировано из base.js в TOP API)
    P_CODE = 'code'
    P_SUB_CODE = 'sub_code'
    P_MSG = 'msg'
    P_SUB_MSG = 'sub_msg'
    # Особо длинные строки (имена в структурах AE и т.д.)
    str_inventory_update_response = 'aliexpress_solution_batch_product_inventory_update_response'
    str_price_update_response = 'aliexpress_solution_batch_product_price_update_response'
    str_product_dto = 'synchronize_product_response_dto'
    # Время ожидания между выполнением запросов на обновление, сек.
    request_sleep_time = 4

    # Конструктор класса
    def __init__(self, config):
        # Логгер для записи ошибок
        self.logger = AELogger(config)
        self.config = config

    @staticmethod
    def create_data_batch(full_df, max_size_ID=15, size_SKU=200, name_col_ID='product_id'):
        """
        Функция разбивает полученный датафрейм с данными обновления цен и остатков на порции.
        Согласно требованиям AliExpress, одна порция товаров для обновления цен и остатков может содержать
        20 товаров (идентификаторов AliExpress) или 200 вариаций товаров (SKU).

        ВАЖНО: 28032022 при обновлении цен и остатков API начал возвращать сообщение о том что для 1-2 товаров
        обновление не произошло по причине ошибки типа "HSF Provider thread pool is full".
        Сокращение порции товаров (параметр max_size_ID) с 20 до 15 решило данную проблему.
        В документации к API нет информации об изменении ограничений для порций товаров.
        """
        col_names = {}
        num = 0
        for col in full_df.columns.values:
            col_names.update({str(col): num})
            num += 1

        count_ID = 0
        count_SKU = 0
        batch_list = []
        data_dict = {}

        for i in col_names.keys():
            data_dict.update({i: []})

        ID_list_unique = full_df[name_col_ID].unique()

        for ID_num in ID_list_unique:
            count_ID += 1
            count_SKU = 0

            df_filter = full_df[full_df[name_col_ID] == ID_num]
            for val in df_filter.values:
                count_SKU += 1
                if count_ID > max_size_ID:
                    df_ = pd.DataFrame(data_dict)
                    batch_list.append(df_)
                    count_ID = 1
                    count_SKU = 1

                    data_dict = {}
                    for i in col_names.keys():
                        data_dict.update({i: []})

                if count_SKU > size_SKU:
                    df_ = pd.DataFrame(data_dict)
                    batch_list.append(df_)
                    count_ID = 1
                    count_SKU = 1

                    data_dict = {}
                    for i in col_names.keys():
                        data_dict.update({i: []})

                for key in col_names.keys():
                    data_dict[key].append(val[col_names[key]])

        df_ = pd.DataFrame(data_dict)
        batch_list.append(df_)

        return batch_list

    @staticmethod
    def form_update_list(dataFrame, updated_resource_string):
        """
        Функция формирует тело HTTP-запроса на обновление цен или остатков согласно полученному параметру
        updated_resource_string.
        Тело представляет из себя список словарей.
        Каждый словарь имеет ключи:
        1) product_id - идентификатор товара на AliExpress
        2) multiple_sku_update_list - cписок словарей. Каждый словарь имеет клчючи
            1) sku_code - обновляемый SKU товара
            2) price/inventory - ключ для цены если обновляется цена или ключ для остатков если обновляются остатки.
        """
        # Формируем список данных для передачи в запросе
        current_product_update_list = list()
        # Получаем список уникальных product_id в датафрейме
        unique_ids = dataFrame['product_id'].unique()
        # Циклом формируем данные запроса для AE
        for nextId in unique_ids:
            # Получаем словарь с данными по каждому product_id для формирования части запроса
            next_batch_of_data = dataFrame[dataFrame['product_id'] == nextId].reset_index().to_dict()

            id_dict = dict()
            id_dict["product_id"] = nextId
            # Формируем список словарей с новыми значениями "ресурса" для каждого SKU товара
            id_dict["multiple_sku_update_list"] = list()
            for i in range(len(next_batch_of_data['SKU'])):
                row_dict = {
                    'sku_code': next_batch_of_data['SKU'][i],
                    updated_resource_string: next_batch_of_data[updated_resource_string][i]
                }
                id_dict["multiple_sku_update_list"].append(row_dict)
            current_product_update_list.append(id_dict)
        return current_product_update_list

    def make_request_with_retry(self, request, current_product_list, access_token, batch_index):
        """
        Функция выполняет запрос по API обновления цен или остатков товаров AliExpress.
        Если запрос возвращает ошибку, функция повторяет запрос до трех раз.
        Если данные товара были обновлены успешно, функция возвращает True.
        Если все три попытки обновить данные завершились ошибками, или произошла непредвиденная ошибка,
        функция возвращает False.
        """
        request.mutiple_product_update_list = current_product_list
        response = None
        for j in range(3):
            response = request.getResponse(access_token)
            if "error_response" in response and j < 3:
                time.sleep(self.request_sleep_time)
            else:
                try:
                    # Обработка ответа сервера, если общих ошибок не возникло
                    if type(request) is top.api.AliexpressSolutionBatchProductInventoryUpdateRequest:
                        if not response[self.str_inventory_update_response]['update_success']:
                            self.log_failed_update_response(request, response)
                            # 20210420: в случае если при обновлении произошли ошибки, возвращаем False
                            return False
                        else:
                            return True
                    elif type(request) is top.api.AliexpressSolutionBatchProductPriceUpdateRequest:
                        if not response[self.str_price_update_response]['update_success']:
                            self.log_failed_update_response(request, response)
                            # 20210420: в случае если при обновлении произошли ошибки, возвращаем False
                            return False
                        else:
                            return True
                    else:
                        return False
                except Exception as e:
                    log_message = f"Неизвестная ошибка типа {str(type(e))} при валидации ответа от AliExpress.\n" \
                                  f"Описание ошибки с цепочкой вызовов:\n" \
                                  f"{traceback.format_exc()}"
                    self.logger.process_log_message(log_message)
                    # 20210420: в случае если при обновлении произошли ошибки, возвращаем False
                    return False
        # Если после 3 попыток проблема не решена, пишем в лог и возвращаем None
        self.log_final_error_message(response)
        # 20210420: если после трех попыток ничего не обновилось, вне зависимости от наличия описания ошибки в ответе
        # возвращаем False
        return False

    def log_failed_update_response(self, request, response):
        """
        Процедура формирует сообщение о неудачном обновлении одного или более товаров из пакета.
            request - объект http-запроса к API AliExpress.
            response - объект http-ответа от API AliExpress.
        """
        error_code = "Нет кода"
        error_message = "Неизвестная ошибка"
        items_dict = {}
        if type(request) is top.api.AliexpressSolutionBatchProductInventoryUpdateRequest:
            error_code = response[self.str_inventory_update_response]['update_error_code']
            error_message = response[self.str_inventory_update_response]['update_error_message']
            failed_updates_dict = response[self.str_inventory_update_response]['update_failed_list']
            items_dict = failed_updates_dict[self.str_product_dto]
        elif type(request) is top.api.AliexpressSolutionBatchProductPriceUpdateRequest:
            error_code = response[self.str_price_update_response]['update_error_code']
            error_message = response[self.str_price_update_response]['update_error_message']
            failed_updates_dict = response[self.str_price_update_response]['update_failed_list']
            items_dict = failed_updates_dict[self.str_product_dto]
        else:
            pass

        log_message = f"Запрос выполнен с ошибкой:\n" \
                      f"Код ошибки: {error_code}\n" \
                      f"Описание ошибки: {error_message}\n" \
                      f"Список не обновленных товаров:\n"

        for item in items_dict:
            product_id = str(item['product_id'])
            error_code = str(item['error_code'])
            error_message = str(item['error_message'])
            log_message += f"{product_id} - {error_message} ( {error_code} )\n"
        self.logger.process_log_message(log_message)

    def log_final_error_message(self, response):
        """
        Процедура формирует сообщение об ошибке, возникшей при выполнении запроса на обновление списка товаров Ali.
            response - объект http-ответа от API AliExpress.
        """
        log_message = ""
        error_code = ""
        error_code_message = ""
        error_sub_code = ""
        error_sub_code_message = ""

        if "error_response" in response:
            if self.P_CODE in response["error_response"]:
                if "product_info_response" in response["error_response"]:
                    # Если ошибка связана с товаром
                    error_code = str(response["product_info_response"][self.P_CODE])
                else:
                    # Иначе это общая ошибка
                    error_code = str(response["error_response"][self.P_CODE])
            if self.P_MSG in response["error_response"]:
                error_code_message = str(response["error_response"][self.P_MSG])
            if self.P_SUB_CODE in response["error_response"]:
                error_sub_code = str(response["error_response"][self.P_SUB_CODE])
            if self.P_SUB_MSG in response["error_response"]:
                error_sub_code_message = str(response["error_response"][self.P_SUB_MSG])

            log_message = f"Ошибка исполнения запроса.\n" \
                          f"Код ошибки: {error_code}.\n" \
                          f"Описание: {error_code_message}.\n" \
                          f"Дополнительный код ошибки: {error_sub_code}.\n" \
                          f"Описание: {error_sub_code_message}.\n" \
                          f"{'---' * 20}"
        else:
            log_message = f"Неизвестная ошибка исполнения запроса.\n" \
                          f"{'---' * 20}"
        self.logger.process_log_message(log_message)

    def update_price(self, df):
        """
        Функция формирует список батчей для обновления цен и итерирует по нему. Для каждого батча вызывает функцию
        формирования словаря с телом запроса по API и вызывает функцию отправки запроса.
        После каждой итерации в цикле система ожидает ~4 секунды.
        """
        # Обновление цен
        request_price = top.api.AliexpressSolutionBatchProductPriceUpdateRequest(
            self.config.get("AE_DOMAIN"),
            self.config.get("AE_PORT")
        )
        request_price.set_app_info(top.appinfo(
            self.config.get("AE_APPKEY"),
            self.config.get("AE_APPSECRET"))
        )
        # Разбиваем датафрейм на пачки данных, поделенные в соответствии с требованиями AE
        df_batch_list = self.create_data_batch(df)

        # Простой счтечик для номера батча
        batch_num = 0

        for next_batch in df_batch_list:
            batch_num += 1

            next_batch['product_id'] = next_batch['product_id'].astype('int64')
            next_batch['inventory'] = next_batch['inventory'].astype('int32')
            next_batch['price'] = next_batch['price'].astype('str')

            # Цены
            current_product_list = self.form_update_list(next_batch, 'price')
            self.make_request_with_retry(
                request_price,
                current_product_list,
                self.config.get("AE_OAUTH_TOKEN"),
                batch_num)
            time.sleep(self.request_sleep_time)

    def update_inventory(self, df):
        """
        Функция формирует список батчей для обновления остатков и итерирует по нему.
        Для каждого батча вызывает функцию формирования словаря с телом запроса по API и вызывает функцию отправки
        запроса.
        После каждой итерации в цикле система ожидает ~4 секунды.
        """
        # Обновление остатков
        request_inventory = top.api.AliexpressSolutionBatchProductInventoryUpdateRequest(
            self.config.get("AE_DOMAIN"),
            self.config.get("AE_PORT")
        )
        request_inventory.set_app_info(top.appinfo(
            self.config.get("AE_APPKEY"),
            self.config.get("AE_APPSECRET"))
        )

        # Разбиваем датафрейм на пачки данных, поделенные в соответствии с требованиями AE
        df_batch_list = self.create_data_batch(df)

        # Простой счтечик для номера батча
        batch_num = 0

        for next_batch in df_batch_list:
            batch_num += 1

            next_batch['product_id'] = next_batch['product_id'].astype('int64')
            next_batch['inventory'] = next_batch['inventory'].astype('int32')
            next_batch['price'] = next_batch['price'].astype('str')

            # Остатки
            current_product_list = self.form_update_list(next_batch, 'inventory')
            self.make_request_with_retry(
                request_inventory,
                current_product_list,
                self.config.get("AE_OAUTH_TOKEN"),
                batch_num)
            time.sleep(self.request_sleep_time)

    def update_resources(self, df):
        """
        Функция вызывает функции обновления цен и остатков для полученного от 1С списка товаров.
        """
        if df is not None:
            # log_message = f"Начало обновления прайс листа.\n" \
            #               f"{'---' * 20}"
            # self.logger.process_log_message(log_message)
            self.update_price(df)

            # log_message = f"Начало обновления остатков.\n" \
            #               f"{'---' * 20}"
            # self.logger.process_log_message(log_message)
            self.update_inventory(df)

            # log_message = f"Окончание операции обновления цен и остатков товаров на AliExpress.\n" \
            #               f"{'---' * 20}"
            # self.logger.process_log_message(log_message)
        else:
            log_message = f"Нет необходимых данных для обновления цен и остатков на AliExpress.\n" \
                          f"{'---' * 20}"
            self.logger.process_log_message(log_message)


if __name__ == '__main__':
    # Инициализация классов
    config = Utils.load_env()
    collector = AEGeneralProductUpdater(config)
    updater = AEProductBatchUpdater(config)

    # Рассчитываем количество страниц, записываем в свойство класса
    # collector.get_num_page_count()

    # Получаем список идентификаторов AliExpress, записываем в свойство класса и сохраняем в csv-файл
    # collector.get_list_ids_ali()

    # Читаем ранее созданный файл со списком Идентификаторв ALi со SKU
    # collector.read_file_product_info()

    # Получаем по API списки SKU для сохраненных в файле идентификаторов AliExpress,
    # и записываем результирующий словарь со списком идентификаторов AliExpress и связанными SKU в файл
    # collector.collecting_product_data()

    collector.get_data_from_1c()
    updater.update_resources(collector.data_from_1c)
