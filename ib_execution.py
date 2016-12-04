# ib_execution.py

import datetime
import time
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message
from event import FillEvent, OrderEvent
from execution import ExecutionHandler


class IBExecutionHandler(ExecutionHandler):
    """
    Получает информацию о приказе через API брокерской торговой системы для ведения счета при живой торговле.
    """

    def __init__(self, events,
                 order_routing="SMART",
                 currency="USD"):
        """
        Инициализация экземпляра IBExecutionHandler.
        """
        self.events = events
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}

        self.tws_conn = self.create_tws_connection()
        self.order_id = self.create_initial_order_id()
        self.register_handlers()

    def _error_handler(self, msg):
        """
        Отвечает за «ловлю» сообщений об ошибках.
        """
        # В нашей версии нет обработки ошибок
        print
        "Server Error: %s" % msg

    def _reply_handler(self, msg):
        """
       Отвечает за обработку ответов сервера
        """
        # Обработка информации о конкретном приказе orderId
        if msg.typeName == "openOrder" and \
                        msg.orderId == self.order_id and \
                not self.fill_dict.has_key(msg.orderId):
            self.create_fill_dict_entry(msg)
        # Обработка исполненных приказов
        if msg.typeName == "orderStatus" and \
                        msg.status == "Filled" and \
                        self.fill_dict[msg.orderId]["filled"] == False:
            self.create_fill(msg)
        print
        "Server Response: %s, %s\n" % (msg.typeName, msg)

    def create_tws_connection(self):
        """
        Подключение к брокерской системе через порт 7496 с clientId 10. Этот clientId выбран нами и необходимо как-то разделять Id для потоков данных о исполненных приказах и рыночных данных, если последний где-либо используется.
        """
        tws_conn = ibConnection()
        tws_conn.connect()
        return tws_conn

    def create_initial_order_id(self):
        """
        Создатет начальный order ID, использующийся для отслеживания отправленных приказов.
        """
        # Здесь можно использовать довольно сложную    #логику, но мы просто установим значение в 1.

        return 1

    def register_handlers(self):
        """
        Регистрация ошибок и методов обработки ответов сервера.
        """
        self.tws_conn.register(self._error_handler, 'Error')
        self.tws_conn.registerAll(self._reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        """
        Создание объекта Contract, который определяет, что будет покупаться, на какой бирже и за какую валюту.

        symbol - Символ тикера контракта
        sec_type - Тип финансового инструмента ('STK' значит акция)
        exch - Биржа, на которой будет осуществляться сделка
        prim_exch - Основная биржа, на которой сделку совершить предпочтительнее
        curr - Валюта сделки
        """
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action):
        """
        Создается объект Order (типа Market/Limit) для осуществления сделки long/short.

        order_type - 'MKT', 'LMT' для приказов Market или Limit
        quantity – Количество акций, которые надо купить или продать
        action - 'BUY' или 'SELL'
        """
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def create_fill_dict_entry(self, msg):
        """
        Создает пометку в словаре Fill Dictionary, где перечислены orderID. Это нужно для реализации событийно-ориентированного поведения системы обработки сообщений сервера.
        """
        self.fill_dict[msg.orderId] = {
            "symbol": msg.contract.m_symbol,
            "exchange": msg.contract.m_exchange,
            "direction": msg.order.m_action,
            "filled": False
        }

    def create_fill(self, msg):
        """
        Создается FillEvent, который после исполнения ордера помещается в очередь событий

        """
        fd = self.fill_dict[msg.orderId]

        # Подготовка данных об исполнении
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice

        # Создание объекта FillEvent
        fill = FillEvent(
            datetime.datetime.utcnow(), symbol,
            exchange, filled, direction, fill_cost
        )

        # Убеждаемся, что из-за многочисленных сообщений не возникли лишние события
        self.fill_dict[msg.orderId]["filled"] = True

        # Помещаем событие fill в очередь
        self.events.put(fill_event)

    def execute_order(self, event):
        """
        Создание необходимы объектов приказов для отправки в брокерскую систему через API.

        После этого запрашиваются результаты для генерации соответствующих событий fill, которые помещаются в очередь.

        Параметры:
        event – Содержит объект Event с информацией о приказе.
        """
        if event.type == 'ORDER':
            # Подготовка параметров финансового инструмента
            asset = event.symbol
            asset_type = "STK"
            order_type = event.order_type
            quantity = event.quantity
            direction = event.direction

            # Создание контракта в брокерской системе с помощью прошедшего события Order

            ib_contract = self.create_contract(
                asset, asset_type, self.order_routing,
                self.order_routing, self.currency
            )

            # Создание приказа в системе брокера с помощью события Order
            ib_order = self.create_order(
                order_type, quantity, direction
            )

            # Использование подключения для отправки приказа
            self.tws_conn.placeOrder(
                self.order_id, ib_contract, ib_order
            )

            # ПРИМЕЧАНИЕ: Следующая строка очень важна
            # Она позволяет убедиться в том, что приказ прошел!
            time.sleep(1)

            # Инкрементно увеличиваем ID приказа для текущей сессии
            self.order_id += 1