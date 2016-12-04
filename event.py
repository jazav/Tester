# event.py

class Event(object):
    """
    Event — это базовый класс, обеспечивающий интерфейс для последующих (наследованных) событий, которые активируют последующие    события в торговой инфраструктуре.
    """
    pass


class MarketEvent(Event):
    """
    Обрабатывает событие получние нового обновления рыночной информации с соответствущими барами.
    """

    def __init__(self):
        """
        Инициализирует MarketEvent.
        """
        self.type = 'MARKET'


class SignalEvent(Event):
    """
    Обрабатывает событие отправки Signal из объекта Strategy. Его получает объект Portfolio, который предпринимает нужное действие.
    """

    def __init__(self, symbol, datetime, signal_type):
        """
        Инициализирует SignalEvent.

        Параметры:
        symbol - Символ тикера, например для Google — 'GOOG'.
        datetime - временная метка момента генерации сигнала.
        signal_type - 'LONG' или 'SHORT'.
        """

        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type


class OrderEvent(Event):
    """
    Обрабатывает событие отправки приказа Order в торговый движок. Приказ содержит тикер (например, GOOG), тип (market или limit), количество и направление.
    """

    def __init__(self, symbol, order_type, quantity, direction):
        """
        Инициализирует тип приказа (маркет MKT или лимит LMT), также устанавливается число единиц финансового инструмента и направление ордера (BUY или SELL).

        Параметры:
        symbol - Инструмент, сделку с которым нужно осуществить.
        order_type - 'MKT' или 'LMT' для приказов Market или Limit.
        quantity - Не-негативное целое (integer) для определения количества единиц инструмента.
        direction - 'BUY' или 'SELL' для длинной или короткой позиции.
        """

        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        """
        Выводит значения, содержащиеся в приказе Order.
        """
        print(
            "Order: Symbol=%s, Type=%s, Quantity=%s, Direction=%s" % \
            (self.symbol, self.order_type, self.quantity, self.direction))


class FillEvent(Event):
    """
    Инкапсулирует понятие исполненного ордера (Filled Order), возвращаемое брокером.
    Хранит количество единиц инструмента, которые были куплены/проданы по конкретной цене.
    Также хранит комиссии сделки.
    """

    def __init__(self, timeindex, symbol, exchange, quantity,
                 direction, fill_cost, commission=None):
        """
        Инициализирует объек FillEvent.
        Устанавливает тикер, биржевую площадку, количество, направление, цены и (опционально) комиссии.

        Если информация о комиссиях отсутствиет, то объект Fill вычислит их на основе объема сделки
        и информации о тарифах брокерах (полученной через API)

        Параметры:
        timeindex - Разрешение баров в момент выполнения ордера.
        symbol - Инструмент, по которому прошла сделка.
        exchange - Биржа, на которой была осуществлена сделка.
        quantity - Количество единиц инструмента в сделке.
        direction - Направление исполнения ('BUY' или 'SELL')
        fill_cost - Размер обеспечения.
        commission - Опциональная комиссия, информация отправляемая бркоером.
        """

        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
        Вычисляет издержки торговли на основе данных API брокера (в нашем случае, американского, т.е. цены в долларах).

       Не включает комиссии биржи.
        """
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:  # Greater than 500
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.fill_cost)
        return full_cost
