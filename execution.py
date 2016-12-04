# execution.py

class ExecutionHandler(object):
    """
    Абстрактный класс ExecutionHandler обрабатывает взаимодействие между набором объектов приказов, сгенерированных Portfolio и полным набором объектов Fill, которые возникают на рынке.


    Обработчики могут быть использованы с симулированными брокерскими системами или интерфейсами реальных брокерских систем. Это позволяет тестировать стратегию аналогично работе над движком для реальной торговли.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self, event):
        """
        Берет событие Order и выполняет его, получая событие Fill, которе помещается в очередь Events.

        Параметры:
        event - Содержит объект Event с информацией о приказе
        """
        raise NotImplementedError("Should implement execute_order()")

class SimulatedExecutionHandler(ExecutionHandler):
    """
    Симулированный обработчик конвертирует все объекты приказов в их эквивалентные объекты Fill, автоматически и без задержки, проскальзывания и т.п. Это позволяет быстро протестировать стратегию в первом приближении перед разработкой более сложных реализаций обработчиков.

    """

    def __init__(self, events):
        """
        Инициализирует обработчик, устанавливает внутренние очереди событий.

        Параметры:
        events - Очередь событий Event.
        """
        self.events = events

    def execute_order(self, event):
        """
        Просто наивно конвертирует объекты Order в объекты Fill, то есть не учитывается задержка, проскальзывание или количество акций в приказе, которые можно купить/продать по заданной цене.

        Параметры:
        event - Содержит объект Event с информацией о приказе.
        """
        if event.type == 'ORDER':
            fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol,
                                   'ARCA', event.quantity, event.direction, None)
            self.events.put(fill_event)

