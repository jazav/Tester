# performance.py

import numpy as np
import pandas as pd

def create_sharpe_ratio(returns, periods=252):
    """
    Создает коэффициент Шарпа для стратегии, основанной на бенчмарке ноль (нет информации о рисках ).

    Параметры:
    returns -  Series из Pandas представляет процент прибыли за период - Дневной (252), Часовой (252*6.5), Минутный (252*6.5*60) и т.п..
    """
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)

def create_drawdowns(equity_curve):
    """
    Вычисляет крупнейшее падение от пика до минимума кривой PnL и его длительность. Требует возврата  pnl_returns в качестве pandas Series.

    Параметры:
    pnl - pandas Series, представляющая процент прибыли за период.

    Прибыль:
    drawdown, duration - Наибольшая просадка и ее длительность
    """

    # Подсчет общей прибыли
    # и установка High Water Mark
    # Затем создаются серии для просадки и длительности
    hwm = [0]
    eq_idx = equity_curve.index
    drawdown = pd.Series(index = eq_idx)
    duration = pd.Series(index = eq_idx)

    # Цикл проходит по диапазону значений индекса
    for t in range(1, len(eq_idx)):
        cur_hwm = max(hwm[t-1], equity_curve[t])
        hwm.append(cur_hwm)
        drawdown[t]= hwm[t] - equity_curve[t]
        duration[t]= 0 if drawdown[t] == 0 else duration[t-1] + 1
    return drawdown.max(), duration.max()

