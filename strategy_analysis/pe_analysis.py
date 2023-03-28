import os
import tushare as ts
import math
from typing import List
from random import sample
import time
from multiprocessing import Pool

from pyalgotrade import strategy
from pyalgotrade.broker.backtesting import Broker, TradePercentage
from pyalgotrade.barfeed.tusharefeed import Feed


class PEStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, ins_set, initial_capital, group_index, group_num=5):
        assert group_index <= group_num
        commission = TradePercentage(0.003)
        broker = Broker(
            cash=initial_capital,
            barFeed=feed,
            commission=commission)
        super().__init__(feed, broker)

        self.__ins_set = ins_set
        self.__group_index = group_index
        self.__group_num = group_num
        self.__positions = {}
        self.__buy_num = 100
        self.__refresh_rate = 20
        self.__days = 0

    # def onEnterOk(self, position):
    #     execInfo = position.getEntryOrder().getExecutionInfo()
    #     price = execInfo.getPrice()
    #     qty = execInfo.getQuantity()
    #     fee = execInfo.getCommission()
    #     cash = self.getBroker().getCash()
    #     self.info('Buy at ￥%.2f, %d shares, commission ￥%.2f, remaining cash: ￥%.2f' % (price, qty, fee, cash))

    def onExitOk(self, position):
        # exec_info = position.getExitOrder().getExecutionInfo()
        # price = exec_info.getPrice()
        # qty = exec_info.getQuantity()
        # fee = exec_info.getCommission()
        # pnl = position.getPnL(price)
        # cash = self.getBroker().getCash()
        # self.info('Sell at ￥%.2f, %d shares, commission ￥%.2f, profit or loss ￥%.2f, remaining cash: ￥%.2f' % (
        #     price, qty, fee, pnl, cash
        # ))
        del self.__positions[position.getEntryOrder().getInstrument()]

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        position.exitMarket()

    def onBars(self, bars):
        if self.__days == 0:
            # sell all stocks
            for stock, position in self.__positions.items():
                if not position.exitActive():
                    position.exitMarket(goodTillCanceled=True)
            for order in self.getBroker().getActiveOrders():
                if order.isSell():
                    self.getBroker().onBarsImpl(order, bars)

            # buy stocks
            if len(self.__positions) < self.__buy_num:
                cash_per_stock = self.getBroker().getCash() / (self.__buy_num - len(self.__positions))

                date = bars.getDateTime().date().strftime('%Y%m%d')
                buy_stock_list = self.__choose_stocks(date, self.__buy_num - len(self.__positions))

                # buy stocks
                for stock in buy_stock_list:
                    if stock in self.__positions:
                        continue
                    shares = self.__max_shares_can_buy(cash_per_stock, bars[stock].getClose())

                    if shares > 0:
                        # Enter a buy market order. The order is good till canceled.
                        self.__positions[stock] = self.enterLong(
                            instrument=stock,
                            quantity=shares,
                            goodTillCanceled=False,
                            allOrNone=False)
                for order in self.getBroker().getActiveOrders():
                    if order.isBuy():
                        self.getBroker().onBarsImpl(order, bars)
        self.__days = (self.__days + 1) % self.__refresh_rate

    def __choose_stocks(self, date, buy_num) -> List[str]:
        pro = ts.pro_api('8b9cf2cea52f98da2bb6b80e8669693c0f4a08e3f0e2a57aebd4159b')
        pe = pro.daily_basic(trade_date=date)
        pe = pe[pe['pe'].notnull()]
        pe.sort_values('pe', inplace=True)
        pe.reset_index(drop=True, inplace=True)
        stock_list = pe['ts_code'].tolist()
        stock_list = [stock for stock in stock_list if stock in self.__ins_set]
        n = len(stock_list) // self.__group_num
        stock_list = stock_list[n * (self.__group_index - 1): n * self.__group_index]
        if len(stock_list) <= buy_num:
            buy_list = stock_list
        else:
            buy_list = sample(stock_list, buy_num)
        return buy_list

    def __max_shares_can_buy(self, cash, price) -> int:
        commission = self.getBroker().getCommission()
        ub = int(math.floor(cash / price))
        if ub * price + commission.calculate(None, price, ub) <= cash:
            return ub
        lb = 0
        assert cash >= 0
        while lb < ub:
            mid = (lb + ub + 1) // 2
            if cash < price * mid + self.getBroker().getCommission().calculate(None, price, mid):
                ub = mid - 1
            else:
                lb = mid
        return lb


def prepare_daily_data(stocks, start_date, end_date, path=''):
    pro = ts.pro_api('8b9cf2cea52f98da2bb6b80e8669693c0f4a08e3f0e2a57aebd4159b')
    print('data preparation: ')
    n = len(stocks) // 100
    for i, stock in enumerate(stocks):
        csv_path = path + '{stock} {st}-{et}.csv'.format(stock=stock, st=start_date, et=end_date)
        if not os.path.exists(csv_path):
            ret = pro.daily(ts_code=stock, start_date=start_date, end_date=end_date)
            ret['vol'] *= 100
            ret['amount'] *= 1000
            ret.to_csv(csv_path)
        if n > 0 and i % n == 0:
            print('\r[' + '-' * (i // n) + '>' + '.' * (99 - i // n) + ']', end='')
    print('\ndata preparation finished!')


def get_total_stock_set(start_date):
    pro = ts.pro_api('8b9cf2cea52f98da2bb6b80e8669693c0f4a08e3f0e2a57aebd4159b')
    ret = pro.stock_basic(list_status='L')
    # 筛选在start_date之前上市的股票
    ret = ret[ret['list_date'] < start_date]
    stock_set = set(ret['ts_code'])
    return stock_set


def main(group_index):
    t = time.time()
    start_date = '20100101'
    end_date = '20141231'
    path = 'C:/developer/data/daily/stocks/'

    stock_set = get_total_stock_set(start_date)
    print('stocks number: %d' % (len(stock_set)))
    # prepare_data
    prepare_daily_data(stock_set, start_date, end_date, path)
    pre_time = time.time() - t

    t = time.time()
    feed = Feed()
    print('csv data feed: ')
    n = len(stock_set) // 100
    i = 0
    for stock in stock_set:
        csv_path = path + '{stock} {st}-{et}.csv'.format(stock=stock, st=start_date, et=end_date)
        feed.addBarsFromCSV(stock, csv_path)
        i += 1
        if n > 0 and i % n == 0:
            print('\r[' + '-' * (i // n) + '>' + '.' * (99 - i // n) + ']', end='')
    print('\ncsv data feed finished!')
    feed_time = time.time() - t

    t = time.time()
    my_strategy = PEStrategy(feed, stock_set, initial_capital=1_000_000, group_index=group_index, group_num=5)
    my_strategy.info('-' * 100)
    my_strategy.run()
    my_strategy.info("Final portfolio value: $%.2f" % my_strategy.getResult())
    run_time = time.time() - t

    with open('log/result.txt', 'a') as f:
        f.write(
            "group: %d, data preparation time: %.2f, data feed time: %.2f, backtest running time: %.2f, "
            "Final portfolio value: $%.2f\n" % (
                group_index, pre_time, feed_time, run_time, my_strategy.getResult()))

    del feed
    del my_strategy


if __name__ == '__main__':
    my_pool = Pool(3)
    for _ in range(10):
        for i in range(1, 6):
            my_pool.apply_async(main, args=(i,), error_callback=print)

    my_pool.close()
    my_pool.join()

    print('ALL FINISHED!!!!!!!')
