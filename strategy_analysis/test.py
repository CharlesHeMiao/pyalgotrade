import math

from pyalgotrade import strategy
from pyalgotrade.technical import ma
from pyalgotrade.technical import cross
from pyalgotrade.broker.backtesting import Broker, TradePercentageWithMin
from pyalgotrade.barfeed.tusharefeed import Feed


class SMACrossOver(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, smaPeriodShort, smaPeriodLong):
        commission = TradePercentageWithMin(0.0003, 5)
        broker = Broker(100000, feed, commission)
        super(SMACrossOver, self).__init__(feed, broker)
        self.__instrument = instrument
        self.__position = None
        self.__prices = feed[instrument].getPriceDataSeries()
        self.__sma_short = ma.SMA(self.__prices, smaPeriodShort)
        self.__sma_long = ma.SMA(self.__prices, smaPeriodLong)

    def getSMAShort(self):
        return self.__sma_short

    def getSMALong(self):
        return self.__sma_long

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        price = execInfo.getPrice()
        qty = execInfo.getQuantity()
        fee = execInfo.getCommission()
        self.info('Buy at ￥%.2f, %d shares, commission ￥%.2f' % (price, qty, fee))
        broker = self.getBroker()
        cash = broker.getCash()
        stock = price * qty
        equity = cash + stock
        self.info('cash ￥%.2f, stock ￥%.2f, total equity ￥%.2f' % (cash, stock, equity))

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        price = execInfo.getPrice()
        qty = execInfo.getQuantity()
        fee = execInfo.getCommission()
        pnl = position.getPnL(price)
        self.info('Sell at ￥%.2f, %d shares, commission ￥%.2f, profit or loss ￥%.2f' % (price, qty, fee, pnl))
        broker = self.getBroker()
        cash = broker.getCash()
        equity = broker.getEquity()
        stock = equity - cash
        self.info('cash ￥%.2f, stock ￥%.2f, total equity ￥%.2f' % (cash, stock, equity))

        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onBars(self, bars):
        # If a position was not opened, check if we should enter a long position.
        if self.__position is None:
            if cross.cross_above(self.__sma_short, self.__sma_long) > 0:
                shares = int(self.getBroker().getCash() * 0.9 / bars[self.__instrument].getPrice())
                shares = math.floor(shares / 100) * 100

                if shares > 0:
                    # Enter a buy market order. The order is good till canceled.
                    self.__position = self.enterLong(self.__instrument, shares, True)
        # Check if we have to exit the position.
        elif not self.__position.exitActive() and cross.cross_below(self.__sma_short, self.__sma_long) > 0:
            self.__position.exitMarket()


def main():
    # Load the bar feed from the CSV file
    feed = Feed()
    feed.addBarsFromCSV('000001.SZ', 'C:/developer/tmp.csv')

    # Evaluate the strategy with the feed's bars.
    myStrategy = SMACrossOver(feed, '000001.SZ', 5, 100)

    # Run the strategy.
    myStrategy.run()
    myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())


if __name__ == '__main__':
    main()
