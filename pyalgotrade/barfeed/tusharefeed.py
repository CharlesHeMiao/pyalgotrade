"""
.. moduleauthor:: Charles He <charles_he@berkeley.edu>
"""

from pyalgotrade.barfeed import csvfeed
from pyalgotrade import bar


class Feed(csvfeed.GenericBarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from tushare.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=None):
        if frequency not in [bar.Frequency.DAY, bar.Frequency.WEEK]:
            raise Exception("Invalid frequency")

        super(Feed, self).__init__(frequency, timezone, maxLen)

        self.setDateTimeFormat("%Y%m%d")
        self.setColumnName("datetime", "trade_date")
        self.setColumnName("open", "open")
        self.setColumnName("high", "high")
        self.setColumnName("low", "low")
        self.setColumnName("close", "close")
        self.setColumnName("volume", "vol")
