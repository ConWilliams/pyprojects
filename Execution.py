import Datetime
import Queue

from abc import ABCMeta, abstractmethod

from event import FillEvent, OrderEvent

class ExecutionHandler(object):
    """
    The ExecutionHandler abstract class handles the interaction between a set of order
    objects generated by a Portfolio and the ultimate set of Fill objects that acctually
    occur in the market.

    The handlers can be used to subclass simulated brokerages or live brokerages, with
    identical interfaces.  This allows strategies to be backtested similar to live trading.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self, event):
        raise NotImplementedError("should implement execute_order()")

    class SimulatedExecutionHandler(ExecutionHandler):
        def __init__(self, events):
            self.events = events

        def execute_order(self, event):
            if event.type == 'ORDER':
                fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol,
                                        'ARCA', event.quantity, event.direction, None)
                self.events.put(fill_event)

        
