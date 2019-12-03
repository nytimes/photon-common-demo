import queue
from heapq import heappop, heappush
from dataclasses import dataclass, field
from typing import Any, List, TYPE_CHECKING

# https://stackoverflow.com/questions/45414066/mypy-how-to-define-a-generic-subclass
if TYPE_CHECKING:
    Queue = queue.Queue
else:

    class FakeGenericMeta(type):
        def __getitem__(self, item):
            return self

    class Queue(queue.Queue, metaclass=FakeGenericMeta):
        pass


# https://docs.python.org/3/library/queue.html
@dataclass(order=True)
class ItemDC:
    score: float
    data: Any = field(compare=False)


class SafePriorityQueue(Queue[ItemDC]):
    """
    A "safe" Priority Queue that insures each item gets popped eventually.

    Each time an item is retrieved the scores of the remaining items are decreased
    by the  "decay" factor; eventually each item will make it to the head of the queue
    (lowest score, highest priority) so no items should "starve" - possibly experiment
    to get the right decay factor, although the default should work ok.

    """

    def __init__(self, maxsize: int = 0, decay: float = 0.99) -> None:
        super().__init__(maxsize=maxsize)
        self._decay = decay

    def _init(self, maxsize: int) -> None:
        self.spqueue: List[ItemDC] = []

    def _qsize(self) -> int:
        return len(self.spqueue)

    def _put(self, itemdc: ItemDC) -> None:
        heappush(self.spqueue, itemdc)  # the heap will rearrange to maintain order

    def _get(self) -> ItemDC:
        head_itemdc = heappop(self.spqueue)  # ditto

        # decay the score of each item - creep toward the head (lowest score)
        self.spqueue = [  # no need to rearrange the heap - relative order is maintained
            ItemDC(score=self._decay * itemdc.score, data=itemdc.data)
            for itemdc in self.spqueue
        ]

        return head_itemdc
