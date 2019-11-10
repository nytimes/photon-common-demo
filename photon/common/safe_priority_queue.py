import queue
from heapq import heappop, heappush
from typing import Any, List, NamedTuple, TYPE_CHECKING

# https://stackoverflow.com/questions/45414066/mypy-how-to-define-a-generic-subclass
if TYPE_CHECKING:
    Queue = queue.Queue
else:

    class FakeGenericMeta(type):
        def __getitem__(self, item):
            return self

    class Queue(queue.Queue, metaclass=FakeGenericMeta):
        pass


class ItemNT(NamedTuple):
    priority: float
    data: Any


ItemNT.priority.__doc__ = "float (field 0): The priority of the item."
ItemNT.data.__doc__ = "Any (field 1): The item data."


class SafePriorityQueue(Queue[ItemNT]):
    """
    A "safe" Priority Queue that insures each item gets popped eventually.

    Each time an item is retrieved the priorities of the remaining items are decreased
    by the  "decay" factor; eventually each item will make it to the head of the queue
    (lowest priority) so no items should "starve" - possibly experiment to get the right
    decay factor, though the default should work ok.

    """

    def __init__(self, maxsize: int = 0, decay: float = 0.999) -> None:
        super().__init__(maxsize=maxsize)
        self.decay = decay

    def _init(self, maxsize: int) -> None:
        self.spqueue: List[ItemNT] = []

    def _qsize(self) -> int:
        return len(self.spqueue)

    def _put(self, itemnt: ItemNT) -> None:
        heappush(self.spqueue, itemnt)

    def _get(self) -> ItemNT:
        head_itemnt = heappop(self.spqueue)

        for i, itemnt in enumerate(self.spqueue):
            self.spqueue[i] = ItemNT(  # relative order is unchanged
                priority=self.decay * itemnt.priority, data=itemnt.data
            )

        return head_itemnt
