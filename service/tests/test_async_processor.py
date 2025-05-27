from typing import Optional
from unittest import TestCase

from agent.utils.queue_async_processor import QueueAsyncProcessor


class AsyncProcessorTets(TestCase):
    def test_exception_handling(self):
        # failing to execute an operation shouldn't stop the processor
        processor: Optional[QueueAsyncProcessor[str]] = None
        invocations = []

        def handler(param: str):
            invocations.append(param)
            if param == "fail":
                raise Exception("test")
            elif param == "stop":
                if processor:
                    processor._running = False
            else:
                pass

        processor = QueueAsyncProcessor("test", handler, 1)
        processor.schedule("fail")
        processor.schedule("ok")
        processor.schedule("stop")
        processor._running = True
        processor._run(0)

        self.assertEqual(3, len(invocations))
