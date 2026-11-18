import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response

logger = Logger()
processor = BatchProcessor(event_type=EventType.SQS)


def record_handler(record):
    item = json.loads(record.body)
    if item.get("poison"):
        # 何度受け取っても処理できないメッセージ。受信回数の上限でDLQへ移る
        raise ValueError(f"cannot process {item['itemId']}")
    return item["itemId"]


@logger.inject_lambda_context
def handler(event, context):
    return process_partial_response(event=event, record_handler=record_handler, processor=processor, context=context)
