import json
import os
import uuid

import boto3
from aws_lambda_powertools import Logger

logger = Logger()
sqs = boto3.client("sqs")
QUEUE_URL = os.environ["WORK_QUEUE_URL"]


@logger.inject_lambda_context
def handler(event, context):
    # 夜間バッチの対象を作業キューへ積む。poison の件数だけ処理できないメッセージを混ぜる
    total = int(event.get("items", 20))
    poison = int(event.get("poison", 0))
    entries = [{"Id": str(i), "MessageBody": json.dumps({"itemId": str(uuid.uuid4()), "poison": i < poison})} for i in range(total)]
    for start in range(0, len(entries), 10):
        sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries[start:start + 10])
    logger.info("queued", extra={"items": total, "poison": poison})
    return {"queued": total}
