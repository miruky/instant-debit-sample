import json
import os
from decimal import Decimal

import boto3
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.idempotency import (
    DynamoDBPersistenceLayer,
    IdempotencyConfig,
    idempotent,
)
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyAlreadyInProgressError,
    IdempotencyKeyError,
    IdempotencyValidationError,
)

logger = Logger()
metrics = Metrics()
dynamodb = boto3.client("dynamodb")
TABLE = os.environ["ACCOUNTS_TABLE"]

persistence = DynamoDBPersistenceLayer(table_name=os.environ["IDEMPOTENCY_TABLE"])
config = IdempotencyConfig(
    # 同じ Idempotency-Key の再送は、保存済みの応答をそのまま返す（HTTP/2 ではヘッダー名が小文字で届く）
    event_key_jmespath='headers."idempotency-key" || headers."Idempotency-Key"',
    # 同じキーで口座や金額が違う要求は、二重引き落としの疑いとして弾く
    payload_validation_jmespath="powertools_json(body).[accountId, amount]",
    raise_on_no_idempotency_key=True,
    expires_after_seconds=24 * 60 * 60,
)


def response(status, body):
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, ensure_ascii=False)}


@idempotent(config=config, persistence_store=persistence)
def debit(event, context):
    body = json.loads(event["body"])
    account_id = body["accountId"]
    amount = int(body["amount"])
    headers = {k.lower(): v for k, v in event["headers"].items()}
    key = headers["idempotency-key"]
    try:
        result = dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": TABLE,
                        "Item": {"pk": {"S": f"TXN#{key}"}, "accountId": {"S": account_id}, "amount": {"N": str(amount)}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
                {
                    "Update": {
                        "TableName": TABLE,
                        "Key": {"pk": {"S": f"ACCT#{account_id}"}},
                        "UpdateExpression": "SET balance = balance - :amt",
                        "ConditionExpression": "balance >= :amt",
                        "ExpressionAttributeValues": {":amt": {"N": str(amount)}},
                    }
                },
            ]
        )
    except dynamodb.exceptions.TransactionCanceledException as e:
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if reasons and reasons[0] == "ConditionalCheckFailed":
            metrics.add_metric(name="DuplicateDebitBlocked", unit=MetricUnit.Count, value=1)
            return response(409, {"result": "duplicate"})
        metrics.add_metric(name="InsufficientFunds", unit=MetricUnit.Count, value=1)
        return response(409, {"result": "insufficient_funds"})
    metrics.add_metric(name="DebitSucceeded", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name="DebitAmount", unit=MetricUnit.Count, value=amount)
    # 引き落とし後の残高を応答に含める
    balance = int(result["Attributes"]["balance"]["N"])
    return response(200, {"result": "debited", "accountId": account_id, "amount": amount, "balance": balance})


@metrics.log_metrics
@logger.inject_lambda_context
def handler(event, context):
    try:
        return debit(event, context)
    except IdempotencyValidationError:
        # 同じキーなのに中身が違う。クライアントの不具合か改ざんの可能性がある
        metrics.add_metric(name="IdempotencyPayloadMismatch", unit=MetricUnit.Count, value=1)
        logger.warning("idempotency payload mismatch")
        return response(422, {"result": "payload_mismatch"})
    except IdempotencyAlreadyInProgressError:
        return response(409, {"result": "in_progress"})
    except IdempotencyKeyError:
        return response(400, {"result": "missing_idempotency_key"})
