import json
import sys
import logging
import pandas as pd

from enum import Enum

from executor_service.mq import PikaChannel
from executor_service.schemas.queries import QueryPublishIn
from executor_service.endpoints.queries import select_query_result
from executor_service.database import db_session, clickhouse_client
from executor_service.settings import settings


logger = logging.getLogger(__name__)


class QueryRunningPublishStatus(Enum):
    PUBLISHING = 'publishing'
    PUBLISHED = 'published'
    ERROR = 'error'


async def publish_request(body: bytes, mq: PikaChannel):
    query_publish_in = QueryPublishIn.parse_raw(body)
    async with db_session() as session:
        rows = await select_query_result(
            query_publish_in.guid, sys.maxsize, 0, {'identity_id': query_publish_in.identity_id}, session
        )

        df = pd.DataFrame(rows)

        clickhouse_client.create_publish_table(query_publish_in.publish_name, df)

        try:
            clickhouse_client.insert_dataframe(query_publish_in.publish_name, df)
            status = QueryRunningPublishStatus.PUBLISHED.value
        except Exception as err:
            logger.error(err)
            status = QueryRunningPublishStatus.ERROR.value
        finally:
            await mq.basic_publish(
                settings.publish_exchange,
                'result',
                json.dumps({'guid': query_publish_in.guid, 'status': status})
            )
