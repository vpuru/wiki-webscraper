from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct


class CrawlStateStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table = dynamodb.TableV2(
            self,
            "WikiCrawlState",
            table_name="WikiCrawlState",
            partition_key=dynamodb.Attribute(
                name="url_hash",
                type=dynamodb.AttributeType.STRING,
            ),
            billing=dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.DESTROY,
            global_secondary_indexes=[
                dynamodb.GlobalSecondaryIndexPropsV2(
                    index_name="StatusIndex",
                    partition_key=dynamodb.Attribute(
                        name="status",
                        type=dynamodb.AttributeType.STRING,
                    ),
                    sort_key=dynamodb.Attribute(
                        name="fetched_at",
                        type=dynamodb.AttributeType.STRING,
                    ),
                ),
            ],
        )
