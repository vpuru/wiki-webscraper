#!/usr/bin/env python3
import os
import sys

import aws_cdk as cdk

from stacks.crawl_state_stack import CrawlStateStack

account = os.environ.get("CDK_DEFAULT_ACCOUNT")
region = os.environ.get("CDK_DEFAULT_REGION")

if not account or not region:
    print("Error: CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION must be set.", file=sys.stderr)
    sys.exit(1)

app = cdk.App()
CrawlStateStack(
    app,
    "CrawlStateStack",
    env=cdk.Environment(account=account, region=region),
)
app.synth()
