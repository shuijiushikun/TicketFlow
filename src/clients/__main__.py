"""运行 python -m src.clients，提交秒杀申请或继续查询已有申请。"""

import argparse
import asyncio
import os
from math import isfinite

import httpx

from src.clients.seckill import (
    RetryExhaustedError,
    SeckillAttempt,
    SeckillClient,
    SeckillWaitTimeout,
)


def positive_seconds(value: str) -> float:
    """先验证等待预算，再允许命令发起申请。"""
    seconds = float(value)
    if not isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("等待时间必须为有限的正数")
    return seconds


def parser() -> argparse.ArgumentParser:
    """提交必须显式提供幂等键，避免重启命令后误建另一份申请。"""
    root = argparse.ArgumentParser(
        description="秒杀客户端：同键有限重试，受理后自动查询结果"
    )
    root.add_argument("--base-url", default="http://127.0.0.1:18000")
    root.add_argument("--api-prefix", default="/api/v1")
    root.add_argument("--wait-seconds", type=positive_seconds, default=70.0)
    commands = root.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="提交一次购票申请，受理后等待结果")
    submit.add_argument("--ticket-type-id", type=int, required=True)
    submit.add_argument("--idempotency-key", required=True)
    result = commands.add_parser("result", help="只查询已有申请，绝不重复提交")
    result.add_argument("--request-id", type=int, required=True)
    return root


async def run(args: argparse.Namespace, token: str) -> int:
    """认证令牌只从环境读取，先输出可恢复标识再进行网络操作。"""
    async with httpx.AsyncClient(
        base_url=args.base_url,
        headers={"Authorization": f"Bearer {token}"},
        trust_env=False,
        timeout=5.0,
    ) as http:
        client = SeckillClient(http, args.api_prefix)
        if args.command == "submit":
            attempt = SeckillAttempt(args.ticket_type_id, args.idempotency_key)
            print(f"本次幂等键：{attempt.idempotency_key}", flush=True)
            receipt = await client.submit(attempt)
            request_id = receipt.id
            print(f"已受理，申请编号：{request_id}；接下来只查询结果。", flush=True)
        else:
            request_id = args.request_id
        result = await client.wait_result(request_id, timeout=args.wait_seconds)
        messages = {
            "SUCCEEDED": "预约成功，可使用票据编号继续支付",
            "SOLD_OUT": "票已售罄，本次申请处理结束",
            "EXPIRED": "申请排队期限已到，本次申请处理结束",
            "REJECTED": "申请已被拒绝，请核对票种状态",
        }
        print(f"处理结果：{messages[result.status]}")
        print(result.model_dump_json(indent=2))
        return 0


def main() -> int:
    """执行一次有限客户端操作；停止等待不会撤销后台已受理的申请。"""
    root = parser()
    args = root.parse_args()
    token = os.environ.get("TICKETFLOW_ACCESS_TOKEN", "").strip()
    if not token:
        root.error("请先设置 TICKETFLOW_ACCESS_TOKEN 环境变量为登录获得的令牌")
    try:
        return asyncio.run(run(args, token))
    except (RetryExhaustedError, SeckillWaitTimeout, ValueError) as error:
        print(str(error))
        return 2
    except httpx.HTTPError as error:
        if isinstance(error, httpx.HTTPStatusError):
            print(f"接口返回 HTTP {error.response.status_code}，本次操作已停止。")
        else:
            print("客户端连接失败，请保留原幂等键或申请编号后再试。")
        return 2
    except KeyboardInterrupt:
        print("已停止等待；请保留原幂等键，已有申请编号可用 result 命令继续查询。")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
