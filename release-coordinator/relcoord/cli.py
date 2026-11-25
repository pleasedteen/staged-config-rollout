from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence

from . import client
from .status import build_status, render_status
from .storage import Store
from .util import DEFAULT_HOME, RelCoordError, pretty_json


def _home_default() -> str:
    return os.environ.get("RELCOORD_HOME", str(DEFAULT_HOME))


def _split_batch(value: str) -> list[str]:
    names = [x.strip() for x in value.split(",") if x.strip()]
    if not names:
        raise argparse.ArgumentTypeError("batch must contain at least one service name")
    return names


def cmd_init(args: argparse.Namespace) -> int:
    store = Store(args.home)
    names = None
    if args.services:
        names = [x.strip() for x in args.services.split(",") if x.strip()]
    ports = None
    if args.ports:
        ports = [int(x.strip()) for x in args.ports.split(",") if x.strip()]
    cluster = store.initialize_home(
        names=names,
        ports=ports,
        base_port=args.base_port,
        host=args.host,
        initial_state_path=args.initial_state,
    )
    print(f"initialized home: {store.home}")
    print(f"cluster file: {store.cluster_path}")
    for s in cluster["services"]:
        print(f"  {s['name']:8} {s['url']}  state={s['state_path']}")
    return 0


def cmd_submit(args: argparse.Namespace) -> int:
    batches = [_split_batch(b) for b in args.batch]
    store = Store(args.home)
    release_id = store.submit_release(
        config_path=args.config,
        batches=batches,
        group=args.group,
        note=args.note or "",
    )
    print(f"submitted release {release_id}")
    print(f"target config: {args.config}")
    for i, batch in enumerate(batches, 1):
        print(f"  batch {i}: {', '.join(batch)}")
    print("query progress with: relctl status")
    return 0


def cmd_pause(args: argparse.Namespace) -> int:
    rid, changed = Store(args.home).pause(args.group, args.reason or "")
    print(("pause accepted" if changed else "pause already accepted") + f": {rid}")
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    rid, changed = Store(args.home).resume(args.group)
    print(("resumed" if changed else "already running") + f": {rid}")
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    rid, changed = Store(args.home).request_rollback(args.group, args.reason or "")
    print(("rollback accepted" if changed else "rollback already accepted/complete") + f": {rid}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    data = build_status(args.home, live=not args.no_live)
    if args.json:
        print(pretty_json(data))
    else:
        print(render_status(data))
    return 0


def cmd_faults(args: argparse.Namespace) -> int:
    store = Store(args.home)
    svc = store.get_service(args.service)
    if args.reset:
        patch = {"reset": True}
    else:
        if not args.set:
            raise RelCoordError("pass --set JSON or --reset")
        patch = json.loads(args.set)
        if not isinstance(patch, dict):
            raise RelCoordError("--set must be a JSON object")
    result = client.set_faults(svc["base_url"], patch)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="relctl",
        description="Local configuration release coordinator",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--home", default=_home_default(), help="runtime home (default: ./runtime or RELCOORD_HOME)")

    p_init = sub.add_parser("init", parents=[common], help="initialize SQLite DB, cluster and initial state")
    p_init.add_argument("--services", default=None, help="comma-separated service names")
    p_init.add_argument("--ports", default=None, help="comma-separated ports matching services")
    p_init.add_argument("--base-port", type=int, default=9301)
    p_init.add_argument("--host", default="127.0.0.1")
    p_init.add_argument("--initial-state", default=None)
    p_init.set_defaults(func=cmd_init)

    p_submit = sub.add_parser("submit", parents=[common], help="submit target config and ordered batches")
    p_submit.add_argument("config", help="JSON file containing version and content")
    p_submit.add_argument("--batch", action="append", required=True, help="comma-separated service names; repeat per batch")
    p_submit.add_argument("--group", default="core")
    p_submit.add_argument("--note", default="")
    p_submit.set_defaults(func=cmd_submit)

    p_pause = sub.add_parser("pause", parents=[common], help="accept pause after current batch boundary")
    p_pause.add_argument("--group", default="core")
    p_pause.add_argument("--reason", default="")
    p_pause.set_defaults(func=cmd_pause)

    p_resume = sub.add_parser("resume", parents=[common], help="resume and allow later batches")
    p_resume.add_argument("--group", default="core")
    p_resume.set_defaults(func=cmd_resume)

    p_rb = sub.add_parser("rollback", parents=[common], help="roll back started services to their own baselines")
    p_rb.add_argument("--group", default="core")
    p_rb.add_argument("--reason", default="")
    p_rb.set_defaults(func=cmd_rollback)

    p_status = sub.add_parser("status", parents=[common], help="query progress and live effective configuration")
    p_status.add_argument("--json", action="store_true")
    p_status.add_argument("--no-live", action="store_true", help="do not contact services; use SQLite state")
    p_status.set_defaults(func=cmd_status)

    p_fault = sub.add_parser("faults", parents=[common], help="configure mock-service fault injection")
    p_fault.add_argument("--service", required=True)
    p_fault.add_argument("--set", default=None, help="JSON fault patch")
    p_fault.add_argument("--reset", action="store_true")
    p_fault.set_defaults(func=cmd_faults)

    sub.add_parser("service", help="run a mock service; run `relctl service --help` for options")
    sub.add_parser("coordinator", help="run coordinator; run `relctl coordinator --help` for options")
    sub.add_parser("demo", help="run the self-checking fault demo")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)

    # These subcommands own their more detailed argument parsers.
    if args_list and args_list[0] == "service":
        from .service import main as service_main

        return service_main(args_list[1:])
    if args_list and args_list[0] == "coordinator":
        from .coordinator import main as coordinator_main

        return coordinator_main(args_list[1:])
    if args_list and args_list[0] == "demo":
        from .demo import main as demo_main

        return demo_main(args_list[1:])

    parser = build_parser()
    args = parser.parse_args(args_list)
    try:
        return int(args.func(args) or 0)
    except RelCoordError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except (json.JSONDecodeError, ValueError) as e:
        print(f"error: invalid input: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
