"""streamhouse CLI: produce | stream | maintain | bench."""

import argparse
import json
from datetime import date

from streamhouse.config import Settings


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="streamhouse")
    sub = p.add_subparsers(dest="cmd", required=True)

    prod = sub.add_parser("produce", help="generate synthetic telemetry and publish it to Kafka")
    for name, typ, default in (("--seed", int, 42), ("--vehicles", int, 30), ("--hours", float, 3.0)):
        prod.add_argument(name, type=typ, default=default)

    st = sub.add_parser("stream", help="run the bronze/silver/gold streaming queries")
    st.add_argument("--files", help="replay a directory of JSON-lines files instead of reading Kafka")
    st.add_argument("--once", action="store_true", help="availableNow: drain what is there, then stop")

    mt = sub.add_parser("maintain", help="batch maintenance (what the Airflow DAG runs)")
    mt.add_argument("step", choices=["gold", "dq", "compact", "expire"])
    mt.add_argument("--day", type=date.fromisoformat, action="append", help="gold: rebuild only these days")

    bn = sub.add_parser("bench", help="end-to-end benchmark + correctness eval vs ground truth")
    bn.add_argument("--source", choices=["files", "kafka"], default="files")
    for name, typ, default in (
        ("--seed", int, 42),
        ("--vehicles", int, 100),
        ("--hours", float, 4.0),
        ("--batch-size", int, 500),
    ):
        bn.add_argument(name, type=typ, default=default)

    args = p.parse_args(argv)
    s = Settings()

    if args.cmd == "produce":
        from streamhouse.generate import generate
        from streamhouse.producer import make_producer, publish

        arrivals = generate(seed=args.seed, vehicles=args.vehicles, hours=args.hours)
        stats = publish(make_producer(s.kafka_bootstrap), (a.payload for a in arrivals), s.topic, s.dlq_topic)
        print(json.dumps(stats))
    elif args.cmd == "bench":
        from streamhouse import bench

        print(
            json.dumps(
                bench.run(args.source, args.seed, args.vehicles, args.hours, args.batch_size), indent=2, default=str
            )
        )
    else:
        from streamhouse import maintenance, streaming, tables
        from streamhouse.spark import build_spark

        spark = build_spark(s)
        tables.ensure_tables(spark)
        if args.cmd == "stream":
            raw = streaming.file_source(spark, args.files) if args.files else streaming.kafka_source(spark, s)
            streaming.run_until_drained(streaming.start(spark, raw, s.checkpoints, available_now=args.once))
        elif args.step == "gold":
            print(maintenance.rebuild_gold_daily(spark, args.day))
        elif args.step == "dq":
            print(json.dumps(maintenance.dq_checks(spark)))
        else:
            fn = maintenance.compact if args.step == "compact" else maintenance.expire_snapshots
            print(json.dumps([fn(spark, t) for t in tables.ALL]))


if __name__ == "__main__":
    main()
