#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "cost_model_results.txt"
HOURS_PER_MONTH = 730
BUDGET_CEILING_USD = 50_000


@dataclass(frozen=True)
class LineItem:
    name: str
    label: str
    monthly_units: float
    unit: str
    unit_price_usd: float
    note: str

    @property
    def monthly_cost_usd(self) -> float:
        return self.monthly_units * self.unit_price_usd


def build_line_items() -> list[LineItem]:
    # EC2 unit prices are deliberately conservative placeholders until the exact
    # AWS Pricing Calculator quote is exported for the target region and sizes.
    return [
        LineItem("ingestion_ec2", "Assumed", 3 * HOURS_PER_MONTH, "instance-hour", 0.20, "3 Node.js ingestion nodes"),
        LineItem("writer_ec2", "Assumed", 3 * HOURS_PER_MONTH, "instance-hour", 0.20, "3 Python writer workers"),
        LineItem("redpanda_ec2", "Assumed", 3 * HOURS_PER_MONTH, "instance-hour", 1.00, "3 Redpanda brokers"),
        LineItem("clickhouse_ec2", "Assumed", 3 * HOURS_PER_MONTH, "instance-hour", 2.00, "3 ClickHouse hot-query nodes"),
        LineItem("ops_ec2", "Assumed", 2 * HOURS_PER_MONTH, "instance-hour", 0.10, "bastion and small ops workers"),
        LineItem("redpanda_gp3_storage", "Benchmarked", 6_000, "GB-month", 0.08, "gp3 storage, AWS EBS public price example"),
        LineItem("clickhouse_gp3_storage", "Benchmarked", 24_000, "GB-month", 0.08, "gp3 storage, AWS EBS public price example"),
        LineItem("s3_archive_storage", "Benchmarked", 9_500, "GB-month", 0.023, "S3 Standard first-tier public price"),
        LineItem("nat_gateway_hours", "Benchmarked", 3 * HOURS_PER_MONTH, "gateway-hour", 0.045, "3-AZ NAT gateway hourly charge"),
        LineItem("nat_gateway_processing", "Benchmarked", 5_000, "GB", 0.045, "NAT data processing for exports/admin egress"),
        LineItem("internet_data_transfer", "Assumed", 5_000, "GB", 0.09, "warehouse/customer export transfer allowance"),
        LineItem("alb_cloudwatch_misc", "Assumed", 1, "monthly bundle", 1_000.00, "ALB, CloudWatch, logs, dashboards, alarms"),
        LineItem("support_ops_buffer", "Assumed", 1, "monthly bundle", 2_000.00, "support, snapshots, small managed services"),
    ]


def render() -> str:
    lines = build_line_items()
    subtotal = sum(item.monthly_cost_usd for item in lines)
    contingency = subtotal
    total = subtotal + contingency
    budget_pass = int(total < BUDGET_CEILING_USD)
    margin = BUDGET_CEILING_USD - total

    output = [
        "# cost_model_results.txt",
        "",
        "- [Assumed] region = us-east-1",
        "- [Assumed] hours_per_month = 730",
        "- [Assumed] brief_current_events_per_day = 50000000",
        "- [Estimated] events_per_month = 1500000000",
        "- [Assumed] average_event_payload_kb = 1.5",
        "- [Estimated] raw_ingest_gb_per_month = 2250",
        "- [Assumed] s3_archive_retention_months = 12",
        "- [Assumed] budget_ceiling_usd = 50000",
        "",
        "## Line Items",
        "",
    ]
    for item in lines:
        output.append(
            f"- [{item.label}] {item.name}_monthly_usd = {item.monthly_cost_usd:.2f} "
            f"({item.monthly_units:g} {item.unit} * ${item.unit_price_usd:g}; {item.note})"
        )
    output.extend(
        [
            "",
            "## Total",
            "",
            f"- [Estimated] subtotal_monthly_usd = {subtotal:.2f}",
            f"- [Assumed] contingency_percent = 100",
            f"- [Estimated] contingency_monthly_usd = {contingency:.2f}",
            f"- [Estimated] estimated_monthly_total_usd = {total:.2f}",
            f"- [Estimated] budget_margin_usd = {margin:.2f}",
            f"- [Estimated] budget_pass = {budget_pass}",
            "",
            "## Scope",
            "",
            "- [Assumed] This model excludes vendor contract discounts, support plan changes, taxes, and committed-use discounts.",
            "- [Assumed] EC2 instance-hour prices are conservative placeholders until an AWS Pricing Calculator export is attached.",
            "- [Benchmarked] EBS gp3, S3 Standard, and NAT gateway unit prices are from public AWS pricing pages checked on 2026-06-10.",
        ]
    )
    return "\n".join(output) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Render or verify the Engineer-004 AWS cost model")
    parser.add_argument("--write", action="store_true", help="write cost_model_results.txt")
    parser.add_argument("--check", action="store_true", help="fail if cost_model_results.txt is stale")
    args = parser.parse_args()

    rendered = render()
    if args.write:
        RESULTS.write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0
    if args.check:
        current = RESULTS.read_text(encoding="utf-8")
        if current != rendered:
            print("cost_model_results.txt is stale; run: python cost_model.py --write")
            return 1
        print("[PASS] cost_model_results.txt is current")
        return 0
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
