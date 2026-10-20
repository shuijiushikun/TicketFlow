"""持久化交易幂等及副作用待办，并增加库存同步版本。"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "71c7d5e4b903"
down_revision: str | Sequence[str] | None = "3ecc93ee2e17"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """保留现有数据，新增库存版本、持久化结果及到期扫描索引。"""
    op.add_column(
        "ticket_types",
        sa.Column(
            "inventory_version", sa.BigInteger(), server_default="0", nullable=False
        ),
    )
    op.create_check_constraint(
        op.f("ck_ticket_types_check_inventory_version"),
        "ticket_types",
        "inventory_version >= 0",
    )
    op.create_index("ix_tickets_expiry", "tickets", ["status", "created_at", "id"])
    op.create_table(
        "idempotency_requests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=100), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("owner_token", sa.String(length=32), nullable=False),
        sa.Column("response", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_idempotency_requests")),
        sa.UniqueConstraint(
            "user_id", "action", "idempotency_key", name="uq_idempotency_request_scope"
        ),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempts >= 0", name=op.f("ck_outbox_events_check_outbox_attempts")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_events")),
    )
    op.create_index("ix_outbox_events_due", "outbox_events", ["next_attempt_at", "id"])


def downgrade() -> None:
    """删除待办表；执行前需由操作者确认未投递待办的处置。"""
    op.drop_index("ix_outbox_events_due", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("idempotency_requests")
    op.drop_index("ix_tickets_expiry", table_name="tickets")
    op.drop_constraint(
        op.f("ck_ticket_types_check_inventory_version"), "ticket_types", type_="check"
    )
    op.drop_column("ticket_types", "inventory_version")
