"""增加秒杀票种标记和可恢复的抢票申请。"""

import sqlalchemy as sa
from alembic import op

revision = "8a4d2f1c6e90"
down_revision = "71c7d5e4b903"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """原票种默认保持普通预约，新增独立申请表。"""
    op.add_column(
        "ticket_types",
        sa.Column(
            "is_seckill", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
    )
    op.create_table(
        "seckill_requests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "owner_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "ticket_type_id",
            sa.Integer(),
            sa.ForeignKey("ticket_types.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), server_default="QUEUED", nullable=False),
        sa.Column("admission_token", sa.String(32), nullable=False, unique=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_dispatch_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "ticket_id",
            sa.Integer(),
            sa.ForeignKey("tickets.id", ondelete="RESTRICT"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED','SUCCEEDED','SOLD_OUT','EXPIRED','REJECTED')",
            name=op.f("ck_seckill_requests_seckill_status"),
        ),
        sa.CheckConstraint(
            "(status = 'SUCCEEDED' AND ticket_id IS NOT NULL) OR "
            "(status <> 'SUCCEEDED' AND ticket_id IS NULL)",
            name=op.f("ck_seckill_requests_seckill_result"),
        ),
    )
    op.create_index(
        "ix_seckill_requests_recovery",
        "seckill_requests",
        ["status", "next_dispatch_at", "id"],
    )
    op.create_index("ix_seckill_requests_owner", "seckill_requests", ["owner_id", "id"])


def downgrade() -> None:
    """回退前应先停止接收并处理完抢票申请；保留实际票据。"""
    op.drop_index("ix_seckill_requests_owner", table_name="seckill_requests")
    op.drop_index("ix_seckill_requests_recovery", table_name="seckill_requests")
    op.drop_table("seckill_requests")
    op.drop_column("ticket_types", "is_seckill")
