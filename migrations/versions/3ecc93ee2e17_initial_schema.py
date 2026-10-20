"""初始化数据库表结构。

迁移版本：3ecc93ee2e17
前置版本：无
创建时间：2026-03-11 13:03:24.766512
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# Alembic 用于识别迁移版本的编号。
revision: str = '3ecc93ee2e17'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """创建活动、用户、票种和票据表，以及对应索引和约束。"""
    # 以下操作由 Alembic 自动生成，修改模型后仍需人工检查生成结果。
    op.create_table('events',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('title', sa.String(length=100), nullable=False),
    sa.Column('date', sa.DateTime(timezone=True), nullable=False),
    sa.Column('country', sa.String(length=100), nullable=False),
    sa.Column('city', sa.String(length=100), nullable=False),
    sa.Column('street_address', sa.String(length=100), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_events'))
    )
    op.create_index(op.f('ix_events_title'), 'events', ['title'], unique=False)
    op.create_table('users',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('email', sa.String(length=254), nullable=False),
    sa.Column('hashed_password', sa.String(length=255), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('is_superuser', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_users'))
    )
    op.create_index(op.f('ix_users_email'), 'users', ['email'], unique=True)
    op.create_table('ticket_types',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=50), nullable=False),
    sa.Column('price', sa.Integer(), nullable=False),
    sa.Column('tickets_quantity', sa.Integer(), nullable=False),
    sa.Column('tickets_sold', sa.Integer(), nullable=False),
    sa.Column('event_id', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('price >= 0', name=op.f('ck_ticket_types_check_ticket_price')),
    sa.CheckConstraint('tickets_quantity > 0', name=op.f('ck_ticket_types_check_ticket_quantity')),
    sa.CheckConstraint('tickets_quantity >= tickets_sold', name=op.f('ck_ticket_types_check_sold_limit')),
    sa.CheckConstraint('tickets_sold >= 0', name=op.f('ck_ticket_types_check_ticket_sold')),
    sa.ForeignKeyConstraint(['event_id'], ['events.id'], name=op.f('fk_ticket_types_event_id_events'), ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_ticket_types'))
    )
    op.create_table('tickets',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('ticket_type_id', sa.Integer(), nullable=False),
    sa.Column('owner_id', sa.Integer(), nullable=False),
    sa.Column('status', sa.Enum('RESERVED', 'SOLD', 'CANCELED', name='ticketstatus'), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['owner_id'], ['users.id'], name=op.f('fk_tickets_owner_id_users'), ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['ticket_type_id'], ['ticket_types.id'], name=op.f('fk_tickets_ticket_type_id_ticket_types'), ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_tickets'))
    )
    op.create_index(op.f('ix_tickets_status'), 'tickets', ['status'], unique=False)
    # Alembic 自动生成的操作结束。


def downgrade() -> None:
    """按依赖顺序撤销本次迁移创建的索引和表。"""
    # 以下操作由 Alembic 自动生成，修改模型后仍需人工检查生成结果。
    op.drop_index(op.f('ix_tickets_status'), table_name='tickets')
    op.drop_table('tickets')
    op.drop_table('ticket_types')
    op.drop_index(op.f('ix_users_email'), table_name='users')
    op.drop_table('users')
    op.drop_index(op.f('ix_events_title'), table_name='events')
    op.drop_table('events')
    # Alembic 自动生成的操作结束。
