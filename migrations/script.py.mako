"""${message}

迁移版本： ${up_revision}
前置版本： ${down_revision | comma,n}
创建时间： ${create_date}

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# Alembic 用于识别迁移版本的编号。
revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    """执行本次数据库结构升级。"""
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """撤销本次数据库结构变更。"""
    ${downgrades if downgrades else "pass"}
