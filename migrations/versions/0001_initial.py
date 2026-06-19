"""initial transactions table

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "transactions",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 8), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("amount_usd", sa.Numeric(18, 8), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_transactions_user_id_timestamp",
        "transactions",
        ["user_id", "timestamp"],
    )


def downgrade():
    op.drop_index("ix_transactions_user_id_timestamp", table_name="transactions")
    op.drop_table("transactions")
