"""empty message

Revision ID: ae96eec1a4e9
Revises: b2100f98b32e
Create Date: 2018-08-02 16:05:17.598427

"""

# revision identifiers, used by Alembic.
revision = 'ae96eec1a4e9'
down_revision = 'b2100f98b32e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cosmos_columns', sa.Column('is_dttm', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cosmos_columns', 'is_dttm')
    # ### end Alembic commands ###
