import uuid
from datetime import UTC, datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field
from sqlalchemy import select

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_domain.trading_mode import TradingMode

router = APIRouter(prefix="/tenants", tags=["tenants"])


class TenantCreate(BaseModel):
    name: str = Field(min_length=1, max_length=256)
    default_trading_mode: TradingMode = TradingMode.PAPER


class TenantOut(BaseModel):
    id: uuid.UUID
    name: str
    created_at: datetime
    default_trading_mode: TradingMode


def _tenant_out(row: Tenant) -> TenantOut:
    return TenantOut(
        id=row.id,
        name=row.name,
        created_at=row.created_at,
        default_trading_mode=TradingMode(row.default_trading_mode),
    )


@router.get("", response_model=list[TenantOut])
def list_tenants(_admin: RootAdminUser, db: DbSession) -> list[TenantOut]:
    rows = list(db.scalars(select(Tenant).order_by(Tenant.created_at.desc())).all())
    return [_tenant_out(r) for r in rows]


@router.post("", response_model=TenantOut, status_code=201)
def create_tenant(body: TenantCreate, _admin: RootAdminUser, db: DbSession) -> TenantOut:
    now = datetime.now(UTC)
    row = Tenant(
        id=uuid.uuid4(),
        name=body.name,
        created_at=now,
        default_trading_mode=body.default_trading_mode.value,
    )
    db.add(row)
    db.flush()
    db.add(
        TenantIntegration(
            tenant_id=row.id,
            coinbase_connected=False,
            updated_at=now,
        )
    )
    db.flush()
    return _tenant_out(row)
