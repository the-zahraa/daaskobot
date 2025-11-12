# backend/app/repositories/referrals.py
from __future__ import annotations
import secrets
import string
from typing import Optional, Tuple, List

from app.db import get_con

# ---------- helpers ----------

def _rand(n: int = 5) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

# ---------- API ----------

async def get_or_create_ref_code(customer_tg_id: int) -> str:
    """
    Return an existing code for the customer, or create one.
    We keep it simple: one stable code per customer.
    """
    async with get_con() as con:
        row = await con.fetchrow(
            "select code from public.ref_codes where customer_tg_id=$1 order by created_at asc limit 1",
            customer_tg_id
        )
        if row:
            return str(row["code"])

        # try a few times to avoid rare collisions
        for _ in range(5):
            code = f"c{customer_tg_id}-{_rand(6)}"
            try:
                await con.execute(
                    "insert into public.ref_codes(code, customer_tg_id) values ($1,$2)",
                    code, customer_tg_id
                )
                return code
            except Exception:
                continue
        # last resort: fetch again (race)
        row2 = await con.fetchrow(
            "select code from public.ref_codes where customer_tg_id=$1 order by created_at asc limit 1",
            customer_tg_id
        )
        if row2:
            return str(row2["code"])
        raise RuntimeError("could not create referral code")

async def assign_ref_on_start(user_tg_id: int, raw_payload: str) -> Optional[int]:
    """
    If payload matches a known code and user has no referrer yet, link them.
    Returns customer_tg_id if assigned, else None.
    Accepts payload with or without 'ref-' prefix.
    """
    payload = (raw_payload or "").strip()
    if payload.startswith("ref-"):
        payload = payload[4:]

    async with get_con() as con:
        # lookup code
        row = await con.fetchrow(
            "select customer_tg_id from public.ref_codes where code=$1",
            payload
        )
        if not row:
            return None
        customer_id = int(row["customer_tg_id"])

        # don’t allow self-refer
        if user_tg_id == customer_id:
            return None

        # set once (first touch wins)
        await con.execute(
            """
            update public.users
               set referred_by   = coalesce(referred_by, $1),
                   first_ref_code = coalesce(first_ref_code, $2),
                   referred_at    = coalesce(referred_at, now())
             where tg_id = $3
            """,
            customer_id, payload, user_tg_id
        )
        return customer_id

async def count_referred(customer_tg_id: int) -> int:
    async with get_con() as con:
        row = await con.fetchrow(
            "select count(*) as c from public.users where referred_by=$1",
            customer_tg_id
        )
    return int(row["c"]) if row else 0

async def count_referred_with_filters(
    customer_tg_id: int,
    has_phone: bool | None,
    min_username_len: int,
    min_name_len: int
) -> int:
    where = ["referred_by = $1"]
    params = [customer_tg_id]
    if has_phone is True:
        where.append("phone_e164 is not null")
    elif has_phone is False:
        where.append("phone_e164 is null")
    if min_username_len > 0:
        where.append("username is not null and char_length(username) >= $%d" % (len(params)+1))
        params.append(min_username_len)
    if min_name_len > 0:
        where.append("char_length(coalesce(first_name,'')||coalesce(last_name,'')) >= $%d" % (len(params)+1))
        params.append(min_name_len)

    sql = f"select count(*) as c from public.users where {' and '.join(where)}"
    async with get_con() as con:
        row = await con.fetchrow(sql, *params)
    return int(row["c"]) if row else 0

async def select_user_ids_for_customer(
    customer_tg_id: int,
    has_phone: bool | None,
    min_username_len: int,
    min_name_len: int,
    limit: int | None = None
) -> List[int]:
    where = ["referred_by = $1"]
    params = [customer_tg_id]
    if has_phone is True:
        where.append("phone_e164 is not null")
    elif has_phone is False:
        where.append("phone_e164 is null")
    if min_username_len > 0:
        where.append("username is not null and char_length(username) >= $%d" % (len(params)+1))
        params.append(min_username_len)
    if min_name_len > 0:
        where.append("char_length(coalesce(first_name,'')||coalesce(last_name,'')) >= $%d" % (len(params)+1))
        params.append(min_name_len)

    sql = f"""
        select tg_id
        from public.users
        where {' and '.join(where)}
        order by coalesce(last_seen_at, created_at) desc
        { 'limit %d' % limit if limit else '' }
    """
    async with get_con() as con:
        rows = await con.fetch(sql, *params)
    return [int(r["tg_id"]) for r in rows]

# ---- owner helpers (for later step) ----

async def list_customers_with_counts(limit: int = 50) -> list[tuple[int,int]]:
    """
    Returns list of (customer_tg_id, referred_count) ordered by count desc.
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            select referred_by as customer, count(*)::int as c
            from public.users
            where referred_by is not null
            group by referred_by
            order by c desc
            limit $1
            """, limit
        )
    return [(int(r["customer"]), int(r["c"])) for r in rows]
