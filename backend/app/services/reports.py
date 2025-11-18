# backend/app/services/reports.py
from __future__ import annotations
from typing import Tuple, List, Optional
from datetime import datetime, timedelta, timezone
import io
import asyncio
import logging

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyBboxPatch

from app.services.i18n import t
from app.repositories.stats import get_last_days
from app.repositories.activity import (
    get_messages_daily,
    get_dau_daily,
    get_top_talkers,
    get_peak_hour,
    get_most_active_user,
)
from app.repositories.campaigns_read import get_top_campaigns_30d

# Quiet the “categorical units” noise
logging.getLogger("matplotlib.category").setLevel(logging.WARNING)

# ---------- layout + style helpers ----------

_A4_LANDSCAPE = (11.69, 8.27)  # inches
CARD_BG = "#F7FAFC"
CARD_EDGE = "#E2E8F0"
DELTA_UP = "#16a34a"
DELTA_DOWN = "#dc2626"
TEXT_MUTED = "#4B5563"


def _new_fig() -> plt.Figure:
    return plt.figure(figsize=_A4_LANDSCAPE, dpi=120)


def _style_axes(ax, title: str, xlabel: Optional[str] = None, ylabel: Optional[str] = None):
    ax.set_title(title, pad=14, fontsize=14, fontweight="bold")
    if xlabel:
        ax.set_xlabel(xlabel, labelpad=8)
    if ylabel:
        ax.set_ylabel(ylabel, labelpad=8)
    ax.grid(True, alpha=0.25)


def _thin_xticks_dates(ax, max_ticks: int = 10):
    locator = mdates.AutoDateLocator(minticks=4, maxticks=max_ticks)
    formatter = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)


def _parse_days(days_str: List[str]) -> List[datetime]:
    out: List[datetime] = []
    for s in days_str:
        try:
            out.append(datetime.strptime(s, "%Y-%m-%d"))
        except Exception:
            out.append(datetime.utcnow())
    return out


def _pct_delta(curr: float, prev: float) -> tuple[str, Optional[str]]:
    """Return (pretty_text, color) where color is green/red or None if no change."""
    if prev <= 0 and curr <= 0:
        return "0%", None
    if prev <= 0 and curr > 0:
        return "+100%", DELTA_UP
    change = (curr - prev) / max(prev, 1e-9) * 100.0
    if abs(change) < 0.5:
        return "0%", None
    sign = "+" if change > 0 else ""
    color = DELTA_UP if change > 0 else DELTA_DOWN
    return f"{sign}{change:.0f}%", color


def _draw_kpi_cards(ax, items: List[dict]):
    """
    items: list of dicts with keys:
      - label: str
      - value: str
      - sublabel: Optional[str]
      - delta_text: Optional[str]
      - delta_color: Optional[str]
    Renders a 3x2 grid of rounded cards.
    """
    ax.axis("off")
    cols, rows = 3, 2
    margin_x, margin_y = 0.05, 0.12
    gutter_x, gutter_y = 0.035, 0.06
    card_w = (1 - 2 * margin_x - (cols - 1) * gutter_x) / cols
    card_h = (1 - 2 * margin_y - (rows - 1) * gutter_y) / rows

    for idx, it in enumerate(items[: rows * cols]):
        r = idx // cols
        c = idx % cols
        x0 = margin_x + c * (card_w + gutter_x)
        y0 = 1 - margin_y - (r + 1) * card_h - r * gutter_y

        # Card background
        patch = FancyBboxPatch(
            (x0, y0),
            card_w,
            card_h,
            boxstyle="round,pad=0.012,rounding_size=0.02",
            facecolor=CARD_BG,
            edgecolor=CARD_EDGE,
            linewidth=1.0,
            transform=ax.transAxes,
            clip_on=False,
        )
        ax.add_patch(patch)

        # Value (big)
        ax.text(
            x0 + 0.03,
            y0 + card_h - 0.08,
            it["value"],
            fontsize=26,
            fontweight="bold",
            transform=ax.transAxes,
            va="top",
        )

        # Label
        ax.text(
            x0 + 0.03,
            y0 + 0.12,
            it["label"],
            fontsize=11,
            color=TEXT_MUTED,
            transform=ax.transAxes,
            va="bottom",
        )

        # Sub-label (optional, small)
        sublabel = it.get("sublabel")
        if sublabel:
            ax.text(
                x0 + 0.03,
                y0 + 0.08,
                sublabel,
                fontsize=10,
                color=TEXT_MUTED,
                transform=ax.transAxes,
                va="bottom",
            )

        # Delta (top-right)
        dt = it.get("delta_text")
        dc = it.get("delta_color")
        if dt:
            ax.text(
                x0 + card_w - 0.03,
                y0 + card_h - 0.08,
                dt,
                fontsize=12,
                color=(dc or TEXT_MUTED),
                transform=ax.transAxes,
                va="top",
                ha="right",
            )


# ---------- main API ----------

async def build_report_pdf_bytes(
    chat_id: int,
    chat_title: str | None,
    days: int,
    is_pro: bool,
    *,
    user_id: Optional[int] = None,
    lang: Optional[str] = None,
    tz: str = "UTC",
) -> Tuple[bytes, str]:
    """
    Build a professional multi-page PDF report (Pro only).
    """
    if not is_pro:
        raise PermissionError(t("reports.errors.pro_only", user_id=user_id, lang=lang))

    window_days = max(7, min(int(days or 30), 30))

    # For deltas we fetch double windows (recent vs previous)
    jl60_task = get_last_days(chat_id, window_days * 2)
    msg60_task = get_messages_daily(chat_id, 60)
    dau60_task = get_dau_daily(chat_id, 60)
    top5_task = get_top_talkers(chat_id, days=30, limit=5)
    camp_task = get_top_campaigns_30d(chat_id, limit=10)
    peak_task = get_peak_hour(chat_id, days=30, tz=tz)
    topu_task = get_most_active_user(chat_id, days=30)

    (jl60, msg60, dau60, top5, top_camp, peak_info, top_user) = await asyncio.gather(
        jl60_task, msg60_task, dau60_task, top5_task, camp_task, peak_task, topu_task
    )

    # Split into current window and previous window (newest-first input)
    jl_curr, jl_prev = jl60[:window_days], jl60[window_days : window_days * 2]
    msg_curr, msg_prev = msg60[:30], msg60[30:60]
    dau_curr, dau_prev = dau60[:30], dau60[30:60]

    # KPIs current
    total_joins = sum(int(j) for _, j, _ in jl_curr)
    total_leaves = sum(int(l) for _, _, l in jl_curr)
    net_growth = total_joins - total_leaves
    total_msgs = sum(int(c) for _, c in msg_curr) if msg_curr else 0
    avg_dau = (
        round(sum(int(c) for _, c in dau_curr) / max(len(dau_curr), 1), 1) if dau_curr else 0.0
    )
    peak_hour = f"{peak_info[0]:02d}" if peak_info else "—"
    peak_count = int(peak_info[1]) if peak_info else 0
    top_user_id = top_user[0] if top_user else None
    top_user_cnt = int(top_user[1]) if top_user else 0

    # KPIs previous
    prev_joins = sum(int(j) for _, j, _ in jl_prev) if jl_prev else 0
    prev_leaves = sum(int(l) for _, _, l in jl_prev) if jl_prev else 0
    prev_net = prev_joins - prev_leaves
    prev_msgs = sum(int(c) for _, c in msg_prev) if msg_prev else 0
    prev_avgdau = (
        round(sum(int(c) for _, c in dau_prev) / max(len(dau_prev), 1), 1) if dau_prev else 0.0
    )

    # Deltas
    d_joins_txt, d_joins_col = _pct_delta(total_joins, prev_joins)
    d_leaves_txt, d_leaves_col = _pct_delta(total_leaves, prev_leaves)
    d_net_txt, d_net_col = _pct_delta(net_growth, prev_net)
    d_msgs_txt, d_msgs_col = _pct_delta(total_msgs, prev_msgs)
    d_dau_txt, d_dau_col = _pct_delta(avg_dau, prev_avgdau)

    # NEW derived engagement KPIs
    avg_joins_per_day = round(total_joins / max(window_days, 1), 1)
    avg_leaves_per_day = round(total_leaves / max(window_days, 1), 1)
    msgs_per_join = round(total_msgs / max(total_joins, 1), 1) if total_joins > 0 else 0.0
    msgs_per_active_per_day = 0.0
    if avg_dau > 0:
        # approximate: total messages divided by (avg DAU * days)
        msgs_per_active_per_day = round(
            total_msgs / max(avg_dau * window_days, 1.0),
            2,
        )

    now_utc = datetime.now(timezone.utc)
    end_str = now_utc.strftime("%Y-%m-%d")
    start_dt = now_utc - timedelta(days=window_days - 1)
    start_str = start_dt.strftime("%Y-%m-%d")

    pdf_buf = io.BytesIO()
    with PdfPages(pdf_buf) as pdf:
        # 1) Cover
        fig = _new_fig()
        ax = fig.add_subplot()
        ax.axis("off")
        ax.text(
            0.05,
            0.80,
            t("reports.cover.title", user_id=user_id, lang=lang),
            fontsize=28,
            fontweight="bold",
            transform=ax.transAxes,
        )
        ax.text(
            0.05,
            0.70,
            t(
                "reports.cover.subtitle",
                user_id=user_id,
                lang=lang,
                start=start_str,
                end=end_str,
            ),
            fontsize=14,
            transform=ax.transAxes,
        )
        ax.text(
            0.05,
            0.62,
            t(
                "reports.cover.chat",
                user_id=user_id,
                lang=lang,
                title=(chat_title or str(chat_id)),
            ),
            fontsize=13,
            transform=ax.transAxes,
        )
        ax.text(
            0.05,
            0.54,
            t(
                "reports.cover.generated",
                user_id=user_id,
                lang=lang,
                dt=now_utc.strftime("%Y-%m-%d %H:%M"),
            ),
            fontsize=11,
            color="#555",
            transform=ax.transAxes,
        )
        fig.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # 2) KPI cards
        fig = _new_fig()
        ax = fig.add_subplot()
        ax.text(
            0.05,
            0.92,
            t("reports.kpi.title", user_id=user_id, lang=lang),
            fontsize=18,
            fontweight="bold",
            transform=ax.transAxes,
        )
        items = [
            dict(
                label=t("reports.kpi.joins", user_id=user_id, lang=lang),
                value=f"{total_joins:,}",
                sublabel=t(
                    "reports.kpi.window",
                    user_id=user_id,
                    lang=lang,
                    n=window_days,
                ),
                delta_text=d_joins_txt,
                delta_color=d_joins_col,
            ),
            dict(
                label=t("reports.kpi.leaves", user_id=user_id, lang=lang),
                value=f"{total_leaves:,}",
                sublabel=t(
                    "reports.kpi.window",
                    user_id=user_id,
                    lang=lang,
                    n=window_days,
                ),
                delta_text=d_leaves_txt,
                delta_color=d_leaves_col,
            ),
            dict(
                label=t("reports.kpi.net", user_id=user_id, lang=lang),
                value=f"{net_growth:,}",
                sublabel=t(
                    "reports.kpi.window",
                    user_id=user_id,
                    lang=lang,
                    n=window_days,
                ),
                delta_text=d_net_txt,
                delta_color=d_net_col,
            ),
            dict(
                label=t("reports.kpi.messages", user_id=user_id, lang=lang),
                value=f"{total_msgs:,}",
                sublabel=t("reports.kpi.window30", user_id=user_id, lang=lang),
                delta_text=d_msgs_txt,
                delta_color=d_msgs_col,
            ),
            dict(
                label=t("reports.kpi.avg_dau", user_id=user_id, lang=lang),
                value=f"{avg_dau:,}",
                sublabel=t(
                    "reports.kpi.window30_users",
                    user_id=user_id,
                    lang=lang,
                ),
                delta_text=d_dau_txt,
                delta_color=d_dau_col,
            ),
            dict(
                label=t("reports.kpi.peak_hour", user_id=user_id, lang=lang),
                value=(f"{peak_hour}:00" if peak_info else "—"),
                sublabel=(
                    t(
                        "reports.kpi.peak_count",
                        user_id=user_id,
                        lang=lang,
                        n=peak_count,
                    )
                    if peak_info
                    else ""
                ),
                delta_text=None,
                delta_color=None,
            ),
        ]
        _draw_kpi_cards(ax, items)

        # top user (caption)
        if top_user_id is not None:
            ax.text(
                0.05,
                0.06,
                t(
                    "reports.kpi.top_user",
                    user_id=user_id,
                    lang=lang,
                    top_user_id=top_user_id,
                    count=top_user_cnt,
                ),
                fontsize=11,
                color=TEXT_MUTED,
                transform=ax.transAxes,
            )
        fig.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # 2b) NEW: Engagement summary page
        fig = _new_fig()
        ax = fig.add_subplot()
        ax.axis("off")
        ax.text(
            0.05,
            0.90,
            t("reports.eng.title", user_id=user_id, lang=lang),
            fontsize=18,
            fontweight="bold",
            transform=ax.transAxes,
        )

        lines = [
            t(
                "reports.eng.avg_joins_per_day",
                user_id=user_id,
                lang=lang,
                value=f"{avg_joins_per_day:.1f}",
            ),
            t(
                "reports.eng.avg_leaves_per_day",
                user_id=user_id,
                lang=lang,
                value=f"{avg_leaves_per_day:.1f}",
            ),
            t(
                "reports.eng.msgs_per_active_per_day",
                user_id=user_id,
                lang=lang,
                value=f"{msgs_per_active_per_day:.2f}",
            ),
            t(
                "reports.eng.msgs_per_join",
                user_id=user_id,
                lang=lang,
                value=f"{msgs_per_join:.1f}",
            ),
        ]

        y = 0.80
        for line in lines:
            ax.text(
                0.07,
                y,
                f"• {line}",
                fontsize=12,
                transform=ax.transAxes,
            )
            y -= 0.06

        fig.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # 3) Joins vs Leaves
        if jl_curr:
            days_lbl = [d for d, *_ in jl_curr][::-1]
            x = _parse_days(days_lbl)
            joins_y = [int(j) for _, j, _ in jl_curr][::-1]
            leaves_y = [int(l) for _, _, l in jl_curr][::-1]

            fig = _new_fig()
            ax = fig.add_subplot()
            _style_axes(
                ax,
                t("reports.series.joins_leaves", user_id=user_id, lang=lang),
                xlabel=t("reports.axis.day", user_id=user_id, lang=lang),
                ylabel=t("reports.axis.count", user_id=user_id, lang=lang),
            )
            ax.plot(
                x,
                joins_y,
                linewidth=2.2,
                label=t("reports.kpi.joins", user_id=user_id, lang=lang),
            )
            ax.plot(
                x,
                leaves_y,
                linewidth=2.2,
                label=t("reports.kpi.leaves", user_id=user_id, lang=lang),
            )
            _thin_xticks_dates(ax, max_ticks=10)
            fig.autofmt_xdate()
            ax.legend()
            fig.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # 4) Messages
        if msg_curr:
            days_lbl = [d for d, _ in msg_curr][::-1]
            x = _parse_days(days_lbl)
            y = [int(c) for _, c in msg_curr][::-1]
            fig = _new_fig()
            ax = fig.add_subplot()
            _style_axes(
                ax,
                t("reports.series.messages", user_id=user_id, lang=lang),
                xlabel=t("reports.axis.day", user_id=user_id, lang=lang),
                ylabel=t("reports.axis.count", user_id=user_id, lang=lang),
            )
            ax.plot(x, y, linewidth=2.2)
            _thin_xticks_dates(ax, max_ticks=10)
            fig.autofmt_xdate()
            fig.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # 5) DAU
        if dau_curr:
            days_lbl = [d for d, _ in dau_curr][::-1]
            x = _parse_days(days_lbl)
            y = [int(c) for _, c in dau_curr][::-1]
            fig = _new_fig()
            ax = fig.add_subplot()
            _style_axes(
                ax,
                t("reports.series.dau", user_id=user_id, lang=lang),
                xlabel=t("reports.axis.day", user_id=user_id, lang=lang),
                ylabel=t("reports.axis.users", user_id=user_id, lang=lang),
            )
            ax.plot(x, y, linewidth=2.2)
            _thin_xticks_dates(ax, max_ticks=10)
            fig.autofmt_xdate()
            fig.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # 6) Top campaigns
        if top_camp:
            labels = [name for name, _ in top_camp]
            vals = [int(v) for _, v in top_camp]
            fig = _new_fig()
            ax = fig.add_subplot()
            _style_axes(
                ax,
                t("reports.campaigns.title", user_id=user_id, lang=lang),
                ylabel=t("reports.campaigns.ylabel", user_id=user_id, lang=lang),
            )
            ax.bar(labels, vals)
            ax.tick_params(axis="x", rotation=30)
            fig.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # 7) Top talkers
        if top5:
            fig = _new_fig()
            ax = fig.add_subplot()
            ax.set_title(
                t("reports.top_talkers.title", user_id=user_id, lang=lang),
                fontsize=14,
                fontweight="bold",
            )
            ax.axis("off")
            headers = [
                t("reports.top_talkers.header_user", user_id=user_id, lang=lang),
                t("reports.top_talkers.header_msgs", user_id=user_id, lang=lang),
            ]
            y_text = 0.85
            ax.text(
                0.06,
                y_text,
                f"{headers[0]:<18}  {headers[1]:>10}",
                family="monospace",
                fontsize=12,
                transform=ax.transAxes,
            )
            y_text -= 0.03
            ax.text(
                0.06,
                y_text,
                "-" * 34,
                family="monospace",
                color="#666",
                transform=ax.transAxes,
            )
            for uid, total in top5:
                y_text -= 0.05
                ax.text(
                    0.06,
                    y_text,
                    f"{str(uid):<18}  {total:>10}",
                    family="monospace",
                    fontsize=12,
                    transform=ax.transAxes,
                )
            fig.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

    pdf_buf.seek(0)
    return pdf_buf.read(), f"report_chat_{chat_id}.pdf"
