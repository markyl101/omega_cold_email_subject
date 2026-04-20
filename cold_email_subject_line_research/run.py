import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


INSTANTLY_BASE_URL = os.getenv("INSTANTLY_BASE_URL", "https://api.instantly.ai").rstrip("/")

POSITIVE_LT_STATUSES = {1, 2, 3, 4}   # Interested, Meeting Booked, Meeting Completed, Won
NEGATIVE_LT_STATUSES = {-1, -2, -3, -4}  # Not Interested, Wrong Person, Lost, No Show
AUTO_REPLY_LT_STATUS = 0  # Out of Office


@dataclass(frozen=True)
class Settings:
    instantly_api_key: str
    instantly_campaign_ids: Tuple[str, ...]

    openai_api_key: str
    openai_model: str = "gpt-5-mini"

    window_hours: int = int(os.getenv("CEA_WINDOW_HOURS", "72"))
    min_sent_per_variant: int = int(os.getenv("CEA_MIN_SENT_PER_VARIANT", "50"))
    min_hours_after_change: int = int(os.getenv("CEA_MIN_HOURS_AFTER_CHANGE", "6"))
    min_sent_after_change: int = int(os.getenv("CEA_MIN_SENT_AFTER_CHANGE", "50"))
    step: int = int(os.getenv("CEA_STEP", "1"))  # 1-indexed campaign step
    variants: int = int(os.getenv("CEA_VARIANTS", "1"))  # single variant: one subject line per campaign

    baseline_positive_rate: Optional[float] = None
    baseline_subject: Optional[str] = None

    state_dir: str = os.getenv("CEA_STATE_DIR", "cold_email_subject_line_research/.state")


class InstantlyClient:
    def __init__(self, api_key: str, base_url: str = INSTANTLY_BASE_URL, timeout_s: int = 30):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.timeout_s = timeout_s

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        r = self.session.get(self._url(f"/api/v2/campaigns/{campaign_id}"), timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def patch_campaign(self, campaign_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.patch(self._url(f"/api/v2/campaigns/{campaign_id}"), json=payload, timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def campaign_steps_analytics(self, campaign_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        params = {"campaign_id": campaign_id, "start_date": start_date, "end_date": end_date}
        r = self.session.get(self._url("/api/v2/campaigns/analytics/steps"), params=params, timeout=self.timeout_s)
        if r.status_code >= 400:
            try:
                body = r.json()
            except Exception:
                body = r.text
            print(
                f"[warn] campaign_steps_analytics failed for campaign_id={campaign_id} "
                f"({r.status_code}) response={body}"
            )
            return []
        return r.json()

    def list_leads_replied_since(self, campaign_id: str, since_iso: str, limit: int = 100) -> List[Dict[str, Any]]:
        # Uses smart view query: reply within occurrence-days; we approximate by pulling replied leads
        # and filtering on timestamp_last_reply client-side for an exact cutoff.
        items: List[Dict[str, Any]] = []
        starting_after: Optional[str] = None
        while True:
            body: Dict[str, Any] = {
                "campaign": campaign_id,
                "filter": "FILTER_VAL_REPLIED",
                "limit": min(100, limit),
            }
            if starting_after:
                body["starting_after"] = starting_after
            r = self.session.post(self._url("/api/v2/leads/list"), json=body, timeout=self.timeout_s)
            r.raise_for_status()
            data = r.json()
            page = data.get("items", [])
            if not page:
                break
            for lead in page:
                ts = lead.get("timestamp_last_reply")
                if ts and ts >= since_iso:
                    items.append(lead)
            starting_after = data.get("next_starting_after")
            if not starting_after:
                break
            if len(items) >= limit:
                break
        return items[:limit]

    def list_received_emails_for_lead(
        self,
        campaign_id: str,
        lead_email: str,
        min_timestamp_created: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        params = {
            "campaign_id": campaign_id,
            "lead": lead_email,
            "email_type": "received",
            "min_timestamp_created": min_timestamp_created,
            "limit": min(100, limit),
            "sort_order": "desc",
        }
        r = self.session.get(self._url("/api/v2/emails"), params=params, timeout=self.timeout_s)
        r.raise_for_status()
        return r.json().get("items", [])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_baseline_positive_rate() -> Optional[float]:
    """
    CEA_BASELINE_POSITIVE_RATE: decimal (0.031) or percent (3.1 or 3.1%).
    Values > 1 are treated as a percentage and divided by 100.
    """
    raw = (os.getenv("CEA_BASELINE_POSITIVE_RATE") or "").strip()
    if not raw:
        return None
    try:
        v = float(raw.rstrip("%").strip())
    except ValueError:
        return None
    if v > 1.0:
        return v / 100.0
    return v


def parse_baseline_subject() -> Optional[str]:
    s = (os.getenv("CEA_BASELINE_SUBJECT") or "").strip()
    return s if s else None


def load_dotenv_if_present(dotenv_path: str = ".env", override: bool = True) -> None:
    """
    Lightweight .env loader (no external dependency).
    - Does not override existing environment variables.
    - Supports simple KEY=VALUE lines (optionally quoted).
    """
    p = Path(dotenv_path)
    if not p.exists() or not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = val


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_state(state_dir: str) -> Dict[str, Any]:
    p = Path(state_dir)
    p.mkdir(parents=True, exist_ok=True)
    f = p / "state.json"
    if not f.exists():
        return {"last_run_started_at": None, "campaigns": {}}
    return json.loads(f.read_text())


def save_state(state_dir: str, state: Dict[str, Any]) -> None:
    p = Path(state_dir)
    p.mkdir(parents=True, exist_ok=True)
    f = p / "state.json"
    f.write_text(json.dumps(state, indent=2, sort_keys=True))

def _campaign_state(state: Dict[str, Any], campaign_id: str) -> Dict[str, Any]:
    campaigns = state.setdefault("campaigns", {})
    if campaign_id not in campaigns:
        campaigns[campaign_id] = {
            "last_reply_processed_at": None,
            "classified_reply_ids": [],
            "last_decision": None,
            "experiments": {},  # keyed by "<step>:<variant>"
        }
    return campaigns[campaign_id]


def _exp_key(step: int, variant: int) -> str:
    return f"{step}:{variant}"


def _get_exp(cstate: Dict[str, Any], step: int, variant: int) -> Dict[str, Any]:
    exps = cstate.setdefault("experiments", {})
    key = _exp_key(step, variant)
    if key not in exps:
        exps[key] = {
            "best_subject": None,
            "best_rate": None,
            "pending": None,  # {"started_at": iso, "prev_subject": str, "candidate_subject": str}
            "tested_subjects": [],
        }
    exp = exps[key]
    # Backward-compatibility for older body-focused state entries.
    if "best_subject" not in exp:
        exp["best_subject"] = exp.get("best_body")
    pending = exp.get("pending")
    if isinstance(pending, dict):
        if "prev_subject" not in pending and "prev_body" in pending:
            pending["prev_subject"] = pending.get("prev_body")
        if "candidate_subject" not in pending and "candidate_body" in pending:
            pending["candidate_subject"] = pending.get("candidate_body")
    if "tested_subjects" not in exp or not isinstance(exp.get("tested_subjects"), list):
        exp["tested_subjects"] = []
    return exp


def _parse_iso(s: str) -> datetime:
    # expects "Z" suffixed UTC timestamps produced by iso()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _compute_counts_for_window(
    client: InstantlyClient,
    campaign_id: str,
    step: int,
    variants: int,
    window_start: datetime,
    window_end: datetime,
) -> Tuple[Dict[int, int], Dict[int, int]]:
    # Instantly analytics uses 0-indexed steps (Step 1 in UI => step=0 in API).
    api_step = step - 1
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()

    steps_analytics = client.campaign_steps_analytics(campaign_id, start_date=start_date, end_date=end_date)
    sent_by_variant: Dict[int, int] = {v: 0 for v in range(variants)}
    for row in steps_analytics:
        try:
            s_step = int(row.get("step")) if row.get("step") is not None else None
            s_variant = int(row.get("variant")) if row.get("variant") is not None else None
        except Exception:
            continue
        if s_step != api_step or s_variant is None:
            continue
        if 0 <= s_variant < variants:
            sent_by_variant[s_variant] = int(row.get("sent", 0))

    # Replies: fetch leads replied since window_start (server-side filter), then client-side filter to window_end.
    leads = client.list_leads_replied_since(campaign_id, since_iso=iso(window_start), limit=500)
    positives_by_variant: Dict[int, int] = {v: 0 for v in range(variants)}
    for lead in leads:
        reply_ts = lead.get("timestamp_last_reply")
        if not reply_ts:
            continue
        try:
            rt = _parse_iso(reply_ts)
        except Exception:
            continue
        if rt > window_end:
            continue
        # Only count replies for the target step (0-indexed in Instantly lead fields).
        replied_step = lead.get("email_replied_step")
        try:
            rs = int(replied_step) if replied_step is not None else None
        except Exception:
            rs = None
        if rs is None or rs != api_step:
            continue
        lt_interest_status = lead.get("lt_interest_status")
        replied_variant = lead.get("email_replied_variant")
        try:
            is_positive = lt_interest_status is not None and int(lt_interest_status) in POSITIVE_LT_STATUSES
        except Exception:
            is_positive = False
        if not is_positive:
            continue
        try:
            v = int(replied_variant) if replied_variant is not None else None
        except Exception:
            v = None
        if v is not None and 0 <= v < variants:
            positives_by_variant[v] += 1

    return sent_by_variant, positives_by_variant


def _parse_campaign_ids() -> Tuple[str, ...]:
    # Support both vars; merge if both are present.
    multi = os.getenv("INSTANTLY_CAMPAIGN_IDS") or ""
    single = os.getenv("INSTANTLY_CAMPAIGN_ID") or ""
    raw = ",".join([x for x in [multi, single] if x.strip()])
    parsed = [c.strip() for c in raw.split(",") if c.strip()]
    # De-dupe while preserving order (avoids accidental repeats in env).
    seen = set()
    ids_list: List[str] = []
    for cid in parsed:
        if cid in seen:
            continue
        seen.add(cid)
        ids_list.append(cid)
    ids = tuple(ids_list)
    if not ids:
        raise KeyError("INSTANTLY_CAMPAIGN_ID (or INSTANTLY_CAMPAIGN_IDS) is required")
    return ids


def openai_client(api_key: str):
    if OpenAI is None:
        raise RuntimeError("openai package not installed")
    return OpenAI(api_key=api_key)


def resolve_openai_model(default_model: str = "gpt-5-mini") -> str:
    model = (os.getenv("OPENAI_MODEL") or "").strip()
    return model or default_model

def load_guidelines_text() -> str:
    # Optional file a user can edit without touching code.
    p = Path(__file__).with_name("guidelines.md")
    if not p.exists():
        return ""
    txt = p.read_text(encoding="utf-8").strip()
    return txt


def load_subject_lines() -> List[str]:
    p = Path(__file__).with_name("guidelines.md")
    if not p.exists():
        return []
    lines: List[str] = []
    seen = set()
    for raw in p.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        lines.append(s)
    return lines


def pick_next_subject(
    subject_lines: List[str],
    tested_subjects: List[str],
    current_subject: str,
    baseline_subject: Optional[str] = None,
) -> Optional[str]:
    tested = {s.strip().casefold() for s in tested_subjects if isinstance(s, str) and s.strip()}
    current_key = (current_subject or "").strip().casefold()
    baseline_key = (baseline_subject or "").strip().casefold()
    for subject in subject_lines:
        key = subject.casefold()
        if key in tested or key == current_key:
            continue
        if baseline_key and key == baseline_key:
            continue
        return subject
    return None


def comparison_threshold(
    exp: Dict[str, Any],
    baseline_positive_rate: Optional[float],
) -> float:
    if baseline_positive_rate is not None:
        return float(baseline_positive_rate)
    return float(exp.get("best_rate") or 0.0)


def resolve_results_csv_path(state_dir: str) -> str:
    override = (os.getenv("CEA_RESULTS_CSV") or "").strip()
    if override:
        return override
    return str(Path(state_dir) / "subject_test_results.csv")


def append_subject_test_result_row(
    path: str,
    *,
    recorded_at: str,
    campaign_id: str,
    campaign_name: str,
    step: int,
    variant_index: int,
    subject_line: str,
    sent: int,
    positive_replies: int,
    positive_reply_rate: float,
    outcome: str,
) -> None:
    """Append one row when a subject-line test finishes (keep or discard). CSV is created with header if missing."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    new_file = not p.exists()
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(
                [
                    "recorded_at",
                    "campaign_id",
                    "campaign_name",
                    "step",
                    "variant_index",
                    "subject_line",
                    "sent",
                    "positive_replies",
                    "positive_reply_rate",
                    "outcome",
                ]
            )
        w.writerow(
            [
                recorded_at,
                campaign_id,
                campaign_name,
                step,
                variant_index,
                subject_line,
                sent,
                positive_replies,
                round(positive_reply_rate, 8),
                outcome,
            ]
        )


def _openai_text(openai, model: str, prompt: str) -> str:
    raw = ""
    # Newer OpenAI SDKs
    if hasattr(openai, "responses"):
        resp = openai.responses.create(
            model=model,
            input=prompt,
        )
        raw = (getattr(resp, "output_text", "") or "").strip()
    # Older OpenAI SDKs
    elif hasattr(openai, "chat") and hasattr(openai.chat, "completions"):
        resp = openai.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        try:
            raw = (resp.choices[0].message.content or "").strip()
        except Exception as e:
            raise RuntimeError(f"OpenAI chat completion parse failure: {e}")
    else:
        raise RuntimeError("Unsupported OpenAI client: missing responses and chat.completions")

    if not raw:
        raise RuntimeError("OpenAI returned empty output")
    return raw


def generate_new_subject(
    openai,
    model: str,
    best_copy: Dict[str, str],
    worst_copy: Dict[str, str],
    forced_subject: str,
) -> Dict[str, str]:
    guidelines = load_guidelines_text()
    prompt = (
        "You are improving cold email subject lines to increase positive reply rate.\n"
        + (f"\nWRITING_GUIDELINES:\n{guidelines}\n\n" if guidelines else "\n")
        + "Constraints:\n"
        + f"- Use EXACTLY this subject line with no edits: {forced_subject}\n"
        "- Keep subject concise and natural.\n"
        "- Avoid spammy words and excessive punctuation.\n"
        "- Keep placeholders like {{firstName}} exactly as-is.\n"
        "- IMPORTANT: Do NOT change the email body.\n"
        "- IMPORTANT: Do NOT change the subject text you were given.\n"
        "- Output ONLY JSON: {\"subject\": \"...\"}\n\n"
        "BEST_VARIANT (keep style strengths):\n"
        f"Subject: {best_copy['subject']}\nBody:\n{best_copy['body']}\n\n"
        "WORST_VARIANT (rewrite this one):\n"
        f"Subject: {worst_copy['subject']}\nBody:\n{worst_copy['body']}\n"
    )
    raw = _openai_text(openai, model, prompt)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not m:
            raise RuntimeError(f"OpenAI output was not valid JSON: {raw[:300]}")
        obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise RuntimeError("OpenAI output JSON was not an object")
    # Hard guardrail: always preserve the rotating subject exactly.
    obj["subject"] = forced_subject.strip()
    return obj


def compute_positive_reply_rate_by_variant(
    positives_by_variant: Dict[int, int],
    sent_by_variant: Dict[int, int],
    variants: int,
) -> Dict[int, float]:
    rates: Dict[int, float] = {}
    for v in range(variants):
        sent = max(0, int(sent_by_variant.get(v, 0)))
        pos = max(0, int(positives_by_variant.get(v, 0)))
        rates[v] = (pos / sent) if sent > 0 else 0.0
    return rates


def get_step_variant_copy(campaign: Dict[str, Any], step_index_1: int, variant_index_0: int) -> Dict[str, str]:
    sequences = campaign.get("sequences") or []
    seq0 = sequences[0] if sequences else {}
    steps = seq0.get("steps") or []
    step = steps[step_index_1 - 1]
    variants = step.get("variants") or []
    v = variants[variant_index_0]
    return {"subject": v.get("subject", ""), "body": v.get("body", "")}


def get_step_variant_subjects(campaign: Dict[str, Any], step_index_1: int, variants: int) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for v in range(variants):
        try:
            out[v] = get_step_variant_copy(campaign, step_index_1, v).get("subject", "")
        except Exception:
            out[v] = ""
    return out


def set_step_variant_copy(campaign_patch: Dict[str, Any], step_index_1: int, variant_index_0: int, subject: str, body: str) -> Dict[str, Any]:
    # Build a minimal patch payload that includes sequences/steps/variants for the specific step.
    # Instantly requires "sequences" array and uses only first element.
    seq = {"steps": []}
    for _ in range(step_index_1):
        seq["steps"].append({"type": "email", "delay": 0, "variants": []})
    # We will fetch live campaign and then fill the exact structure elsewhere; this function expects
    # caller to overwrite this placeholder with real steps. Kept for shape clarity.
    campaign_patch["sequences"] = [seq]
    return campaign_patch


def main() -> None:
    load_dotenv_if_present(override=True)
    s = Settings(
        instantly_api_key=os.environ["INSTANTLY_API_KEY"],
        instantly_campaign_ids=_parse_campaign_ids(),
        openai_api_key=os.environ["OPENAI_API_KEY"],
        openai_model=resolve_openai_model(),
        baseline_positive_rate=parse_baseline_positive_rate(),
        baseline_subject=parse_baseline_subject(),
    )
    results_csv_path = resolve_results_csv_path(s.state_dir)
    print(
        json.dumps(
            {
                "config": {
                    "window_hours": s.window_hours,
                    "min_sent_per_variant": s.min_sent_per_variant,
                    "min_hours_after_change": s.min_hours_after_change,
                    "min_sent_after_change": s.min_sent_after_change,
                    "step": s.step,
                    "variants": s.variants,
                    "baseline_positive_rate": s.baseline_positive_rate,
                    "baseline_subject": s.baseline_subject,
                    "openai_model": s.openai_model,
                    "campaign_ids": list(s.instantly_campaign_ids),
                    "results_csv_path": results_csv_path,
                }
            },
            indent=2,
        )
    )

    state = load_state(s.state_dir)
    run_started = utc_now()
    state["last_run_started_at"] = iso(run_started)

    client = InstantlyClient(s.instantly_api_key)
    openai = openai_client(s.openai_api_key)

    window_start = run_started - timedelta(hours=s.window_hours)
    start_date = window_start.date().isoformat()
    end_date = run_started.date().isoformat()

    all_decisions: Dict[str, Any] = {}

    for campaign_id in s.instantly_campaign_ids:
        cstate = _campaign_state(state, campaign_id)

        # Rolling window metrics (used for baseline and for picking a candidate).
        sent_by_variant, positives_by_variant = _compute_counts_for_window(
            client,
            campaign_id=campaign_id,
            step=s.step,
            variants=s.variants,
            window_start=window_start,
            window_end=run_started,
        )
        rates = compute_positive_reply_rate_by_variant(positives_by_variant, sent_by_variant, s.variants)

        if any(sent_by_variant[v] < s.min_sent_per_variant for v in range(s.variants)):
            cstate["last_decision"] = {
                "action": "hold",
                "reason": "insufficient_sent_volume",
                "sent_by_variant": sent_by_variant,
                "positives_by_variant": positives_by_variant,
                "rates": rates,
                "at": iso(run_started),
            }
            all_decisions[campaign_id] = cstate["last_decision"]
            continue

        # Single-variant mode: only step variant 0 is optimized (one subject line per campaign).
        # Multi-variant: rotate the worst-performing variant (legacy A/B).
        if s.variants == 1:
            best_variant = 0
            worst_variant = 0
        else:
            best_variant = max(range(s.variants), key=lambda v: rates.get(v, 0.0))
            worst_variant = min(range(s.variants), key=lambda v: rates.get(v, 0.0))
            if best_variant == worst_variant:
                worst_variant = (best_variant + 1) % s.variants

        exp = _get_exp(cstate, s.step, worst_variant)

        campaign = client.get_campaign(campaign_id)
        best_copy = get_step_variant_copy(campaign, s.step, best_variant)
        worst_copy = get_step_variant_copy(campaign, s.step, worst_variant)
        subjects_by_variant = get_step_variant_subjects(campaign, s.step, s.variants)

        # Initialize best_* on first observation (hurdle for keep/discard).
        if exp.get("best_subject") is None:
            if s.baseline_positive_rate is not None:
                exp["best_rate"] = float(s.baseline_positive_rate)
                exp["best_subject"] = (s.baseline_subject or "").strip() or worst_copy.get("subject", "")
            else:
                exp["best_subject"] = worst_copy.get("subject", "")
                exp["best_rate"] = rates.get(worst_variant, 0.0)

        pending = exp.get("pending")
        if pending:
            live_subject = (worst_copy.get("subject") or "").strip()
            pending_subject = (pending.get("candidate_subject") or "").strip()
            pending_prev_subject = (pending.get("prev_subject") or "").strip()
            if live_subject and live_subject not in {pending_subject, pending_prev_subject}:
                # Manual subject edit detected in Instantly. Clear pending and re-sync state.
                exp["pending"] = None
                exp["best_subject"] = live_subject
                exp["best_rate"] = rates.get(worst_variant, 0.0)
                tested_keys = {x.casefold() for x in exp.get("tested_subjects", []) if isinstance(x, str)}
                if live_subject.casefold() not in tested_keys:
                    exp["tested_subjects"].append(live_subject)
                cstate["last_decision"] = {
                    "action": "sync_manual_subject_override",
                    "campaign_id": campaign_id,
                    "variant": worst_variant,
                    "live_subject": live_subject,
                    "replaced_pending_subject": pending_subject,
                    "subjects_by_variant": subjects_by_variant,
                    "at": iso(run_started),
                }
                all_decisions[campaign_id] = cstate["last_decision"]
                continue

            started_at = _parse_iso(pending["started_at"])
            hours_since = (run_started - started_at).total_seconds() / 3600.0

            sent_after, pos_after = _compute_counts_for_window(
                client,
                campaign_id=campaign_id,
                step=s.step,
                variants=s.variants,
                window_start=started_at,
                window_end=run_started,
            )
            rates_after = compute_positive_reply_rate_by_variant(pos_after, sent_after, s.variants)
            candidate_rate = rates_after.get(worst_variant, 0.0)
            sent_delta = sent_after.get(worst_variant, 0)

            if hours_since < s.min_hours_after_change or sent_delta < s.min_sent_after_change:
                cstate["last_decision"] = {
                    "action": "wait",
                    "reason": "not_enough_post_change_data",
                    "campaign_id": campaign_id,
                    "variant": worst_variant,
                    "hours_since_change": round(hours_since, 2),
                    "sent_since_change": sent_delta,
                    "candidate_rate_so_far": candidate_rate,
                    "best_rate": exp.get("best_rate"),
                    "candidate_subject": pending.get("candidate_subject"),
                    "current_subject": live_subject,
                    "subjects_by_variant": subjects_by_variant,
                    "at": iso(run_started),
                }
                all_decisions[campaign_id] = cstate["last_decision"]
                continue

            hurdle = comparison_threshold(exp, s.baseline_positive_rate)
            if candidate_rate >= hurdle:
                # KEEP: beat hurdle (baseline or prior best), promote candidate, clear pending.
                exp["best_subject"] = pending["candidate_subject"]
                exp["best_rate"] = candidate_rate
                exp["pending"] = None
                cstate["last_decision"] = {
                    "action": "keep",
                    "campaign_id": campaign_id,
                    "variant": worst_variant,
                    "hurdle_rate": hurdle,
                    "candidate_rate": candidate_rate,
                    "sent_since_change": sent_delta,
                    "kept_subject": pending.get("candidate_subject"),
                    "subjects_by_variant": subjects_by_variant,
                    "at": iso(run_started),
                }
                all_decisions[campaign_id] = cstate["last_decision"]
                append_subject_test_result_row(
                    results_csv_path,
                    recorded_at=iso(run_started),
                    campaign_id=campaign_id,
                    campaign_name=str(campaign.get("name") or campaign.get("title") or ""),
                    step=s.step,
                    variant_index=worst_variant,
                    subject_line=str(pending.get("candidate_subject") or ""),
                    sent=int(sent_delta),
                    positive_replies=int(pos_after.get(worst_variant, 0)),
                    positive_reply_rate=float(candidate_rate),
                    outcome="keep",
                )
                continue

            # DISCARD: rollback to previous best subject.
            rollback_subject = pending["prev_subject"]
            seq0 = (campaign.get("sequences") or [{}])[0]
            steps = seq0.get("steps") or []
            steps_patch = []
            for idx, step_obj in enumerate(steps, start=1):
                if idx != s.step:
                    steps_patch.append(step_obj)
                    continue
                variants_list = step_obj.get("variants") or []
                variants2 = []
                for v_idx, v_obj in enumerate(variants_list):
                    if v_idx == worst_variant:
                        variants2.append({**v_obj, "subject": rollback_subject})
                    else:
                        variants2.append(v_obj)
                steps_patch.append({**step_obj, "variants": variants2})
            client.patch_campaign(campaign_id, {"sequences": [{"steps": steps_patch}]})

            exp["pending"] = None
            cstate["last_decision"] = {
                "action": "discard_and_rollback",
                "campaign_id": campaign_id,
                "variant": worst_variant,
                "candidate_rate": candidate_rate,
                "hurdle_rate": hurdle,
                "sent_since_change": sent_delta,
                "rolled_back_to_subject": rollback_subject,
                "discarded_subject": pending.get("candidate_subject"),
                "subjects_by_variant": subjects_by_variant,
                "at": iso(run_started),
            }
            all_decisions[campaign_id] = cstate["last_decision"]
            append_subject_test_result_row(
                results_csv_path,
                recorded_at=iso(run_started),
                campaign_id=campaign_id,
                campaign_name=str(campaign.get("name") or campaign.get("title") or ""),
                step=s.step,
                variant_index=worst_variant,
                subject_line=str(pending.get("candidate_subject") or ""),
                sent=int(sent_delta),
                positive_replies=int(pos_after.get(worst_variant, 0)),
                positive_reply_rate=float(candidate_rate),
                outcome="discard",
            )
            continue

        # No pending candidate: rotate to next untested subject and apply immediately.
        subject_lines = load_subject_lines()
        next_subject = pick_next_subject(
            subject_lines=subject_lines,
            tested_subjects=exp.get("tested_subjects", []),
            current_subject=worst_copy.get("subject", ""),
            baseline_subject=s.baseline_subject,
        )
        if not next_subject:
            cstate["last_decision"] = {
                "action": "hold",
                "reason": "no_untested_subjects_available",
                "campaign_id": campaign_id,
                "variant": worst_variant,
                "tested_subjects_count": len(exp.get("tested_subjects", [])),
                "current_subject": worst_copy.get("subject", ""),
                "subjects_by_variant": subjects_by_variant,
                "at": iso(run_started),
            }
            all_decisions[campaign_id] = cstate["last_decision"]
            continue

        new_copy = generate_new_subject(
            openai,
            s.openai_model,
            best_copy=best_copy,
            worst_copy=worst_copy,
            forced_subject=next_subject,
        )
        new_subject = (new_copy.get("subject") or "").strip()
        if not new_subject:
            raise RuntimeError("OpenAI returned empty subject")

        seq0 = (campaign.get("sequences") or [{}])[0]
        steps = seq0.get("steps") or []
        if len(steps) < s.step:
            raise RuntimeError(f"Campaign {campaign_id} has {len(steps)} steps; cannot access step {s.step}")

        steps_patch = []
        for idx, step_obj in enumerate(steps, start=1):
            if idx != s.step:
                steps_patch.append(step_obj)
                continue
            variants_list = step_obj.get("variants") or []
            if len(variants_list) < s.variants:
                raise RuntimeError(f"Campaign {campaign_id} step has {len(variants_list)} variants; expected {s.variants}")
            variants2 = []
            for v_idx, v_obj in enumerate(variants_list):
                if v_idx == worst_variant:
                    variants2.append({**v_obj, "subject": new_subject})
                else:
                    variants2.append(v_obj)
            steps_patch.append({**step_obj, "variants": variants2})

        client.patch_campaign(campaign_id, {"sequences": [{"steps": steps_patch}]})
        exp["pending"] = {
            "started_at": iso(run_started),
            "prev_subject": worst_copy.get("subject", ""),
            "candidate_subject": new_subject,
        }
        tested_keys = {x.casefold() for x in exp.get("tested_subjects", []) if isinstance(x, str)}
        if new_subject.casefold() not in tested_keys:
            exp["tested_subjects"].append(new_subject)

        cstate["last_decision"] = {
            "action": "mutate",
            "campaign_id": campaign_id,
            "variant": worst_variant,
            "hurdle_rate": comparison_threshold(exp, s.baseline_positive_rate),
            "new_subject_applied": True,
            "applied_subject": new_subject,
            "previous_subject": worst_copy.get("subject", ""),
            "kept_body": worst_copy.get("body", ""),
            "subjects_by_variant": subjects_by_variant,
            "at": iso(run_started),
        }
        all_decisions[campaign_id] = cstate["last_decision"]

    save_state(s.state_dir, state)
    print(json.dumps(all_decisions, indent=2))


if __name__ == "__main__":
    main()

