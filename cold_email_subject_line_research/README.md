# cold_email_subject_line_research

Autopilot cold-email subject-line experimentation loop for Instantly.

## What it does

- Pulls recent replies + per-variant send counts from Instantly.
- Uses **Instantly's own classification** (lead `lt_interest_status`) to determine whether a reply is **positive**.
- Computes **positive reply rate** for the step (default: **one variant** per campaign—one active subject line).
- Cycles through untested subject lines from `guidelines.md`, keeping the body unchanged. After enough sends, **keeps** a line if its positive rate **meets or beats** your hurdle (see `CEA_BASELINE_POSITIVE_RATE`), otherwise **rolls back** and tries the next line.
- Updates your Instantly campaign sequence with the new subject line.
- Runs hourly via GitHub Actions.

## Required setup (once)

1. Create a campaign in Instantly with at least one email step and **one variant** on that step (recommended). You can set `CEA_VARIANTS=2` for legacy A/B testing on the same step.
2. Note the campaign UUID.
3. Add GitHub repo secrets:
   - `INSTANTLY_API_KEY`
   - `OPENAI_API_KEY`
   - `INSTANTLY_CAMPAIGN_ID` (single) **or** `INSTANTLY_CAMPAIGN_IDS` (comma-separated for multiple campaigns)

Optional secrets / env:
   - `OPENAI_MODEL` (default: `gpt-5-mini`)
   - `CEA_WINDOW_HOURS` (default: `72`) – how far back to score replies
   - `CEA_MIN_SENT_PER_VARIANT` (default: `50`) – don’t mutate until enough data
   - `CEA_MIN_HOURS_AFTER_CHANGE` (default: `6`) – minimum time to wait before judging a new subject line
   - `CEA_MIN_SENT_AFTER_CHANGE` (default: `50`) – minimum sends after a change before judging it
   - `CEA_STEP` (default: `1`) – which email step to optimize (1-indexed)
   - `CEA_VARIANTS` (default: `1`) – number of sequence variants on that step (variant `0` is optimized)
   - `CEA_BASELINE_POSITIVE_RATE` – optional hurdle to beat, e.g. `0.031` or `3.1` (for 3.1% positive reply rate). If unset, the runner compares each candidate to the best rate seen so far for that experiment (previous behavior).
   - `CEA_BASELINE_SUBJECT` – optional; if set, this subject is **skipped** in the rotation from `guidelines.md` (use for your control line, e.g. `{{firstName}}, Your Digital Twin`)
   - `CEA_STATE_DIR` (default: `cold_email_subject_line_research/.state`)
   - `CEA_RESULTS_CSV` – optional path for the **test results log** (default: `subject_test_results.csv` inside `CEA_STATE_DIR`). Each time a subject line finishes its trial (`keep` or `discard`), a row is appended with campaign, subject, sends, positive replies, and positive reply rate for that test window.

## Test results CSV

After a run completes a trial, the script appends to the results file (see `CEA_RESULTS_CSV`). Columns:

| Column | Meaning |
|--------|---------|
| `recorded_at` | When the trial was decided (UTC ISO) |
| `campaign_id` | Instantly campaign UUID |
| `campaign_name` | Campaign name from the API (if present) |
| `step` | 1-indexed email step |
| `variant_index` | 0-based sequence variant |
| `subject_line` | The line that was measured |
| `sent` | Sends attributed to that variant since the subject was applied |
| `positive_replies` | Replies classified as positive interest (`lt_interest_status`) for that step/variant in the same window |
| `positive_reply_rate` | `positive_replies / sent` (0 if `sent` is 0) |
| `outcome` | `keep` or `discard` |

Open the CSV in Excel, Numbers, or Google Sheets. The default path lives under `.state/` (gitignored); set `CEA_RESULTS_CSV` to a path you want to track or sync.

## Local run

From repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r cold_email_subject_line_research/requirements.txt
export INSTANTLY_API_KEY="..."
export INSTANTLY_CAMPAIGN_ID="..."
export OPENAI_API_KEY="..."
python cold_email_subject_line_research/run.py
```

## Notes

- Instantly API base URL is `https://api.instantly.ai` and uses Bearer auth.
- This system uses Instantly’s **campaign step analytics** (which include `variant`) plus **leads** (which include `email_replied_variant`) to attribute positive replies to the correct variant.
- A reply is counted as **positive** when the replied lead has `lt_interest_status` in {Interested, Meeting Booked, Meeting Completed, Won}.
- Put one subject line per line in `cold_email_subject_line_research/guidelines.md`; the runner rotates through untested lines only.
