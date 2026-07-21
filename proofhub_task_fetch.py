import os
import requests
import csv
import json
import time
from datetime import datetime, timezone
from collections import Counter

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
COMPANY_NAME = "srvmedia"
API_KEY      = os.environ.get("PROOFHUB_API_KEY")

if not API_KEY:
    print("❌ PROOFHUB_API_KEY environment variable not set.")
    exit()

BASE_URL = f"https://{COMPANY_NAME}.proofhub.com/api/v3"
HEADERS  = {
    "X-API-KEY":  API_KEY,
    "User-Agent": "ZohoIntegration (ashish.kate@srvmedia.com)",
    "Accept":     "application/json"
}

# ─────────────────────────────────────────────
# DATE FILTER — Apr 2025 through today (updates automatically every run)
# ─────────────────────────────────────────────
DATE_FROM = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
DATE_TO   = datetime.now(timezone.utc)


def in_date_range(date_str):
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return DATE_FROM <= dt <= DATE_TO
    except Exception:
        return False


def safe_get(url, headers, params=None, max_retries=6):
    """
    Wrapper around requests.get that automatically waits and retries
    on 429 rate-limit responses, honoring ProofHub's retry_after value
    (falls back to exponential backoff if that field isn't present).
    """
    for attempt in range(1, max_retries + 1):
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:
            wait_seconds = 10
            try:
                body = response.json()
                retry_after_raw = str(body.get("retry_after", "")).strip()
                digits = "".join(c for c in retry_after_raw if c.isdigit())
                if digits:
                    wait_seconds = int(digits)
            except Exception:
                pass

            wait_seconds = max(wait_seconds, attempt * 5)  # back off a bit more each retry
            print(f"       ⏳ Rate limited (429). Waiting {wait_seconds}s before retry "
                  f"({attempt}/{max_retries})...")
            time.sleep(wait_seconds)
            continue

        return response

    print(f"       ❌ Gave up after {max_retries} retries due to repeated rate limiting: {url}")
    return response


def extract_list(data, *keys):
    """Extract a list of dicts from API response."""
    if isinstance(data, list):
        return [i for i in data if isinstance(i, dict)]
    if isinstance(data, dict):
        # Try caller-supplied keys first, then common fallbacks
        for key in list(keys) + ["projects", "tasks", "todolists", "data", "items", "results"]:
            if key in data and isinstance(data[key], list):
                return data[key]
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


# ─────────────────────────────────────────────
# STEP 1 — Get ALL projects
# ─────────────────────────────────────────────
def get_all_projects():
    print("Fetching all projects...")
    response = safe_get(f"{BASE_URL}/projects", headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Projects error {response.status_code}: {response.text[:200]}")
        return []

    projects = extract_list(response.json(), "projects")
    print(f"✅ Total projects found: {len(projects)}\n")
    return projects


# ─────────────────────────────────────────────
# STEP 2 — Get todolists for a project
# ─────────────────────────────────────────────
def get_todolists(project_id):
    time.sleep(0.5)  # small gap before each call to stay under rate limits
    response = safe_get(
        f"{BASE_URL}/projects/{project_id}/todolists",
        headers=HEADERS
    )
    if response.status_code != 200:
        print(f"     ❌ Todolists error {response.status_code}: {response.text[:100]}")
        return []
    return extract_list(response.json(), "todolists")


# ─────────────────────────────────────────────
# STEP 3 — Get ALL tasks for a todolist
# ─────────────────────────────────────────────
def get_all_tasks(project_id, todolist_id):
    url      = f"{BASE_URL}/projects/{project_id}/todolists/{todolist_id}/tasks"
    seen_ids = set()
    all_tasks = []

    for params in [{}, {"completed": "true"}]:
        time.sleep(0.5)  # small gap before each call to stay under rate limits
        response = safe_get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            for task in extract_list(response.json(), "tasks"):
                tid = task.get("id")
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    all_tasks.append(task)
        else:
            print(f"       ❌ Tasks error {response.status_code}: {response.text[:100]}")

    return all_tasks


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
projects = get_all_projects()

if not projects:
    print("❌ No projects found. Check API key.")
    exit()

all_rows = []

for proj in projects:
    project_id   = proj.get("id")
    project_name = (
        proj.get("name")
        or proj.get("title")
        or proj.get("project_name")
        or proj.get("label")
        or str(project_id)
    )

    print(f"📁 Project: '{project_name}' (ID: {project_id})")

    todolists = get_todolists(project_id)
    if not todolists:
        print(f"   (no todolists)\n")
        time.sleep(0.3)
        continue

    print(f"   Todolists: {len(todolists)}")

    for tl in todolists:
        tl_id   = tl.get("id")
        tl_name = tl.get("title") or tl.get("name", "Unknown List")

        tasks = get_all_tasks(project_id, tl_id)

        matched = 0
        for task in tasks:

            # ONLY filter: created_at within date range
            if not in_date_range(task.get("created_at", "")):
                continue

            stage         = task.get("stage") or {}
            stage_name    = stage.get("name", "") if isinstance(stage, dict) else ""
            workflow      = task.get("workflow") or {}
            workflow_name = workflow.get("name", "") if isinstance(workflow, dict) else ""
            assigned_ids  = task.get("assigned", [])

            all_rows.append({
                "Project":      project_name,
                "Project ID":   project_id,
                "Task List":    tl_name,
                "Task Title":   task.get("title", ""),
                "Stage":        stage_name,
                "Workflow":     workflow_name,
                "Completed":    task.get("completed", False),
                "Created At":   task.get("created_at", ""),
                "Start Date":   task.get("start_date") or "",
                "Due Date":     task.get("due_date") or "",
                "Assigned IDs": ", ".join(str(i) for i in assigned_ids),
                "Task ID":      task.get("id", ""),
                "Ticket #":     task.get("ticket", ""),
            })
            matched += 1

        if matched:
            print(f"   ✅ '{tl_name}': {matched} tasks in range")

    time.sleep(0.5)  # small gap between projects to stay under rate limits
    print()

# ─────────────────────────────────────────────
# SAVE OUTPUT
# ─────────────────────────────────────────────
if all_rows:
    csv_file  = "proofhub_tasks.csv"
    json_file = "proofhub_tasks.json"

    fieldnames = [
        "Project", "Project ID", "Task List", "Task Title", "Stage", "Workflow",
        "Completed", "Created At", "Start Date", "Due Date",
        "Assigned IDs", "Task ID", "Ticket #"
    ]

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2, default=str)

    print(f"✅ {len(all_rows)} tasks saved to: {csv_file}")
    print(f"📁 Raw JSON saved to:              {json_file}")

    # Summary breakdowns
    print(f"\n📊 Tasks by Project:")
    proj_counts = Counter(r["Project"] for r in all_rows)
    for proj, count in sorted(proj_counts.items(), key=lambda x: -x[1]):
        print(f"   {proj}: {count}")

    print(f"\n📊 Tasks by Stage:")
    for stage, count in sorted(Counter(r["Stage"] for r in all_rows).items(), key=lambda x: -x[1]):
        print(f"   {stage or '(no stage)'}: {count}")

else:
    print("\n⚠ No tasks matched the date range across all projects.")

print("\n" + "=" * 50)
print("DONE")
print("=" * 50)
