"""main.py – single‑command CLI (auto‑import)

Run:
    python main.py chat "your question here"
    python main.py chat                # interactive REPL

If `database.db` or the target table is missing, the script automatically
imports **freelancer_earnings_bd.csv** (or a file at path given by env `CSV_PATH`) and then
continues the chat. No separate `import` step is required.
"""

import argparse
import csv
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, List

import openai
from dotenv import load_dotenv

# ── Configuration
OPENAI_MODEL = "gpt-4o-mini"
DATABASE = Path("database.db")
TABLE = "freelancer_earnings_bd"
DEFAULT_CSV = Path(os.getenv("CSV_PATH", "freelancer_earnings_bd.csv"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY", "")
if not openai.api_key:
    log.warning("OPENAI_API_KEY not set; chat disabled.")

# ── Helpers
def _clean(col: str) -> str:
    return col.strip().replace(" ", "_").replace("-", "_").replace(".", "")


def import_csv(csv_file: Path) -> bool:
    if not csv_file.exists():
        log.error("CSV not found: %s", csv_file)
        return False
    try:
        with csv_file.open(encoding="utf-8") as fh, sqlite3.connect(DATABASE) as db:
            rdr = csv.reader(fh)
            header = [_clean(c) for c in next(rdr)]
            cols = ", ".join(f'"{c}"' for c in header)
            db.executescript(
                f'DROP TABLE IF EXISTS "{TABLE}";'
                f'CREATE TABLE "{TABLE}" ({cols});'
            )
            placeholders = ", ".join("?" for _ in header)
            db.executemany(
                f'INSERT INTO "{TABLE}" ({cols}) VALUES ({placeholders})', rdr
            )
        log.info("Imported %s into %s (%s rows)", csv_file.name, DATABASE, db.total_changes)
        return True
    except (csv.Error, sqlite3.Error) as exc:
        log.error("Import failed: %s", exc)
        return False


def db_ready() -> bool:
    if not DATABASE.exists():
        return False
    try:
        with sqlite3.connect(DATABASE) as db:
            cur = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
            )
            return cur.fetchone() is not None
    except sqlite3.Error as exc:
        log.error("DB check failed: %s", exc)
        return False


def run_sql_query(query: str) -> str:
    if not db_ready():
        return "Database not initialised."
    try:
        with sqlite3.connect(DATABASE) as db:
            cur = db.execute(query)
            if query.lstrip().upper().startswith("SELECT"):
                rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur]
                return json.dumps(rows[:20], indent=2) or "No rows."
            db.commit()
            return f"OK ({db.total_changes} changes)"
    except sqlite3.Error as exc:
        return f"SQL error: {exc}"

# ── LLM tool
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_sql_query",
            "description": "Execute SQL against local SQLite DB",
            "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
        },
    }
]
PROMPT = f"Assistant for freelance DB. Use run_sql_query on table '{TABLE}'."

# ── Chat
def chat(prompt: str) -> None:
    if not openai.api_key:
        log.error("Chat disabled (no API key).")
        return
    msgs: List[Dict] = [
        {"role": "system", "content": PROMPT},
        {"role": "user", "content": prompt},
    ]
    for _ in range(5):
        rsp = openai.ChatCompletion.create(model=OPENAI_MODEL, messages=msgs, tools=TOOLS, tool_choice="auto")
        msg = rsp["choices"][0]["message"]
        msgs.append(msg)
        if "tool_calls" not in msg:
            print("\nAssistant:\n" + msg["content"] + "\n")
            return
        for call in msg["tool_calls"]:
            sql = json.loads(call["function"]["arguments"])["query"]
            res = run_sql_query(sql)
            msgs.append({"tool_call_id": call["id"], "role": "tool", "name": "run_sql_query", "content": res})
    log.warning("Tool-call budget exceeded.")

# ── CLI
def main() -> None:
    parser = argparse.ArgumentParser(description="Chat with an LLM over your freelancer CSV")
    parser.add_argument("chat", help="Literal word 'chat'", nargs=1)
    parser.add_argument("prompt", nargs="*", help="Initial question (optional)")
    args = parser.parse_args()

    if not db_ready():
        log.info("Database missing; importing %s", DEFAULT_CSV)
        if not import_csv(DEFAULT_CSV):
            log.error("Cannot continue without database/table.")
            return

    text = " ".join(args.prompt) if args.prompt else ""
    if text:
        chat(text)
    else:
        print("Interactive mode — exit/quit to leave")
        while True:
            try:
                line = input("You: ").strip()
                if line.lower() in {"exit", "quit"}:
                    break
                if line:
                    chat(line)
            except (EOFError, KeyboardInterrupt):
                break


if __name__ == "__main__":
    main()
