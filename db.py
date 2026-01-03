import logging
import os
from pathlib import Path
from typing import List, Tuple

import psycopg


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required for database access")
    return psycopg.connect(database_url, autocommit=True)


def run_ddl(sql: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def ensure_template_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS narrative_templates (
        id serial primary key,
        name text not null,
        direction text not null,
        strength text not null default 'any',
        leader text not null default 'any',
        template_text text not null,
        is_active boolean not null default true,
        priority int not null default 0
    );
    CREATE INDEX IF NOT EXISTS idx_narrative_templates_name_active ON narrative_templates (name, is_active);
    CREATE INDEX IF NOT EXISTS idx_narrative_templates_name_direction ON narrative_templates (name, direction);
    """
    run_ddl(ddl)


def fetch_templates(name: str, direction: str) -> List[Tuple[int, str, str, str, str]]:
    query = """
        SELECT id, strength, leader, template_text, priority
        FROM narrative_templates
        WHERE name = %s AND direction = %s AND is_active = true
        ORDER BY priority DESC, id ASC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (name, direction))
            return cur.fetchall()


def seed_templates_if_empty(seed_file: Path, name: str) -> None:
    count_query = "SELECT COUNT(*) FROM narrative_templates WHERE name = %s"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_query, (name,))
            (count,) = cur.fetchone()
            if count and count > 0:
                return

            sql_content = seed_file.read_text(encoding="utf-8")
            logging.info("Seeding narrative templates from %s", seed_file)
            cur.execute(sql_content)
