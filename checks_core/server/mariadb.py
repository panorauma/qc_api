import asyncio
import os
import json
import aiomysql
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
}

pool: Optional[aiomysql.Pool] = None


# setup global pool
async def init_db_pool():
    global pool
    if pool is None:
        pool = await aiomysql.create_pool(
            **DB_CONFIG,
            minsize=1,
            maxsize=int(os.getenv("DB_POOL_MAXSIZE", 20)),
            autocommit=False,  # enable RLS, handle in update_task
        )


async def close_db_pool():
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()


async def create_task(task_id: str):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO validation_tasks (id, status) VALUES (%s, %s)",
                (task_id, "PENDING"),
            )
        await conn.commit()


async def update_task(task_id, status, result=None, error=None, retries=3):
    for attempt in range(retries):
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE validation_tasks
                        SET status=%s, result_json=%s, error=%s
                        WHERE id=%s
                        """,
                        (
                            status,
                            json.dumps(result) if result else None,
                            error,
                            task_id,
                        ),
                    )
                await conn.commit()
            return
        except aiomysql.OperationalError as e:
            if e.args[0] == 1020 and attempt < retries - 1:
                await asyncio.sleep(0.1)  # backoff
                continue
            raise


async def get_task(task_id: str):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM validation_tasks WHERE id=%s",
                (task_id,),
            )
            return await cur.fetchone()


# create single export table if dne
async def create_table():
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS validation_tasks (
                id VARCHAR(36) PRIMARY KEY,
                status VARCHAR(20) NOT NULL,
                result_json JSON DEFAULT NULL,
                error TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """)
        await conn.commit()
