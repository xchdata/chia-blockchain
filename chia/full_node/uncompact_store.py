from typing import List, Tuple
import aiosqlite
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.protocols.timelord_protocol import RequestCompactProofOfTime
from chia.util.db_wrapper import DBWrapper2
from chia.util.ints import uint8, uint32
import logging

log = logging.getLogger(__name__)


class UncompactStore:
    db_wrapper: DBWrapper2

    @classmethod
    async def create(cls, db_wrapper: DBWrapper2):
        self = cls()
        self.db_wrapper = db_wrapper
        async with self.db_wrapper.write_db() as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS uncompacts("
                " header_hash BLOB NOT NULL,"
                " field_vdf TINYINT NOT NULL,"
                " height BIGINT,"
                " number_of_iterations BIGINT,"
                " request BLOB,"
                " enqueued DATE,"
                " PRIMARY KEY (header_hash, field_vdf)"
                ")"
            )
            await db.execute("CREATE INDEX IF NOT EXISTS uc_height on uncompacts(height)")
            await db.execute("CREATE INDEX IF NOT EXISTS uc_height_enqueued on uncompacts(height, enqueued)")
        return self

    async def prune_fully_compactified_blocks(self) -> None:
        async with self.db_wrapper.write_db() as db:
            async with db.execute("""
                DELETE FROM uncompacts WHERE header_hash IN (
                    SELECT header_hash
                        FROM full_blocks
                        WHERE in_main_chain = 1
                        AND is_fully_compactified = 1
                )
            """) as cursor:
                await cursor.fetchall()

    async def get_new_uncompact_heights(self, limit: int) -> List[uint32]:
        async with self.db_wrapper.read_db() as db:
            async with db.execute(f"""
                SELECT fb.height
                    FROM full_blocks fb
                    WHERE fb.in_main_chain = 1
                    AND fb.is_fully_compactified = 0
                    AND NOT EXISTS (
                        SELECT 1 FROM uncompacts uc WHERE uc.height = fb.height
                    )
                    LIMIT {limit}
            """) as cursor:
                rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def prune_enqueued(self, ttl: int) -> None:
        async with self.db_wrapper.write_db() as db:
            async with db.execute(f"""
                UPDATE uncompacts SET enqueued = NULL
                    WHERE enqueued IS NOT NULL AND strftime('%s') - enqueued > {ttl}
            """) as cursor:
                await cursor.fetchall()

    async def mark_enqueued(self, header_hash: bytes, field_vdf: int) -> None:
        async with self.db_wrapper.write_db() as db:
            async with db.execute(f"""
                UPDATE uncompacts SET enqueued = strftime('%s')
                    WHERE header_hash = ? AND field_vdf = ?
            """, (header_hash, field_vdf)) as cursor:
                await cursor.fetchall()

    async def get_uncompact_batch(self, batch_size: int) -> List[RequestCompactProofOfTime]:
        async with self.db_wrapper.read_db() as db:
            async with db.execute(f"""
                WITH
                docompact(yn) AS (
                    SELECT
                        CASE
                            WHEN sum(is_fully_compactified)/cast(count() as float) > 1.00 THEN 0
                            ELSE 1
                        END yn
                        FROM full_blocks
                        WHERE in_main_chain = 1
                )
                SELECT request
                    FROM uncompacts
                    WHERE enqueued IS NULL
                    ORDER BY
                        height DESC
                    LIMIT
                        {batch_size} * (SELECT yn FROM docompact)
                    OFFSET 100
            """) as cursor:
                rows = await cursor.fetchall()
        return [RequestCompactProofOfTime.from_bytes(row[0]) for row in rows]

    async def add_uncompacts(self, uncompacts_list: List[RequestCompactProofOfTime]) -> None:
        async with self.db_wrapper.write_db() as db:
            cursor = await db.executemany(
                "INSERT INTO uncompacts(header_hash, field_vdf, height, number_of_iterations, request)"
                " VALUES(?, ?, ?, ?, ?)",
                [(request.header_hash,
                  request.field_vdf,
                  request.height,
                  request.new_proof_of_time.number_of_iterations,
                  bytes(request),
                 ) for request in uncompacts_list]
            )
            await cursor.close()

    async def remove_uncompact(self, header_hash: bytes32, field_vdf: uint8) -> None:
        async with self.db_wrapper.write_db() as db:
            async with db.execute(
                "DELETE FROM uncompacts WHERE header_hash = ? AND field_vdf = ?",
                (header_hash, field_vdf,)
            ) as cursor:
                await cursor.fetchall()
