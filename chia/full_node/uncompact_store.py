from typing import List, Tuple
import aiosqlite
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.protocols.timelord_protocol import RequestCompactProofOfTime
from chia.util.db_wrapper import DBWrapper
from chia.util.ints import uint8, uint32
import logging

log = logging.getLogger(__name__)


class UncompactStore:
    db: aiosqlite.Connection
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.db_wrapper = db_wrapper
        self.db = db_wrapper.db
        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS uncompacts(header_hash text NOT NULL,"
            " field_vdf tinyint NOT NULL, height bigint, number_of_iterations bigint, request blob,"
            " PRIMARY KEY (header_hash, field_vdf)"
            ")"
        )
        await self.db.execute("CREATE INDEX IF NOT EXISTS uc_height on uncompacts(height)")
        await self.db.commit()
        return self

    async def prune_fully_compactified_blocks(self) -> None:
        cursor = await self.db.execute("""
            WITH RECURSIVE blockchain(header_hash, height) AS (
                SELECT prev_hash, height - 1
                   FROM block_records
                   WHERE is_peak = 1
                UNION ALL
                SELECT br.prev_hash, bc.height - 1
                   FROM block_records br, blockchain bc
                   WHERE bc.header_hash = br.header_hash
                   ORDER BY bc.height - 1 DESC
            )
            DELETE FROM uncompacts WHERE header_hash IN (
                SELECT fb.header_hash
                    FROM full_blocks fb, blockchain bc
                    WHERE fb.header_hash = bc.header_hash
                    AND fb.is_fully_compactified = 1
            )
        """)
        await cursor.close()
        await self.db.commit()

    async def get_new_uncompact_heights(self, limit: int) -> List[uint32]:
        cursor = await self.db.execute(f"""
            WITH RECURSIVE blockchain(header_hash, height) AS (
                SELECT prev_hash, height - 1
                   FROM block_records
                   WHERE is_peak = 1
                UNION ALL
                SELECT br.prev_hash, bc.height - 1
                   FROM block_records br, blockchain bc
                   WHERE bc.header_hash = br.header_hash
                   ORDER BY bc.height - 1 DESC
            )
            SELECT fb.height
                FROM blockchain bc, full_blocks fb
                WHERE bc.header_hash = fb.header_hash
                AND fb.is_fully_compactified = 0
                AND NOT EXISTS (
                    SELECT 1 FROM uncompacts uc WHERE uc.height = fb.height
                )
                LIMIT {limit}
        """)
        rows = await cursor.fetchall()
        await cursor.close()
        return [row[0] for row in rows]

    async def get_uncompact_batch(self, batch_size: int) -> List[RequestCompactProofOfTime]:
        cursor = await self.db.execute(f"""
            SELECT request
                FROM uncompacts
                ORDER BY random()
                LIMIT {batch_size}
        """)
        rows = await cursor.fetchall()
        await cursor.close()
        return [RequestCompactProofOfTime.from_bytes(row[0]) for row in rows]

    async def add_uncompacts(self, uncompacts_list: List[RequestCompactProofOfTime]) -> None:
        cursor = await self.db.executemany(
            "INSERT INTO uncompacts VALUES(?, ?, ?, ?, ?)",
            [(request.header_hash.hex(),
              request.field_vdf,
              request.height,
              request.new_proof_of_time.number_of_iterations,
              bytes(request),
             ) for request in uncompacts_list]
        )
        await cursor.close()
        await self.db.commit()

    async def remove_uncompact(self, header_hash: bytes32, field_vdf: uint8) -> None:
        cursor = await self.db.execute(
            "DELETE FROM uncompacts WHERE header_hash = ? AND field_vdf = ?",
            (header_hash.hex(), field_vdf,)
        )
        await cursor.fetchall()
        await self.db.commit()
