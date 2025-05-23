from collections import defaultdict
from typing import Self

from attrs import define, field, frozen
from eth_typing import ChecksumAddress
from hexbytes import HexBytes
from py_flare_common.fsp.epoch.epoch import VotingEpoch
from py_flare_common.fsp.messaging.types import (
    FdcSubmit1,
    FdcSubmit2,
    FtsoSubmit1,
    FtsoSubmit2,
    ParsedPayload,
    SubmitSignatures,
)
from web3.types import BlockData, TxData

from .reward_epoch_manager import Entity
from .types import AttestationRequest, ProtocolMessageRelayed


@frozen
class WTxData:
    wrapped: TxData
    hash: HexBytes
    to_address: ChecksumAddress | None
    input: HexBytes
    block_number: int
    timestamp: int
    transaction_index: int
    from_address: ChecksumAddress
    value: int

    def is_first_or_second(self) -> bool:
        return (
            True
            if self.transaction_index == 0 or self.transaction_index == 1
            else False
        )

    @classmethod
    def from_tx_data(cls, tx_data: TxData, block_data: BlockData) -> Self:
        assert "hash" in tx_data
        assert "input" in tx_data
        assert "blockNumber" in tx_data
        assert "transactionIndex" in tx_data
        assert "from" in tx_data
        assert "value" in tx_data

        assert "timestamp" in block_data

        return cls(
            wrapped=tx_data,
            hash=tx_data["hash"],
            to_address=tx_data.get("to"),
            input=tx_data["input"],
            block_number=tx_data["blockNumber"],
            transaction_index=tx_data["transactionIndex"],
            from_address=tx_data["from"],
            value=tx_data["value"],
            timestamp=block_data["timestamp"],
        )


@frozen
class WParsedPayload[T]:
    parsed_payload: ParsedPayload[T]
    wtx_data: WTxData


@define
class WParsedPayloadList[T]:
    agg: list[WParsedPayload[T]] = field(factory=list)

    def extract_latest(self, r: range) -> WParsedPayload[T] | None:
        latest: WParsedPayload[T] | None = None

        for wpp in self.agg:
            wtx = wpp.wtx_data

            if not (r.start <= wtx.timestamp < r.stop):
                continue

            if latest is None or wtx.timestamp > latest.wtx_data.timestamp:
                latest = wpp

        return latest


@define
class ParsedPayloadMapper[T]:
    by_identity: dict[ChecksumAddress, WParsedPayloadList[T]] = field(
        factory=lambda: defaultdict(WParsedPayloadList)
    )

    def insert(self, r: Entity, wpp: WParsedPayload[T]):
        self.by_identity[r.identity_address].agg.append(wpp)


@define
class VotingRoundProtocol[S1, S2, SS]:
    submit_1: ParsedPayloadMapper[S1] = field(factory=ParsedPayloadMapper)
    submit_2: ParsedPayloadMapper[S2] = field(factory=ParsedPayloadMapper)
    submit_signatures: ParsedPayloadMapper[SS] = field(factory=ParsedPayloadMapper)

    finalization: ProtocolMessageRelayed | None = None

    def insert_submit_1(self, e: Entity, pp: ParsedPayload[S1], wtx: WTxData) -> None:
        self.submit_1.insert(e, WParsedPayload(pp, wtx))

    def insert_submit_2(self, e: Entity, pp: ParsedPayload[S2], wtx: WTxData) -> None:
        self.submit_2.insert(e, WParsedPayload(pp, wtx))

    def insert_submit_signatures(
        self, e: Entity, pp: ParsedPayload[SS], wtx: WTxData
    ) -> None:
        self.submit_signatures.insert(e, WParsedPayload(pp, wtx))


@define
class FtsoVotingRoundProtocol(
    VotingRoundProtocol[FtsoSubmit1, FtsoSubmit2, SubmitSignatures]
):
    pass


@define
class AttestationRequestMapper:
    agg: list[AttestationRequest] = field(factory=list)

    def sorted(self) -> list[AttestationRequest]:
        ret = []
        seen = set()

        for ar in sorted(self.agg, key=lambda x: (x.block, x.log_index)):
            if ar.data in seen:
                continue

            seen.add(ar.data)
            ret.append(ar)

        return list(reversed(ret))


@define
class FdcVotingRoundProtocol(
    VotingRoundProtocol[FdcSubmit1, FdcSubmit2, SubmitSignatures]
):
    requests: AttestationRequestMapper = field(factory=AttestationRequestMapper)
    consensus_bitvote: dict[bytes, int] = field(factory=lambda: defaultdict(int))


@define
class VotingRound:
    # epoch corresponding to the round
    voting_epoch: VotingEpoch

    ftso: FtsoVotingRoundProtocol = field(factory=FtsoVotingRoundProtocol)
    fdc: FdcVotingRoundProtocol = field(factory=FdcVotingRoundProtocol)


@define
class VotingRoundManager:
    finalized: int
    rounds: dict[VotingEpoch, VotingRound] = field(factory=dict)

    def get(self, v: VotingEpoch) -> VotingRound:
        if v not in self.rounds:
            self.rounds[v] = VotingRound(v)
        return self.rounds[v]

    def finalize(self, block: BlockData) -> list[VotingRound]:
        assert "timestamp" in block
        keys = list(self.rounds.keys())

        rounds = []
        for k in keys:
            if k.id <= self.finalized:
                self.rounds.pop(k, None)
                continue

            # 55 is submit sigs deadline, 10 is relay grace, 10 is additional buffer
            round_completed = k.next.end_s < block["timestamp"]

            if round_completed:
                self.finalized = max(self.finalized, k.id)
                rounds.append(self.rounds.pop(k))

        return rounds
