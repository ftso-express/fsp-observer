from typing import Self

from attrs import define, field, frozen
from eth_typing import ChecksumAddress
from hexbytes import HexBytes
from py_flare_common.fsp.epoch.epoch import RewardEpoch, VotingEpoch
from py_flare_common.fsp.messaging.types import (
    FdcSubmit1,
    FdcSubmit2,
    FtsoSubmit1,
    FtsoSubmit2,
    ParsedPayload,
    SubmitSignatures,
)
from web3.types import BlockData, TxData

from .types import (
    ProtocolMessageRelayed,
    RandomAcquisitionStarted,
    SigningPolicyInitialized,
    VotePowerBlockSelected,
    VoterRegistered,
    VoterRegistrationInfo,
    VoterRemoved,
)


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
class Node:
    node_id: str
    weight: int


@frozen
class Entity:
    identity_address: ChecksumAddress
    submit_address: ChecksumAddress
    submit_signatures_address: ChecksumAddress
    signing_policy_address: ChecksumAddress
    delegation_address: ChecksumAddress

    public_key: str
    nodes: list[Node]

    delegation_fee_bips: int

    w_nat_weight: int
    w_nat_capped_weight: int

    # used for internal calculation, (capped + stake) ** 3/4
    registration_weight: int

    # this is emitted in signing policy initialized event
    normalized_weight: int


@frozen
class EntityMapper:
    by_identity_address: dict[ChecksumAddress, Entity] = field(factory=dict)
    by_submit_address: dict[ChecksumAddress, Entity] = field(factory=dict)
    by_submit_signatures_address: dict[ChecksumAddress, Entity] = field(factory=dict)
    by_signing_policy_address: dict[ChecksumAddress, Entity] = field(factory=dict)
    by_delegation_address: dict[ChecksumAddress, Entity] = field(factory=dict)
    by_omni: dict[ChecksumAddress, Entity] = field(factory=dict)

    def insert(self, e: Entity):
        self.by_identity_address[e.identity_address] = e
        self.by_submit_address[e.submit_address] = e
        self.by_submit_signatures_address[e.submit_signatures_address] = e
        self.by_signing_policy_address[e.signing_policy_address] = e
        self.by_delegation_address[e.delegation_address] = e

        self.by_omni[e.identity_address] = e
        self.by_omni[e.submit_address] = e
        self.by_omni[e.submit_signatures_address] = e
        self.by_omni[e.signing_policy_address] = e
        self.by_omni[e.delegation_address] = e


@frozen
class SigningPolicy:
    reward_epoch: RewardEpoch

    vote_power_block: int
    start_voting_round: int

    threshold: int
    seed: int
    signing_policy_bytes: str

    entities: list[Entity]
    entity_mapper: EntityMapper

    @classmethod
    def builder(cls) -> "SigningPolicyBuilder":
        return SigningPolicyBuilder()


@define
class SigningPolicyBuilder:
    reward_epoch: RewardEpoch | None = None

    random_acquisation_started: RandomAcquisitionStarted | None = None
    vote_power_block_selected: VotePowerBlockSelected | None = None

    voter_registered: list[VoterRegistered] = field(factory=list)
    voter_registration_info: list[VoterRegistrationInfo] = field(factory=list)
    voter_removed: list[VoterRemoved] = field(factory=list)

    signing_policy_initialized: SigningPolicyInitialized | None = None

    def for_epoch(self, r: RewardEpoch) -> Self:
        self.reward_epoch = r
        return self

    def add(
        self,
        event: RandomAcquisitionStarted
        | VotePowerBlockSelected
        | VoterRegistered
        | VoterRegistrationInfo
        | VoterRemoved
        | SigningPolicyInitialized,
    ) -> Self:
        if isinstance(event, RandomAcquisitionStarted):
            assert self.random_acquisation_started is None
            self.random_acquisation_started = event

        if isinstance(event, VotePowerBlockSelected):
            assert self.vote_power_block_selected is None
            self.vote_power_block_selected = event

        if isinstance(event, VoterRegistered):
            self.voter_registered.append(event)

        if isinstance(event, VoterRegistrationInfo):
            self.voter_registration_info.append(event)

        if isinstance(event, VoterRemoved):
            self.voter_removed.append(event)

        if isinstance(event, SigningPolicyInitialized):
            assert self.signing_policy_initialized is None
            self.signing_policy_initialized = event

        return self

    # def status(self, config: Configuration) -> str | None:
    #     ts_now = int(time.time())
    #     next_expected_ts = config.epoch.reward_epoch(self.id + 1).start_s
    #
    #     # current reads
    #     ras = self.random_acquisition_started
    #     vpbs = self.vote_power_block_selected
    #     sp = self.signing_policy
    #
    #     if not ras:
    #         return "collecting offers"
    #
    #     if ras and vpbs is None:
    #         return "selecting snapshot"
    #
    #     if vpbs is not None and sp is None:
    #         return "voter registration"
    #
    #     if sp is not None:
    #         svrs = config.epoch.voting_epoch(sp.start_voting_round_id).start_s
    #         if svrs > ts_now:
    #             return "ready for start"
    #
    #         # here svrs < ts_now
    #         if next_expected_ts > ts_now:
    #             return "active"
    #
    #         if next_expected_ts < ts_now:
    #             return "extended"

    def build(self) -> SigningPolicy:
        assert self.reward_epoch is not None
        rid = self.reward_epoch.id

        assert self.random_acquisation_started is not None
        assert self.random_acquisation_started.reward_epoch_id == rid

        assert self.vote_power_block_selected is not None
        assert self.vote_power_block_selected.reward_epoch_id == rid

        assert self.signing_policy_initialized is not None
        assert self.signing_policy_initialized.reward_epoch_id == rid

        assert len(self.voter_registered) == len(self.voter_registration_info)

        spa = {v.signing_policy_address: v.voter for v in self.voter_registered}
        vres = {v.voter: v for v in self.voter_registered}
        vries = {v.voter: v for v in self.voter_registration_info}

        entities = []
        mapper = EntityMapper()

        for i, voter in enumerate(self.signing_policy_initialized.voters):
            weight = self.signing_policy_initialized.weights[i]
            vre = vres[spa[voter]]
            vrie = vries[spa[voter]]

            nodes = []
            for n, w in zip(vrie.node_ids, vrie.node_weights):
                nodes.append(Node(n, w))

            entity = Entity(
                identity_address=vre.voter,
                submit_address=vre.submit_address,
                submit_signatures_address=vre.submit_signatures_address,
                signing_policy_address=vre.signing_policy_address,
                delegation_address=vrie.delegation_address,
                public_key=vre.public_key,
                nodes=nodes,
                delegation_fee_bips=vrie.delegation_fee_bips,
                w_nat_weight=vrie.w_nat_weight,
                w_nat_capped_weight=vrie.w_nat_capped_weight,
                registration_weight=vre.registration_weight,
                normalized_weight=weight,
            )

            entities.append(entity)
            mapper.insert(entity)

        return SigningPolicy(
            reward_epoch=self.reward_epoch,
            vote_power_block=self.vote_power_block_selected.vote_power_block,
            start_voting_round=self.signing_policy_initialized.start_voting_round_id,
            threshold=self.signing_policy_initialized.threshold,
            seed=self.signing_policy_initialized.seed,
            signing_policy_bytes=self.signing_policy_initialized.signing_policy_bytes,
            entities=entities,
            entity_mapper=mapper,
        )


@define
class ParsedPayloadMapper[T]:
    by_identity: dict[ChecksumAddress, list[tuple[ParsedPayload[T], WTxData]]] = field(
        factory=dict
    )
    # by_submit: dict[ChecksumAddress, list[ParsedMessage[T, U]]] = field(factory=dict)
    # by_signatures: dict[ChecksumAddress, list[ParsedMessage[T, U]]] = field(
    #     factory=dict
    # )
    # by_signing: dict[ChecksumAddress, list[ParsedMessage[T, U]]] = field(factory=dict)
    # by_delegation: dict[ChecksumAddress, list[ParsedMessage[T, U]]] = field(
    #     factory=dict
    # )

    def insert(self, r: Entity, s: ParsedPayload[T], tx: WTxData):
        if r.identity_address not in self.by_identity:
            self.by_identity[r.identity_address] = []
        self.by_identity[r.identity_address].append((s, tx))


@define
class VotingRoundProtocol[S1, S2, SS]:
    submit_1: ParsedPayloadMapper[S1] = field(factory=ParsedPayloadMapper)
    submit_2: ParsedPayloadMapper[S2] = field(factory=ParsedPayloadMapper)
    submit_signatures: ParsedPayloadMapper[SS] = field(factory=ParsedPayloadMapper)
    finalization: ProtocolMessageRelayed | None = None

    def insert_submit_1(self, e: Entity, s: ParsedPayload[S1], tx: WTxData) -> None:
        self.submit_1.insert(e, s, tx)

    def insert_submit_2(self, e: Entity, s: ParsedPayload[S2], tx: WTxData) -> None:
        self.submit_2.insert(e, s, tx)

    def insert_submit_signatures(
        self, e: Entity, s: ParsedPayload[SS], tx: WTxData
    ) -> None:
        self.submit_signatures.insert(e, s, tx)


@define
class VotingRound:
    # epoch corresponding to the round
    voting_epoch: VotingEpoch

    ftso: VotingRoundProtocol[FtsoSubmit1, FtsoSubmit2, SubmitSignatures] = field(
        factory=VotingRoundProtocol
    )
    fdc: VotingRoundProtocol[FdcSubmit1, FdcSubmit2, SubmitSignatures] = field(
        factory=VotingRoundProtocol
    )


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

            # need to wait until end of next epoch for fdc reveal offence condition
            round_completed = k.next.end_s < block["timestamp"]

            if round_completed:
                self.finalized = max(self.finalized, k.id)
                rounds.append(self.rounds.pop(k))

        return rounds
