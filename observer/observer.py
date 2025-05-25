import time
from typing import Self

from eth_account._utils.signing import to_standard_v
from eth_keys.datatypes import Signature as EthSignature
from py_flare_common.fsp.epoch.epoch import RewardEpoch
from py_flare_common.fsp.messaging import (
    parse_generic_tx,
    parse_submit1_tx,
    parse_submit2_tx,
    parse_submit_signature_tx,
)
from py_flare_common.fsp.messaging.byte_parser import ByteParser
from py_flare_common.fsp.messaging.types import ParsedPayload
from py_flare_common.fsp.messaging.types import Signature as SSignature
from py_flare_common.ftso.commit import commit_hash
from web3 import AsyncWeb3
from web3._utils.events import get_event_data
from web3.middleware import ExtraDataToPOAMiddleware

from configuration.types import (
    Configuration,
)
from observer.reward_epoch_manager import (
    Entity,
    SigningPolicy,
)
from observer.types import (
    AttestationRequest,
    ProtocolMessageRelayed,
    RandomAcquisitionStarted,
    SigningPolicyInitialized,
    VotePowerBlockSelected,
    VoterRegistered,
    VoterRegistrationInfo,
    VoterRemoved,
)

from .message import Message, MessageLevel
from .notification import notify_discord, notify_generic, notify_slack, notify_telegram
from .voting_round import (
    VotingRound,
    VotingRoundManager,
    WTxData,
)

from loguru import logger as LOGGER


class Signature(EthSignature):
    @classmethod
    def from_vrs(cls, s: SSignature) -> Self:
        return cls(
            vrs=(
                to_standard_v(int(s.v, 16)),
                int(s.r, 16),
                int(s.s, 16),
            )
        )


async def find_voter_registration_blocks(
    w: AsyncWeb3,
    current_block_id: int,
    reward_epoch: RewardEpoch,
) -> tuple[int, int]:
    # there are roughly 3600 blocks in an hour
    avg_block_time = 3600 / 3600
    current_ts = int(time.time())

    # find timestamp that is more than 2h30min (=9000s) before start_of_epoch_ts
    target_start_ts = reward_epoch.start_s - 9000
    start_diff = current_ts - target_start_ts

    start_block_id = current_block_id - int(start_diff / avg_block_time)
    block = await w.eth.get_block(start_block_id)
    assert "timestamp" in block
    d = block["timestamp"] - target_start_ts
    while abs(d) > 600:
        start_block_id -= 100 * (d // abs(d))
        block = await w.eth.get_block(start_block_id)
        assert "timestamp" in block
        d = block["timestamp"] - target_start_ts

    # end timestamp is 1h (=3600s) before start_of_epoch_ts
    target_end_ts = reward_epoch.start_s - 3600
    end_diff = current_ts - target_end_ts
    end_block_id = current_block_id - int(end_diff / avg_block_time)

    block = await w.eth.get_block(end_block_id)
    assert "timestamp" in block
    d = block["timestamp"] - target_end_ts
    while abs(d) > 600:
        end_block_id -= 100 * (d // abs(d))
        block = await w.eth.get_block(end_block_id)
        assert "timestamp" in block
        d = block["timestamp"] - target_end_ts

    return (start_block_id, end_block_id)


async def get_signing_policy_events(
    w: AsyncWeb3,
    config: Configuration,
    reward_epoch: RewardEpoch,
    start_block: int,
    end_block: int,
) -> SigningPolicy:
    # reads logs for given blocks for the informations about the signing policy

    builder = SigningPolicy.builder().for_epoch(reward_epoch)

    contracts = [
        config.contracts.VoterRegistry,
        config.contracts.FlareSystemsCalculator,
        config.contracts.Relay,
        config.contracts.FlareSystemsManager,
    ]

    event_names = {
        # relay
        "SigningPolicyInitialized",
        # flare systems calculator
        "VoterRegistrationInfo",
        # flare systems manager
        "RandomAcquisitionStarted",
        "VotePowerBlockSelected",
        "VoterRegistered",
        "VoterRemoved",
    }
    event_signatures = {
        e.signature: e
        for c in contracts
        for e in c.events.values()
        if e.name in event_names
    }

    block_logs = await w.eth.get_logs(
        {
            "address": [contract.address for contract in contracts],
            "fromBlock": start_block,
            "toBlock": end_block,
        }
    )

    for log in block_logs:
        sig = log["topics"][0]

        if sig.hex() not in event_signatures:
            continue

        event = event_signatures[sig.hex()]
        data = get_event_data(w.eth.codec, event.abi, log)

        match event.name:
            case "VoterRegistered":
                e = VoterRegistered.from_dict(data["args"])
            case "VoterRemoved":
                e = VoterRemoved.from_dict(data["args"])
            case "VoterRegistrationInfo":
                e = VoterRegistrationInfo.from_dict(data["args"])
            case "SigningPolicyInitialized":
                e = SigningPolicyInitialized.from_dict(data["args"])
            case "VotePowerBlockSelected":
                e = VotePowerBlockSelected.from_dict(data["args"])
            case "RandomAcquisitionStarted":
                e = RandomAcquisitionStarted.from_dict(data["args"])
            case x:
                raise ValueError(f"Unexpected event {x}")

        builder.add(e)

        # signing policy initialized is the last event that gets emitted
        if event.name == "SigningPolicyInitialized":
            break

    return builder.build()


def log_issue(config: Configuration, issue: Message):
    LOGGER.log(issue.level.value, issue.message)

    n = config.notification

    if n.discord is not None:
        notify_discord(n.discord, issue.level.name + " " + issue.message)

    if n.slack is not None:
        notify_slack(n.slack, issue.level.name + " " + issue.message)

    if n.telegram is not None:
        notify_telegram(n.telegram, issue.level.name + " " + issue.message)

    if n.generic is not None:
        notify_generic(n.generic, issue)


def extract[T](
    payloads: list[tuple[ParsedPayload[T], WTxData]],
    round: int,
    time_range: range,
) -> tuple[ParsedPayload[T], WTxData] | None:
    if not payloads:
        return

    latest: tuple[ParsedPayload[T], WTxData] | None = None

    for pl, wtx in payloads:
        if pl.voting_round_id != round:
            continue
        if not (time_range.start <= wtx.timestamp < time_range.stop):
            continue

        if latest is None or wtx.timestamp > latest[1].timestamp:
            latest = (pl, wtx)

    return latest


def validate_ftso(round: VotingRound, entity: Entity, config: Configuration):
    mb = Message.builder().add(
        network=config.chain_id,
        round=round.voting_epoch,
        protocol=100,
    )

    epoch = round.voting_epoch
    ftso = round.ftso
    finalization = ftso.finalization

    _submit1 = ftso.submit_1.by_identity[entity.identity_address]
    submit_1 = _submit1.extract_latest(range(epoch.start_s, epoch.end_s))

    _submit2 = ftso.submit_2.by_identity[entity.identity_address]
    submit_2 = _submit2.extract_latest(
        range(epoch.next.start_s, epoch.next.reveal_deadline())
    )

    sig_grace = max(
        epoch.next.start_s + 55 + 1, (finalization and finalization.timestamp + 1) or 0
    )
    _submit_sig = ftso.submit_signatures.by_identity[entity.identity_address]
    submit_sig = _submit_sig.extract_latest(
        range(epoch.next.reveal_deadline(), sig_grace)
    )

    # TODO:(matej) check for transactions that happened too late (or too early)

    issues = []

    s1 = submit_1 is not None
    s2 = submit_2 is not None
    ss = submit_sig is not None

    if not s1:
        issues.append(mb.build(MessageLevel.INFO, "no submit1 transaction"))

    if s1 and not s2:
        issues.append(
            mb.build(
                MessageLevel.CRITICAL, "no submit2 transaction, causing reveal offence"
            )
        )

    if s2:
        indices = [
            str(i)
            for i, v in enumerate(submit_2.parsed_payload.payload.values)
            if v is None
        ]

        if indices:
            issues.append(
                mb.build(
                    MessageLevel.WARNING,
                    f"submit 2 had 'None' on indices {', '.join(indices)}",
                )
            )

    if s1 and s2:
        # TODO:(matej) should just build back from parsed message
        bp = ByteParser(parse_generic_tx(submit_2.wtx_data.input).ftso.payload)
        rnd = bp.uint256()
        feed_v = bp.drain()

        hashed = commit_hash(entity.submit_address, epoch.id, rnd, feed_v)

        if submit_1.parsed_payload.payload.commit_hash.hex() != hashed:
            issues.append(
                mb.build(
                    MessageLevel.CRITICAL,
                    "commit hash and reveal didn't match, causing reveal offence",
                ),
            )

    if not ss:
        issues.append(
            mb.build(MessageLevel.ERROR, "no submit signatures transaction"),
        )

    if finalization and ss:
        s = Signature.from_vrs(submit_sig.parsed_payload.payload.signature)
        addr = s.recover_public_key_from_msg_hash(
            finalization.to_message()
        ).to_checksum_address()

        if addr != entity.signing_policy_address:
            issues.append(
                mb.build(
                    MessageLevel.ERROR,
                    "submit signatures signature doesn't match finalization",
                ),
            )

    return issues


def validate_fdc(round: VotingRound, entity: Entity, config: Configuration):
    mb = Message.builder().add(
        network=config.chain_id,
        round=round.voting_epoch,
        protocol=200,
    )

    epoch = round.voting_epoch
    fdc = round.fdc
    finalization = fdc.finalization

    _submit1 = fdc.submit_1.by_identity[entity.identity_address]
    submit_1 = _submit1.extract_latest(range(epoch.start_s, epoch.end_s))

    _submit2 = fdc.submit_2.by_identity[entity.identity_address]
    submit_2 = _submit2.extract_latest(
        range(epoch.next.start_s, epoch.next.reveal_deadline())
    )

    sig_grace = max(
        epoch.next.start_s + 55 + 1, (finalization and finalization.timestamp + 1) or 0
    )
    _submit_sig = fdc.submit_signatures.by_identity[entity.identity_address]
    submit_sig = _submit_sig.extract_latest(
        range(epoch.next.reveal_deadline(), sig_grace)
    )
    submit_sig_deadline = _submit_sig.extract_latest(
        range(epoch.next.reveal_deadline(), epoch.next.end_s)
    )

    # TODO:(matej) move this to py-flare-common
    bp = ByteParser(
        sorted(fdc.consensus_bitvote.items(), key=lambda x: x[1], reverse=True)[0][0]
    )
    n_requests = bp.uint16()
    votes = bp.drain()
    consensus_bitvote = [False for _ in range(n_requests)]
    for j, byte in enumerate(reversed(votes)):
        for shift in range(8):
            i = n_requests - 1 - j * 8 - shift
            if i < 0 and (byte >> shift) & 1 == 1:
                raise ValueError("Invalid payload length.")
            elif i >= 0:
                consensus_bitvote[i] = (byte >> shift) & 1 == 1

    # TODO:(matej) check for transactions that happened too late (or too early)

    issues = []

    s1 = submit_1 is not None
    s2 = submit_2 is not None
    ss = submit_sig is not None
    ssd = submit_sig_deadline is not None

    sorted_requests = fdc.requests.sorted()
    assert len(sorted_requests) == n_requests

    if not s1:
        # NOTE:(matej) this is expected behaviour in fdc
        pass

    if not s2:
        issues.append(mb.build(MessageLevel.ERROR, "no submit2 transaction"))

    expected_signatures = True
    # TODO:(matej) unnest some
    if s2:
        if submit_2.parsed_payload.payload.number_of_requests != len(sorted_requests):
            issues.append(
                mb.build(
                    MessageLevel.ERROR,
                    "submit 2 length didn't match number of requests in round",
                )
            )
            expected_signatures = False
        else:
            for i, (r, bit, cbit) in enumerate(
                zip(
                    sorted_requests,
                    submit_2.parsed_payload.payload.bit_vector,
                    consensus_bitvote,
                )
            ):
                idx = n_requests - 1 - i
                at = r.attestation_type
                si = r.source_id

                if cbit and not bit:
                    issues.append(
                        mb.build(
                            MessageLevel.ERROR,
                            "submit2 didn't confirm request that was part of consensus "
                            f"{at.representation}/{si.representation} at index {idx}",
                        )
                    )
                    expected_signatures = False

    if s2 and expected_signatures and not ssd:
        issues.append(
            mb.build(
                MessageLevel.CRITICAL,
                "no submit signatures transaction, causing reveal offence",
            )
        )

    if s2 and ssd and not ss:
        issues.append(
            mb.build(
                MessageLevel.ERROR,
                (
                    "no submit signatures transaction during grace period, "
                    "causing loss of rewards"
                ),
            )
        )

    if not s2 and not ss:
        issues.append(
            mb.build(MessageLevel.ERROR, "no submit signatures transaction"),
        )

    if finalization and ss:
        s = Signature.from_vrs(submit_sig.parsed_payload.payload.signature)
        addr = s.recover_public_key_from_msg_hash(
            finalization.to_message()
        ).to_checksum_address()

        if addr != entity.signing_policy_address:
            issues.append(
                mb.build(
                    MessageLevel.ERROR,
                    "submit signatures signature doesn't match finalization",
                )
            )

    return issues


async def observer_loop(config: Configuration) -> None:
    w = AsyncWeb3(
        AsyncWeb3.AsyncHTTPProvider(config.rpc_url),
        middleware=[ExtraDataToPOAMiddleware],
    )

    # log_issue(
    #     config,
    #     Issue(
    #         IssueLevel.INFO,
    #         MessageBuilder()
    #         .add_network(config.chain_id)
    #         .add_protocol(100)
    #         .add_round(VotingEpoch(12, None))
    #         .build_with_message("testing message" + str(config.notification)),
    #     ),
    # )
    # return

    # reasignments for quick access
    ve = config.epoch.voting_epoch
    # re = config.epoch.reward_epoch
    vef = config.epoch.voting_epoch_factory
    ref = config.epoch.reward_epoch_factory

    # get current voting round and reward epoch
    block = await w.eth.get_block("latest")
    assert "timestamp" in block
    assert "number" in block
    reward_epoch = ref.from_timestamp(block["timestamp"])
    voting_epoch = vef.from_timestamp(block["timestamp"])

    # we first fill signing policy for current reward epoch

    # voter registration period is 2h before the reward epoch and lasts 30min
    # find block that has timestamp approx. 2h30min before the reward epoch
    # and block that has timestamp approx. 1h before the reward epoch
    lower_block_id, end_block_id = await find_voter_registration_blocks(
        w, block["number"], reward_epoch
    )

    # get informations for events that build the current signing policy
    signing_policy = await get_signing_policy_events(
        w,
        config,
        reward_epoch,
        lower_block_id,
        end_block_id,
    )
    spb = SigningPolicy.builder()

    # print("Signing policy created for reward epoch", current_rid)
    # print("Reward Epoch object created", reward_epoch_info)
    # print("Current Reward Epoch status", reward_epoch_info.status(config))

    # set up target address from config
    tia = w.to_checksum_address(config.identity_address)
    # TODO:(matej) log version and initial voting round, maybe signing policy info
    log_issue(
        config,
        Message.builder()
        .add(network=config.chain_id)
        .build(
            MessageLevel.INFO,
            f"Initialized observer for identity_address={tia}",
        ),
    )
    # target_voter = signing_policy.entity_mapper.by_identity_address[tia]
    # notify_discord(
    #     config,
    #     f"flare-observer initialized\n\n"
    #     f"chain: {config.chain}\n"
    #     f"submit address: {target_voter.submit_address}\n"
    #     f"submit signatures address: {target_voter.submit_signatures_address}\n",
    #     # f"this address has voting power of: {signing_policy.voter_weight(tia)}\n\n"
    #     # f"starting in voting round: {voting_round.next.id} "
    #     # f"(current: {voting_round.id})\n"
    #     # f"current reward epoch: {current_rid}",
    # )

    # wait until next voting epoch
    block_number = block["number"]
    while True:
        latest_block = await w.eth.block_number
        if block_number == latest_block:
            time.sleep(2)
            continue

        block_number += 1
        block_data = await w.eth.get_block(block_number)

        assert "timestamp" in block_data

        _ve = vef.from_timestamp(block_data["timestamp"])
        if _ve == voting_epoch.next:
            voting_epoch = voting_epoch.next
            break

    vrm = VotingRoundManager(voting_epoch.previous.id)

    # set up contracts and events (from config)
    # TODO: (nejc) set this up with a function on class
    # or contracts = attrs.asdict(config.contracts) <- this doesn't work
    contracts = [
        config.contracts.Relay,
        config.contracts.VoterRegistry,
        config.contracts.FlareSystemsManager,
        config.contracts.FlareSystemsCalculator,
        config.contracts.FdcHub,
    ]
    event_signatures = {e.signature: e for c in contracts for e in c.events.values()}

    # start listener
    # print("Listener started from block number", block_number)
    # check transactions for submit transactions
    target_function_signatures = {
        config.contracts.Submission.functions[
            "submitSignatures"
        ].signature: "submitSignatures",
        config.contracts.Submission.functions["submit1"].signature: "submit1",
        config.contracts.Submission.functions["submit2"].signature: "submit2",
    }

    while True:
        latest_block = await w.eth.block_number
        if block_number == latest_block:
            time.sleep(2)
            continue

        for block in range(block_number, latest_block):
            LOGGER.debug(f"processing {block}")
            block_data = await w.eth.get_block(block, full_transactions=True)
            assert "transactions" in block_data
            assert "timestamp" in block_data
            block_ts = block_data["timestamp"]

            voting_epoch = vef.from_timestamp(block_ts)

            if (
                spb.signing_policy_initialized is not None
                and spb.signing_policy_initialized.start_voting_round_id == voting_epoch
            ):
                # TODO:(matej) this could fail if the observer is started during
                # last two hours of the reward epoch
                signing_policy = spb.build()
                spb = SigningPolicy.builder().for_epoch(
                    signing_policy.reward_epoch.next
                )

            block_logs = await w.eth.get_logs(
                {
                    "address": [contract.address for contract in contracts],
                    "fromBlock": block,
                    "toBlock": block,
                }
            )

            for log in block_logs:
                sig = log["topics"][0]

                if sig.hex() in event_signatures:
                    event = event_signatures[sig.hex()]
                    data = get_event_data(w.eth.codec, event.abi, log)
                    match event.name:
                        case "ProtocolMessageRelayed":
                            e = ProtocolMessageRelayed.from_dict(
                                data["args"], block_data
                            )
                            voting_round = vrm.get(ve(e.voting_round_id))
                            if e.protocol_id == 100:
                                voting_round.ftso.finalization = e
                            if e.protocol_id == 200:
                                voting_round.fdc.finalization = e

                        case "AttestationRequest":
                            e = AttestationRequest.from_dict(data, voting_epoch)
                            vrm.get(e.voting_epoch_id).fdc.requests.agg.append(e)

                        case "SigningPolicyInitialized":
                            e = SigningPolicyInitialized.from_dict(data["args"])
                            spb.add(e)
                        case "VoterRegistered":
                            e = VoterRegistered.from_dict(data["args"])
                            spb.add(e)
                        case "VoterRemoved":
                            e = VoterRemoved.from_dict(data["args"])
                            spb.add(e)
                        case "VoterRegistrationInfo":
                            e = VoterRegistrationInfo.from_dict(data["args"])
                            spb.add(e)
                        case "VotePowerBlockSelected":
                            e = VotePowerBlockSelected.from_dict(data["args"])
                            spb.add(e)
                        case "RandomAcquisitionStarted":
                            e = RandomAcquisitionStarted.from_dict(data["args"])
                            spb.add(e)

            for tx in block_data["transactions"]:
                assert not isinstance(tx, bytes)
                wtx = WTxData.from_tx_data(tx, block_data)

                called_function_sig = wtx.input[:4].hex()
                input = wtx.input[4:].hex()
                sender_address = wtx.from_address
                entity = signing_policy.entity_mapper.by_omni.get(sender_address)
                if entity is None:
                    continue

                if called_function_sig in target_function_signatures:
                    mode = target_function_signatures[called_function_sig]
                    match mode:
                        case "submit1":
                            try:
                                parsed = parse_submit1_tx(input)
                                if parsed.ftso is not None:
                                    vrm.get(
                                        ve(parsed.ftso.voting_round_id)
                                    ).ftso.insert_submit_1(entity, parsed.ftso, wtx)
                                if parsed.fdc is not None:
                                    vrm.get(
                                        ve(parsed.fdc.voting_round_id)
                                    ).fdc.insert_submit_1(entity, parsed.fdc, wtx)
                            except Exception:
                                pass

                        case "submit2":
                            try:
                                parsed = parse_submit2_tx(input)
                                if parsed.ftso is not None:
                                    vrm.get(
                                        ve(parsed.ftso.voting_round_id)
                                    ).ftso.insert_submit_2(entity, parsed.ftso, wtx)
                                if parsed.fdc is not None:
                                    vrm.get(
                                        ve(parsed.fdc.voting_round_id)
                                    ).fdc.insert_submit_2(entity, parsed.fdc, wtx)
                            except Exception:
                                pass

                        case "submitSignatures":
                            try:
                                parsed = parse_submit_signature_tx(input)
                                if parsed.ftso is not None:
                                    vrm.get(
                                        ve(parsed.ftso.voting_round_id)
                                    ).ftso.insert_submit_signatures(
                                        entity, parsed.ftso, wtx
                                    )
                                if parsed.fdc is not None:
                                    vr = vrm.get(ve(parsed.fdc.voting_round_id))
                                    vr.fdc.insert_submit_signatures(
                                        entity, parsed.fdc, wtx
                                    )

                                    # NOTE:(matej) this is currently the easies way to
                                    # get consensus bitvote
                                    vr.fdc.consensus_bitvote[
                                        parsed.fdc.payload.unsigned_message
                                    ] += 1

                            except Exception:
                                pass

            rounds = vrm.finalize(block_data)
            for r in rounds:
                for i in validate_ftso(
                    r, signing_policy.entity_mapper.by_identity_address[tia], config
                ):
                    log_issue(config, i)
                for i in validate_fdc(
                    r, signing_policy.entity_mapper.by_identity_address[tia], config
                ):
                    log_issue(config, i)

        block_number = latest_block
