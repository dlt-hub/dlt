from dlt.common.versioned_state import (
    generate_state_version_hash,
    bump_state_version_if_modified,
    default_versioned_state,
)


def test_versioned_state() -> None:
    state = default_versioned_state()
    assert state["_state_version"] == 0
    assert state["_state_engine_version"] == 1

    # first hash generation does not change version, attrs are not modified
    version, hash, previous_hash = bump_state_version_if_modified(state)
    assert version == 0
    assert hash is not None
    assert previous_hash is None
    assert state["_version_hash"] == hash

    # change attr, but exclude while generating
    state["foo"] = "bar"  # type: ignore
    version, hash, previous_hash = bump_state_version_if_modified(state, exclude_attrs=["foo"])
    assert version == 0
    assert hash == previous_hash

    # now don't exclude (remember old hash to compare return vars)
    old_hash = state["_version_hash"]
    version, hash, previous_hash = bump_state_version_if_modified(state)
    assert version == 1
    assert hash != previous_hash
    assert old_hash != hash
    assert previous_hash == old_hash

    # messing with state engine version will not change hash
    state["_state_engine_version"] = 5
    version, hash, previous_hash = bump_state_version_if_modified(state)
    assert version == 1
    assert hash == previous_hash

    # make sure state object is not modified while bumping with no effect
    old_state = state.copy()
    version, hash, previous_hash = bump_state_version_if_modified(state)
    assert old_state == state
