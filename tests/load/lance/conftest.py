from tests.utils import ACTIVE_DESTINATIONS

if "lance" in ACTIVE_DESTINATIONS:
    from tests.load.lance.lance_utils import lance_rest_server, cleanup_lance_namespace_root
else:
    # do not collect lance tests
    collect_ignore_glob = ["*"]
