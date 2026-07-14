import gzip
import os

from orion.kgx_bundle import KGXBundle


def _write(path, content: bytes):
    with open(path, 'wb') as f:
        f.write(content)


def test_compress_then_decompress_round_trip(tmp_path):
    bundle_dir = str(tmp_path)
    nodes_content = b'{"id":"n1"}\n{"id":"n2"}\n'
    edges_content = b'{"id":"e1"}\n'
    _write(os.path.join(bundle_dir, KGXBundle.NODES_FILENAME), nodes_content)
    _write(os.path.join(bundle_dir, KGXBundle.EDGES_FILENAME), edges_content)

    bundle = KGXBundle(bundle_dir)
    assert bundle.has_nodes_and_edges()

    # compress: raw jsonl -> valid .gz, originals removed
    bundle.compress_nodes_and_edges()
    assert sorted(os.listdir(bundle_dir)) == ['edges.jsonl.gz', 'nodes.jsonl.gz']
    assert bundle.nodes_path.endswith('.gz')
    with gzip.open(bundle.nodes_path, 'rb') as f:
        assert f.read() == nodes_content

    # decompress: back to raw jsonl, .gz removed, content preserved
    bundle.decompress_nodes_and_edges()
    assert sorted(os.listdir(bundle_dir)) == ['edges.jsonl', 'nodes.jsonl']
    assert bundle.nodes_path.endswith(KGXBundle.NODES_FILENAME)
    assert not bundle.nodes_path.endswith('.gz')
    with open(bundle.nodes_path, 'rb') as f:
        assert f.read() == nodes_content
    with open(bundle.edges_path, 'rb') as f:
        assert f.read() == edges_content


# Regression: decompressing a compressed bundle must produce raw jsonl (not a
# mislabeled '.gz' file still holding uncompressed bytes) so downstream readers
# and the registry get a genuinely gzipped artifact after re-compression.
def test_decompress_leaves_valid_raw_jsonl(tmp_path):
    bundle_dir = str(tmp_path)
    nodes_content = b'{"id":"n1"}\n'
    with gzip.open(os.path.join(bundle_dir, KGXBundle.NODES_FILENAME + '.gz'), 'wb') as f:
        f.write(nodes_content)
    with gzip.open(os.path.join(bundle_dir, KGXBundle.EDGES_FILENAME + '.gz'), 'wb') as f:
        f.write(b'{"id":"e1"}\n')

    bundle = KGXBundle(bundle_dir)
    bundle.decompress_nodes_and_edges()

    raw_nodes = os.path.join(bundle_dir, KGXBundle.NODES_FILENAME)
    assert os.path.exists(raw_nodes)
    assert not os.path.exists(raw_nodes + '.gz')
    with open(raw_nodes, 'rb') as f:
        content = f.read()
    assert content == nodes_content
    # the decompressed file must be plain text, never gzip-magic-prefixed
    assert content[:2] != b'\x1f\x8b'