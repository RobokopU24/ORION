import os


def test_pull_via_http():
    from orion.utils import GetData

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    byte_count: int = gd.pull_via_http('https://renci.org/mission-and-vision', data_file_path)

    assert byte_count

    assert(os.path.exists(os.path.join(data_file_path, 'mission-and-vision')))

    os.remove(os.path.join(data_file_path, 'mission-and-vision'))

