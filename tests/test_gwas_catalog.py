
from parsers.GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader


def test_gwas_catalog_update_version():
    # this is not a great test, it relies on the real gwas catalog service
    # ideally we'd mock up a fake gwas catalog FTP
    g_cat = GWASCatalogLoader()
    latest_version = g_cat.get_latest_source_version()
    assert int(latest_version)


def not_active_test_gwas_catalog_load():
    g_cat = GWASCatalogLoader()
    g_cat.load()
