from GTEx.src.gtex_loader import GTExLoader

# TODO use argparse to specify output location
if __name__ == '__main__':
    loader = GTExLoader()
    loader.load('.', 'gtex_sqtl', load_sqtl=True)
    loader.load('.', 'gtex_eqtl', load_sqtl=False)

