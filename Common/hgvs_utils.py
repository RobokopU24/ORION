
# look up for reference chromosomes for HGVS conversion
REFERENCE_CHROMOSOME_LOOKUP: dict = {
    'b37': {
        'p1': {
            1: 'NC_000001.10', 2: 'NC_000002.11', 3: 'NC_000003.11', 4: 'NC_000004.11', 5: 'NC_000005.9',
            6: 'NC_000006.11', 7: 'NC_000007.13', 8: 'NC_000008.10', 9: 'NC_000009.11', 10: 'NC_000010.10',
            11: 'NC_000011.9', 12: 'NC_000012.11', 13: 'NC_000013.10', 14: 'NC_000014.8', 15: 'NC_000015.9',
            16: 'NC_000016.9', 17: 'NC_000017.10', 18: 'NC_000018.9', 19: 'NC_000019.9', 20: 'NC_000020.10',
            21: 'NC_000021.8', 22: 'NC_000022.10', 23: 'NC_000023.10', 24: 'NC_000024.9'
        }
    },
    'b38': {
        'p1': {
            1: 'NC_000001.11', 2: 'NC_000002.12', 3: 'NC_000003.12', 4: 'NC_000004.12', 5: 'NC_000005.10',
            6: 'NC_000006.12', 7: 'NC_000007.14', 8: 'NC_000008.11', 9: 'NC_000009.12', 10: 'NC_000010.11',
            11: 'NC_000011.10', 12: 'NC_000012.12', 13: 'NC_000013.11', 14: 'NC_000014.9', 15: 'NC_000015.10',
            16: 'NC_000016.10', 17: 'NC_000017.11', 18: 'NC_000018.10', 19: 'NC_000019.10', 20: 'NC_000020.11',
            21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11', 24: 'NC_000024.10'
        }
    },
    'GRCh38': {
        'p13': {
            1: 'NC_000001.11', 2: 'NC_000002.12', 3: 'NC_000003.12', 4: 'NC_000004.12', 5: 'NC_000005.10',
            6: 'NC_000006.12', 7: 'NC_000007.14', 8: 'NC_000008.11', 9: 'NC_000009.12', 10: 'NC_000010.11',
            11: 'NC_000011.10', 12: 'NC_000012.12', 13: 'NC_000013.11', 14: 'NC_000014.9', 15: 'NC_000015.10',
            16: 'NC_000016.10', 17: 'NC_000017.11', 18: 'NC_000018.10', 19: 'NC_000019.10', 20: 'NC_000020.11',
            21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11', 24: 'NC_000024.10'
        }
    }
}


def convert_variant_to_hgvs(chromosome,
                            position,
                            ref_allele,
                            alt_allele,
                            reference_genome: str = 'b38',
                            reference_patch: str = 'p1'):
    try:
        # convert X or Y to integer values for proper indexing
        if chromosome == 'X':
            chromosome = 23
        elif chromosome == 'Y':
            chromosome = 24
        else:
            chromosome = int(chromosome)

        # get the HGVS reference chromosome label
        ref_chromosome = REFERENCE_CHROMOSOME_LOOKUP[reference_genome][reference_patch][chromosome]
    except KeyError:
        return ''

    # get the length of the reference allele
    len_ref = len(ref_allele)

    # is there an alt allele
    if alt_allele == '.':
        # deletions
        if len_ref == 1:
            variation = f'{position}del'
        else:
            variation = f'{position}_{position + len_ref - 1}del'

    elif alt_allele.startswith('<'):
        # we know about these but don't support them yet
        return ''

    else:
        # get the length of the alternate allele
        len_alt = len(alt_allele)

        # if this is a SNP
        if (len_ref == 1) and (len_alt == 1):
            # simple layout of ref/alt SNP
            variation = f'{position}{ref_allele}>{alt_allele}'
        # if the alternate allele is larger than the reference is an insert
        elif (len_alt > len_ref) and alt_allele.startswith(ref_allele):
            # get the length of the insertion
            diff = len_alt - len_ref

            # get the position offset
            offset = len_alt - diff

            # layout the insert
            variation = f'{position + offset - 1}_{position + offset}ins{alt_allele[offset:]}'
        # if the reference is larger than the deletion it is a deletion
        elif (len_ref > len_alt) and ref_allele.startswith(alt_allele):
            # get the length of the deletion
            diff = len_ref - len_alt

            # get the position offset
            offset = len_ref - diff

            # if the diff is only 1 BP
            if diff == 1:
                # layout the SNP deletion
                variation = f'{position + offset}del'
            # else this is more that a single BP deletion
            else:
                # layout the deletion
                variation = f'{position + offset}_{position + offset + diff - 1}del'
        # we do not support this allele
        else:
            return ''

    # layout the final HGVS expression in curie format
    hgvs: str = f'{ref_chromosome}:g.{variation}'

    # return the expression to the caller
    return hgvs
