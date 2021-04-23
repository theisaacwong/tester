import argparse
import csv
import os, sys
import logging
import hail as hl

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

class ExtractFingerprintSite:
    hl.init(default_reference='GRCh38')
    MT_FIELDS_TO_KEEP = ['LGT', 'LPL', 'RGQ', 'END', 'LA']

    def __init__(self, input_matrix_table, sites_path, output_path):
        self.input_matrix_table = input_matrix_table
        self.sites_path = sites_path
        self.output_path = output_path

    def drop_unneeded_fields(self):
        mt = hl.read_matrix_table(self.input_matrix_table)
        return mt.select_entries(*self.MT_FIELDS_TO_KEEP)

    def densify_mt(self, mt):
        logging.info("densifying matrix table")
        return hl.experimental.densify(mt)

    def get_loci_from_sites_path(self):
        with hl.hadoop_open(self.sites_path) as sites:
            rdr = csv.reader(sites, delimiter='\t')
            loci = {
                hl.Locus(contig=chrom, position=int(pos), reference_genome='GRCh38')
                for chrom, pos, *_ in rdr
                if chrom and not chrom[0] in ('@', '#')
            }
            return loci

    def filter_matrix_table(self, mt):
        # memoize the loci we care about
        mt = mt.annotate_globals(fp_loci=self.get_loci_from_sites_path())
        # filter and grab what we need
        mt = mt.filter_rows(mt.fp_loci.contains(mt.locus))
        return mt

    def sparse_split_mt(self, mt):
        sparse_split_mt = hl.experimental.sparse_split_multi(mt)
        if 'RGQ' in sparse_split_mt.row:
            sparse_split_mt.drop('RGQ')
        return sparse_split_mt

    def export_to_fingerprint_vcf(self, mt):
        logging.info(f"Exporting fingerprint vcf to {self.output_path}")
        hl.export_vcf(mt, self.output_path)

    def run(self):
        mt = self.drop_unneeded_fields()
        densified_mt = self.densify_mt(mt)
        filtered_mt = self.filter_matrix_table(densified_mt)
        sparse_split_mt = self.sparse_split_mt(filtered_mt)
        self.export_to_fingerprint_vcf(sparse_split_mt)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--matrix_table', required=True, help='cloud path to matrix table', )
    parser.add_argument('-o', '--output_path', required=True, help='fingerprint sites output path. Should end in .vcf.bgz')
    parser.add_argument('-s', '--sites_path', default="gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.haplotype_database.txt")
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()

    if not args.output_path.endswith('.vcf.bgz'):
        sys.exit('output path has to end with .vcf.bgz')

    ExtractFingerprintSite(
        input_matrix_table=args.matrix_table,
        sites_path=args.sites_path,
        output_path=args.output_path
    ).run()
