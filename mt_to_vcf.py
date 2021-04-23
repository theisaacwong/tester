#!/usr/bin/env python

import argparse
import logging
import os
import hail as hl
from gnomad.utils.vcf import ht_to_vcf_mt
from gnomad.utils.sparse_mt import default_compute_info

logging.basicConfig(
    format="%(asctime)s (%(name)s %(lineno)s): %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
)
logger = logging.getLogger("create_sites_vcf")
logger.setLevel(logging.INFO)


class CreateVcf:
    hl.init(log="/create_sites_vcf.log", default_reference="GRCh38")
    def __init__(self, matrix_table, output_dir, call_set_name, partitions, overwrite, data_type, parallel=None):
        self.matrix_table = matrix_table
        self.output_dir = output_dir
        self.call_set_name = call_set_name
        self.partitions = partitions
        self.overwrite = overwrite
        self.parallel = parallel
        self.data_type = data_type

    def run(self):
        mt = self.read_matrix_table()
        mt = self.filter_rows_and_add_tags(mt)
        info_ht = self.create_info_ht(mt)
        vcf_path = self.create_vcf(info_ht)
        self.copy_logs()

    def read_matrix_table(self):
        logger.info(f"Reading in input {self.matrix_table} MT (raw sparse MT)...")
        return hl.read_matrix_table(self.matrix_table)

    def filter_rows_and_add_tags(self, mt):
        if self.data_type == 'WGS':
            # Densify matrix table
            mt = hl.experimental.densify(mt)
            # Filter to only non-reference sites
            mt = mt.filter_rows((hl.len(mt.alleles) > 1) & (hl.agg.any(mt.LGT.is_non_ref())))

            # annotate site level DP onto the mt rows (but make sure to name it something else to avoid a name collision with the DP entry)
            mt = mt.annotate_rows(site_dp=hl.agg.sum(mt.DP))

            # Add AN tag as ANS
            return mt.annotate_rows(ANS=hl.agg.count_where(hl.is_defined(mt.LGT)) * 2)

        if self.data_type == 'Exome':
            return mt.filter_rows(hl.len(mt.alleles) > 1)

    def create_info_ht(self, mt):
        info_ht = default_compute_info(
            mt, site_annotations=True, n_partitions=self.partitions
        )
        if self.data_type == 'WGS':
            # annotate the info struct in the info HT with the site level DP (you can also name it DP this time)
            info_ht = info_ht.annotate(info=info_ht.info.annotate(DP=mt.rows()[info_ht.key].site_dp))

        # table version of what goes into the VCF -- that becomes the input into Chris F's pipeline that runs VQSR
        return info_ht.checkpoint(
            os.path.join(self.output_dir, f'{self.call_set_name}.info.ht'),
            overwrite=self.overwrite,
            _read_if_exists=not self.overwrite
        )

    def create_vcf(self, info_ht):
        vcf_path = os.path.join(self.output_dir, f'{self.call_set_name}.sites.vcf.bgz')
        vcf_mt = ht_to_vcf_mt(info_ht)
        hl.export_vcf(
            vcf_mt,
            vcf_path,
            parallel=self.parallel,
            tabix=True
        )
        return vcf_path

    def copy_logs(self):
        logger.info("Copying hail log to logging bucket...")
        hl.copy_log(os.path.join(self.output_dir, f'{self.call_set_name}.logs'))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_matrix_table", help="input matrix table", required=True)
    parser.add_argument("-o", "--output_directory", help="output dir", required=True)
    parser.add_argument("-c", "--call_set_name", help="call_set_name", required=True)
    parser.add_argument("-np", "--partitions", help="partitions", type=int, required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--parallel", default=None, help="parallel to use when exporting vcf. Default is None")
    parser.add_argument("-d", "--data_type", choices=["Exome", "WGS"])
    return parser.parse_args()

def main():
    args = get_args()
    CreateVcf(
        matrix_table=args.input_matrix_table,
        output_dir=args.output_directory,
        call_set_name=args.call_set_name,
        partitions=args.partitions,
        overwrite=args.overwrite,
        data_type=args.data_type,
        parallel=args.parallel
    ).run()

if __name__ == "__main__":
    main()
