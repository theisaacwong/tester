import hail as hl
import argparse

hl.init(log='/home/hail/combiner.log')

def get_args():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument("--sample_map", "-s", required=True)
    argparser.add_argument("--output_cloud_path", "-c", required=True)
    argparser.add_argument("--tmp_bucket", "-t", required=True)
    argparser.add_argument("--gvcf_header_file", "-g", required=True)
    argparser.add_argument("--overwrite_existing", "-o", action='store_true')
    return argparser.parse_args()

def get_gvcf_and_sample_from_map(sample_map):
    gvcfs = []
    samples = []
    with hl.hadoop_open(sample_map, 'r') as f:
       for line in f:
           (sample, gvcf) = line.rstrip().split('\t')
           gvcfs.append(gvcf)
           samples.append(sample)
    return gvcfs, samples

if __name__ == "__main__":
    args = get_args()
    gvcf_list, samples_list = get_gvcf_and_sample_from_map(args.sample_map)
    hl.experimental.run_combiner(
        gvcf_list,
        sample_names=samples_list,
        header=args.gvcf_header_file,
        out_file=args.output_cloud_path,
        tmp_path=args.tmp_bucket,
        key_by_locus_and_alleles=True,
        overwrite=args.overwrite_existing,
        reference_genome='GRCh38',
        use_exome_default_intervals=True,
        target_records=10000
    )
