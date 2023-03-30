# CancerDataServices-File_CopieR
This script will take a validated CDS submission template and transfer the files from one AWS bucket to a new AWS bucket and output a new template with the updated file locations.

To run the script on a complete [CDS validated submission template](https://github.com/CBIIT/cds-model/tree/main/metadata-manifest), run the following command in a terminal where R is installed for help.

```
Rscript --vanilla CDS-File_CopieR.R --help
```

```
Usage: CDS-File_CopieR.R [options]

CDS-File_CopieR v2.0.1

Please supply the following script with a validated CDS submission template v1.3.1 file and the new AWS bucket location.

Options:
	-f CHARACTER, --file=CHARACTER
		A validated CDS submission template v1.3.1 file, with their current bucket url, e.g. s3://bucket.location.was.here/time/to/move.txt

	-b CHARACTER, --bucket=CHARACTER
		The new AWS bucket location for the files, e.g. s3://bucket.location.is.here/

	-h, --help
		Show this help message and exit
```

An example usage would be the following:

```
Rscript --vanilla CDS-File_CopieR.R -f Validated_CDS_template.tsv -b s3://new.bucket.location/

The data files are being moved to the new bucket location.

  |======================================================================| 100%

The data files have been moved to the new bucket location.

The updated CDS Submission Template, Validated_CDS_template_updated_buckets.tsv, has been created with the new bucket locations.

Overview of the transfer:
	Number of Files in the new bucket: 0
	Number of Files in the CDS template: 8
	Number of Files transferred: 8
	Number of Files in the new bucket after transfer: 8
```
