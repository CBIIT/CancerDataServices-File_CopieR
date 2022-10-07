#!/usr/bin/env Rscript

#Cancer Data Services - File_CopieR.R

#This script will take a validated CDS submission manifest and copy them over to a given bucket while maintaining the original bucket directory structure.

##################
#
# USAGE
#
##################

#Run the following command in a terminal where R is installed for help.

#Rscript --vanilla CDS-File_CopieR.R --help


##################
#
# Env. Setup
#
##################

#List of needed packages
list_of_packages=c("dplyr","tidyr","readr","stringi","optparse","janitor","tools")

#Based on the packages that are present, install ones that are required.
new.packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]
suppressMessages(if(length(new.packages)) install.packages(new.packages))

#Load libraries.
suppressMessages(library(dplyr,verbose = F))
suppressMessages(library(tidyr,verbose = F))
suppressMessages(library(readr,verbose = F))
suppressMessages(library(stringi,verbose = F))
suppressMessages(library(readxl,verbose = F))
suppressMessages(library(optparse,verbose = F))
suppressMessages(library(janitor,verbose = F))
suppressMessages(library(tools,verbose = F))

#remove objects that are no longer used.
rm(list_of_packages)
rm(new.packages)


##################
#
# Arg parse
#
##################

#Option list for arg parse
option_list = list(
  make_option(c("-f", "--file"), type="character", default=NULL, 
              help="A validated CDS submission template v1.3.1 file, with their current bucket url, e.g. s3://bucket.location.was.here/time/to/move.txt", metavar="character"),
  make_option(c("-b", "--bucket"), type="character", default=NULL, 
              help="The new AWS bucket location for the files, e.g. s3://bucket.location.is.here/", metavar="character")
)

#create list of options and values for file input
opt_parser = OptionParser(option_list=option_list, description = "\nCDS-File_CopieR\n v.1.3.1\n\nPlease supply the following script with a validated CDS submission template v1.3.1 file and the new AWS bucket location.")
opt = parse_args(opt_parser)

#If no template is presented, return --help, stop and print the following message.
if (is.null(opt$file)){
  print_help(opt_parser)
  cat("Please supply the validated CDS submission template v1.3.1 file (-f).\n\n")
  suppressMessages(stop(call.=FALSE))
}

#If no options are presented, return --help, stop and print the following message.
if (is.null(opt$bucket)){
    print_help(opt_parser)
    cat("Please supply the bucket path for the new AWS location for the files (-b).\n\n")
    suppressMessages(stop(call.=FALSE))
}

#Data file pathway
base_bucket=opt$bucket

#Template file pathway
file_path=file_path_as_absolute(opt$file)

#A start message for the user that the file copying is underway.
cat("\nThe data files are being moved to the new bucket location.\n\n")


###########
#
# File name rework
#
###########

#Rework the file path to obtain a file name.
file_name=stri_reverse(stri_split_fixed(str = (stri_split_fixed(str = stri_reverse(file_path), pattern="/",n = 2)[[1]][1]),pattern = ".", n=2)[[1]][2])

ext=tolower(stri_reverse(stri_split_fixed(str = stri_reverse(file_path),pattern = ".",n=2)[[1]][1]))

path=paste(stri_reverse(stri_split_fixed(str = stri_reverse(file_path), pattern="/",n = 2)[[1]][2]),"/",sep = "")
                 
                 
#Output file name based on input file name and date/time stamped.
output_file=paste(file_name,
                  "_updated_bucket_",
                  stri_replace_all_fixed(
                    str = Sys.Date(),
                    pattern = "-",
                    replacement = "_"),
                  sep="")
                                   

#Read in metadata page/file to check against the expected/required properties. 
#Logic has been setup to accept the original XLSX as well as a TSV or CSV format.
if (ext == "tsv"){
  df=suppressMessages(read_tsv(file = file_path, guess_max = 1000000, col_types = cols(.default = col_character())))
}else if (ext == "csv"){
  df=suppressMessages(read_csv(file = file_path, guess_max = 1000000, col_types = cols(.default = col_character())))
}else if (ext == "xlsx"){
  df=suppressMessages(read_xlsx(path = file_path,sheet = "Metadata", guess_max = 1000000, col_types = "text"))
}else{
  stop("\n\nERROR: Please submit a data file that is in either xlsx, tsv or csv format.\n\n")
}

#Remove any situations where deletions of rows are considered NA values in excel
df=remove_empty(df,which = "rows")

#Simplify the data frame to only include the needed column with a simplier name.
df_cds=df%>%
  select(-url)
df=select(df,file_url_in_cds)

colnames(df)<-c("s3")


#############
#
# Generate Stats
#
############

#Add a "/" if it is not present in the new bucket url
if (substr(x = base_bucket,start = nchar(base_bucket), stop = nchar(base_bucket))!="/"){
  base_bucket=paste(base_bucket,"/",sep = "")
}

#New bucket counter before upload
new_bucket_count=suppressWarnings(length(system(command = paste("aws s3 ls --recursive ",base_bucket,sep = ""),intern = TRUE,wait = TRUE)))
                 

#############
#
# Copy over files
#
#############

#Manipulate data frame to add new s3 bucket onto the directory location of the old s3 bucket.
df=df%>%
  mutate(new_s3=s3)%>%
  separate(new_s3,into = c("s3_pre","blank","bucket","directory"),extra = "merge",sep = "/")%>%
  mutate(new_s3=paste(base_bucket,directory,sep = ""))%>%
  select(-directory,-s3_pre,-blank,-bucket)

#Make sure if there are multiples of the same file, we only transfer once.
df=unique(df)

#Count the number of lines in the data frame
cds_count=dim(df)[1]

upload_count=0

#Progress bar setup
pb=txtProgressBar(min=0,max=dim(df)[1],style = 3)

#For loop that will copy the file location from the old bucket to the new bucket.
for (position in 1:dim(df)[1]){
  system(command = paste("aws s3 cp ",df$s3[position]," ",df$new_s3[position],sep = ""),intern = TRUE,wait = TRUE)
  upload_count=upload_count+1
  setTxtProgressBar(pb,position)
}

#New bucket count after upload
newer_bucket_count=length(system(command = paste("aws s3 ls --recursive ",base_bucket,sep = ""),intern = TRUE,wait = TRUE))

#Combine the new and old buckets with the CDS submission template
colnames(df)<-c("file_url_in_cds","url")
df_cds_new=suppressMessages(left_join(df_cds,df))

df_cds_new=df_cds_new%>%
  mutate(file_url_in_cds=url)%>%
  select(GUID,file_size,md5sum,url,acl,everything())

#Write out of new CDS submission template
write_tsv(x = df_cds_new,file = paste(path,output_file,".tsv",sep = ""), na="")

#A stop message for the user that the file copying is done.
cat("\n\nThe data files have been moved to the new bucket location.\n")

cat(paste("\nThe updated CDS Submission Template, ",file_name,"_updated_buckets.tsv, has been created with the new bucket locations.\n",sep = ""))

cat(paste("\nOverview of the transfer:\n\tNumber of Files in the new bucket: ",new_bucket_count,"\n\tNumber of Files in the CDS template: ",cds_count,"\n\tNumber of Files transferred: ", upload_count,"\n\tNumber of Files in the new bucket after transfer: ",newer_bucket_count,"\n\n",sep = ""))
