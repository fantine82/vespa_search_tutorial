#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
constant variables for the scripts

@author: Yizhao Ni, PhD, MBA, FAMIA
@email: yizhao_ni@optum.com
"""

#vespa configuration
app_name = "tutorialvespa"
config_dir = "../../resources/application/"


#data extraction configuration
num_file = 1
current_date = 20230601 #to filter out expired contracts

#local data repo settings
local_indir = "../../data/"
local_outdir = local_indir
all_infiles = ['organization_sample_data.json', 'practitioner_sample_data.json',]
data_types = ["organization", "practitioner"]

all_outfiles = [x[:x.rfind(".")] + ".pkl" for x in all_infiles]

org_infiles = all_infiles[0:1]
org_outfiles = [x[:x.rfind(".")] + ".pkl" for x in org_infiles]

prov_infiles = all_infiles[1:]
prov_outfiles = [x[:x.rfind(".")] + ".pkl" for x in prov_infiles]

#vespa data/config files
schema_name = 'organization' #practitioner
data_feed_flag = 1 #1. batch feed, 2. point-wise feed, 3. data frame feed, 0. no feed
data_feed_debug = False #for point-by-point feed debug
key_id_field = "generated_key" #the unique ID for each doc
num_dispay = 1000 #record number trigger for information display
batch_size = 1000 #batch feed size (small number to avoid crash)
num_connections = 5 #number of connections to host (default=100; max connection depends on JVM GC spped and heap size)
timeout = 100 #time out (in sec) for a batch feed
vespa_url_local = "http://localhost:8080"

#performance testing
local_query_dir = "../../query/"
performance_test_bash_template = "../../resources/template/benchmark_template.sh" #schema patch file

n_queries = 100
query_per_set = int(n_queries/num_file) #select n queries per data file
n_clients = 5 #clients querying at the same time
n_returned_results = 10 #number of returned results per query
performance_test_time = 30 #in seconds
filter_fileds = ['csp_contract', 'national_taxonomy', 'cosmos_contract', 'unet_contract', 'specialty_org', 'contract_org']
geo_search_perc = 0.5 #percentage of queries including geo search
filter_search_perc = 0.2 #percentage of queries including structured filters
contract_date_filter = current_date #expire date filter
geo_random_scale = 0.75