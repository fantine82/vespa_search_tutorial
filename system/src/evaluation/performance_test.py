# -*- coding: utf-8 -*-
"""
Search engine evaluation - performance test
performance test within the local vespa container and save performance report in local dir

v0.1 - version for local prototype; 
       require vespa-fbench in the local docker image.
       
@author: Yizhao Ni, PhD, MBA, FAMIA
@email: yizhao_ni@optum.com
"""

import sys, os, time, re

def main_process(settings):
    schema_name = settings.schema_name
    data_type = schema_name
    container_name = settings.app_name

    queryfile = settings.local_query_dir + "sample_query_"+data_type + ".txt"
    reportfile = settings.local_query_dir + "vespa_performance_report_"+data_type + ".txt"

    performance_test_bash_template = settings.performance_test_bash_template
    performance_test_time = str(settings.performance_test_time)
    n_clients = str(settings.n_clients)

    with open(performance_test_bash_template, 'r', encoding='utf8') as file:
        bash_content = file.read()

    #replace query directory
    bash_content = re.sub("containerxxxxxx", container_name, bash_content)
    bash_content = re.sub("query_filexxxxxx", queryfile, bash_content)
    bash_content = re.sub("report_filexxxxxx", reportfile, bash_content)
    bash_content = re.sub("timexxxxxx", performance_test_time, bash_content)
    bash_content = re.sub("clientxxxxxx", n_clients, bash_content)

    #run the bash content
    os.system(bash_content)
    
    #print the report
    with open(reportfile, 'r', encoding='utf8') as file:
        report_content = file.read()
        
    print(report_content)