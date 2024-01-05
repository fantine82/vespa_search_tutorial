# -*- coding: utf-8 -*-
"""
Search engine evaluation - performance test
generate a set of pesudo queries for search engine performance test

v0.1 - version for local prototype; 
       require vespa-fbench in the local docker image.
       
v0.2 - add contraints including geo search and structured filters

@author: Yizhao Ni, PhD, MBA, FAMIA
@email: yizhao_ni@optum.com
"""



import sys, os, time, re, urllib
import pandas as pd
import numpy as np

def search_term_selection(data_type):
    """
    select data fields to create search terms. 
    Note: use city_name for the address field. 
    
    Inputs:
        data_type - organization/practitioner
        
    Outputs:
        data_fields - data fields to be used

    """
    
    if data_type.startswith("o"):
        #0. org_name, 1. address, 2. use both.
        rand_selection = int(np.random.rand()*3)
        if rand_selection == 0:
            return ['org_name']
        elif rand_selection == 1:
            return ['city_name']
        else:
            return ['org_name', 'city_name']
    else:
        #0. first name, 1. last name, 2. address, 3. first/last name, 4. first/last name, address.
        rand_selection = int(np.random.rand()*5)
        if rand_selection == 0:
            return ['first_name']
        elif rand_selection == 1:
            return ['last_name']
        elif rand_selection == 2:
            return ['city_name']
        elif rand_selection == 3:
            return ['first_name', 'last_name']
        else:
            return ['first_name', 'last_name', 'city_name']




        
def main_process(settings):
    #configuration
    filedir = settings.local_outdir
    schema_name = settings.schema_name
    data_type = schema_name
    
    if schema_name.startswith("o"):
        filenames = settings.org_outfiles
        file_indices = [x for x in range(len(filenames))]
    else:
        filenames = settings.prov_outfiles
        file_indices = [x for x in range(len(filenames))]

    
    outfile = settings.local_query_dir + "sample_query_" + schema_name + ".txt"

    #query configuration
    geo_search_perc = settings.geo_search_perc
    filter_search_perc = settings.filter_search_perc
    filter_fileds = settings.filter_fileds
    n_filter_fields = len(filter_fileds)
    contract_date_filter = settings.contract_date_filter

    query_per_set = settings.query_per_set
    vespa_query_template = {
        'yql': 'select generated_key from sources {} where(userQuery()xxxxxxyyyyyy);'.format(schema_name),
        'query': 'temp',
        'hits': settings.n_returned_results,
        'ranking.profile': 'temp',
        'timeout': '15s',
        'ranking.softtimeout.enable': 'false' 
      }


    with open(outfile, 'w', encoding='utf8') as f:
        #for each data file
        for findex in file_indices:
            filename = filedir + filenames[findex]
            print("Select data points from file: {}".format(filename))

            data = pd.read_pickle(filename)
            selected_indices = np.random.permutation(len(data))[:query_per_set]
            selected_data = data.iloc[selected_indices]

            #for each selected data sample
            for n in range(query_per_set):
                query_body = vespa_query_template.copy()

                #create search term
                search_term = ""
                selected_fields = search_term_selection(data_type)
                for selected_field in selected_fields:
                    search_term += selected_data.iloc[n][selected_field] + " "

                query_body['query'] = search_term.strip()

                #add geo filter if selected
                if np.random.rand()<geo_search_perc:
                    lat = selected_data.iloc[n]['geocode']['lat'] + (np.random.rand()-0.5)*settings.geo_random_scale
                    lng = selected_data.iloc[n]['geocode']['lng'] + (np.random.rand()-0.5)*settings.geo_random_scale
                    geo_filter = ' and geoLocation(geocode, {}, {}, "25 miles")'.format(lat, lng)
                    query_body['yql'] = re.sub("xxxxxx", geo_filter, query_body['yql'])

                    #set ranking profile without geo search
                    if data_type.startswith("o"):
                        query_body['ranking.profile'] = 'org_geo_filter'
                    else:
                        query_body['ranking.profile'] = 'prov_geo_filter'

                else:
                    #set ranking profile without geo search
                    if data_type.startswith("o"):
                        query_body['ranking.profile'] = 'org_bm25'
                    else:
                        query_body['ranking.profile'] = 'prov_bm25'

                    query_body['yql'] = re.sub("xxxxxx", "", query_body['yql'])


                #add structured filters if selected
                if np.random.rand()<filter_search_perc:
                    selected_order = np.random.permutation(n_filter_fields)
                    structured_filter = None

                    for nn in selected_order:
                        temp_field = filter_fileds[nn]

                        if len(selected_data.iloc[n][temp_field]) > 0:
                            temp_key = list(selected_data.iloc[n][temp_field].keys())[0]
                            if temp_key:
                                structured_filter = ' and {} contains sameElement(key contains "{}", value>{})'.format(temp_field, temp_key, contract_date_filter)
                                break

                    if structured_filter:
                        query_body['yql'] = re.sub("yyyyyy", structured_filter, query_body['yql'])
                    else:
                        query_body['yql'] = re.sub("yyyyyy", "", query_body['yql'])
                else:
                    query_body['yql'] = re.sub("yyyyyy", "", query_body['yql'])


                temp_query = "/search/?"+urllib.parse.urlencode(query_body)

                f.write(temp_query+"\n")
        
        
