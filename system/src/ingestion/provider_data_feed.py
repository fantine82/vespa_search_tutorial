# -*- coding: utf-8 -*-
"""
Provider data ingestion - step 2

Feed data into vespa schema via pyvespa

v0.1 - prototype version

@author: Yizhao Ni, PhD, MBA, FAMIA
@email: yizhao_ni@optum.com
"""



from vespa.package import (
    Document,
    Field,
    Function,
    Schema,
    FieldSet,
    RankProfile,
    HNSW,
    ApplicationPackage,
    QueryProfile,
    QueryProfileType,
    QueryTypeField,)

import sys, os, time
from vespa.application import Vespa
import pandas as pd



def convert_df_to_vespa_dictlist(data, id_field):
    """
    convert a data frame to vespa dictionary list
    
    Inputs:
        data - data frame with all required fields
        id_filed - the id field
        
    Outputs:
        data_list - the dictionary list in vespa format
    """
    
    data_list = []
    
    for idx, row in data.iterrows():
        temp_data = dict()
        temp_data['id'] = row[id_field]
        temp_data['fields'] = dict(row)
        data_list.append(temp_data)
        
    return data_list


def feed_data_by_batch(data, id_field, vespa_app, schema_name, settings):
    """
    batch feed data using dictionary list (large GC in JVM)
    
    Input:
        data - data frame with all required fields
        id_field - the id_field
        vespa_app - vespa app connection
        schema_name - schema name in vespa
    """
    
    data_list = convert_df_to_vespa_dictlist(data, id_field)
    
    response = vespa_app.feed_batch(data_list, schema = schema_name, connections=settings.num_connections, 
                                    batch_size = settings.batch_size, total_timeout = settings.timeout)
    return response


def feed_data_by_df(data, id_field, vespa_app, schema_name, settings):
    """
    feed entire data frame
    
    Input:
        data - data frame with all required fields
        id_field - the id_field
        vespa_app - vespa app connection
        schema_name - schema name in vespa
    """
    response = vespa_app.feed_df(data, include_id=True, id_field=id_field, schema = schema_name, connections=settings.num_connections, batch_size = settings.batch_size)
    return response

    
def feed_data_by_point(data, id_field, vespa_app, schema_name, settings):
    """
    feed point-by-point
    
    Input:
        data - data frame with all required fields
        id_field - the id_field
        vespa_app - vespa app connection
        schema_name - schema name in vespa
        debug - display feed information
    """
    
    debug = settings.data_feed_debug
    
    count_example = 0
    for idx, row in data.iterrows():
        temp_data = dict(row)
        response = vespa_app.feed_data_point(schema = schema_name,
                                             data_id = temp_data[id_field], 
                                             fields = temp_data)
    
        if len(response.json) < 1:
            print("Error: {}. Skip this example.".format(response.json))
#            input("Type any key to continue...")
        elif debug:
            print("Feed doc: {}".format(response.json))
        else:  
            count_example +=1
        
            if count_example%settings.num_dispay == 0:
                print("Have processed {} records.".format(count_example))
            
    print("In total processed {} records.".format(count_example))


def main_process(settings):
    #configuration
    indir = settings.local_outdir
    schema_name = settings.schema_name

    if schema_name.startswith('o'):
        filenames = settings.org_outfiles
    else:
        filenames = settings.prov_outfiles
        
    num_file = len(filenames)
    file_indices = [x for x in range(num_file)]
    
    id_field = settings.key_id_field
    data_feed_flag = settings.data_feed_flag

    if data_feed_flag > 0:
        #data feed from the processed data file
        start_time = time.time()
        vespa_app = Vespa(url = settings.vespa_url_local) #connect to local host
    
        for findex in file_indices:
            file_st = time.time()
            filename = indir + filenames[findex]
            print("Feed data from file: {}".format(filename))
            
            data = pd.read_pickle(filename)
            data = data.fillna(999999) #can not process NaN 
            
            
            if data_feed_flag == 1:
                #batch feed
                responses = feed_data_by_batch(data, id_field, vespa_app, schema_name, settings)
            elif data_feed_flag == 2:
                #feed by data point
                feed_data_by_point(data, id_field, vespa_app, schema_name, settings)
            elif data_feed_flag == 3:
                #batch feed
                responses = feed_data_by_df(data, id_field, vespa_app, schema_name, settings)
            else:
                sys.exit("Unknown data feed flag: {}. Stop.".format(data_feed_flag))
            
            
            file_et = time.time()
            print("File process time: {} secs.".format(file_et-file_st))
            
        end_time = time.time()
        print("Execution time: {} secs.".format(end_time-start_time))
    
    else:
        print("No data feed initiated with process flag: {}".format(data_feed_flag))


