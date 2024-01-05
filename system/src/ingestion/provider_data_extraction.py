# -*- coding: utf-8 -*-
"""
Provider data ingestion - step 1
read provider data, select required fields, convert it to vespa format based on the designed schema

v0.1 - prototype version
v0.3 - add major sections in procesing
v0.4 - add accepting patient code to unet (for both organization/practitioner)

@author: Yizhao Ni, PhD, MBA, FAMIA
@email: yizhao_ni@optum.com
"""

import warnings
import re, os, sys, json, time
import pandas as pd
import numpy as np
from datetime import datetime


warnings.simplefilter(action='ignore', category=FutureWarning)



def process_provider_data_section(dsection, settings):
    """
    process provider data section
    Note: only process the 1st record
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        doc_expire_date - provider profile expired date (long in unix time epoch)
        first_name - provider first name
        middle_name - provider middle name 
        last_name - provider last name (if provider)
        org_name - organization name (if organization)
        prov_type_code - provider type code
        organization_type_code - organization type code
    """
    
    dsection = dsection[0] #get the first record
    void_ind = dsection['voidedIndicator']
    
    if void_ind == 'N':
        #convert to unix timestamp epoch
        expire_date = int(time.mktime(datetime.strptime(dsection['cancelDate'], '%Y-%m-%d').timetuple()))
       
        org_name = ""
        last_name = dsection['lastName'].lower()
        
        if settings.data_type.startswith("o"):
            org_name = last_name #organization name is stored in last name
            last_name = "" 
            
        record = {'doc_expire_date': expire_date,
                  'first_name': dsection['firstName'].lower(),
                  'middle_name': dsection['middleName'].lower(),
                  'last_name': last_name,
                  'org_name': org_name,
                  'prov_type_code': dsection['providerTypeCode'].lower(),
                  'organization_type_code': dsection['organizationTypeCode'].lower()
                  }
        
        return record
    else:
        return None #remove void record
    
    
    
def process_provider_address_section(dsection, settings=None):
    """
    process provider address section
    Note: only process the 1st record
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        address_id - address id
        address_line - address line
        city_name - city name
        county_name - county name
        state_code - state code
        zipcode - zipcode 
        geocode - (dict) {'lat':row['lat'], 'lng':row['lng']}
    """
    
    dsection = dsection[0] #get the first record
    zip_code =eval(dsection['zipCode']) #zip code should be a 5-digit string (e.g., 06320 is a valid zip); the current data trim the prefix 0
    zip_code = str(100000 + zip_code)[1:] #add 0 to 4 digit zip code

    record = {'address_id': eval(dsection['addressId'].lower()),
              'address_line': dsection['addressLine1'].lower(),
              'city_name': dsection['cityName'].lower(),
              'county_name': dsection['countyName'].lower(),
              'state_code': dsection['stateCode'].lower(),
              'zipcode': zip_code, 
              'geocode': {'lat': eval(dsection['latitude']), 'lng':eval(dsection['longitude'])}
        }
    
    
    return record
    


def process_csp_contract_section(dsection, settings):
    """
    process csp contract section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        csp_lob_contract - (dict) ovationLOBTypeCode -> max expire date for this contract type
    """
    
    
    csp_contract = dict()
    
    
    for record in dsection:
        m_id = record['cspProviderId'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['ovationLOBTypeCode'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in csp_contract:
                    if expire_date > csp_contract[code]:
                        csp_contract[code] = expire_date
                else:
                    csp_contract[code] = expire_date
        
        
    return {'csp_contract': csp_contract}


def process_national_provider_section(dsection, settings):
    """
    process national provider section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        national_taxonomy - (dict) taxonomy -> max expire date for this taxonomy
    """
    
    
    national_taxonomy = dict()
    
    
    for record in dsection:
        m_id = record['nationalProviderId'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['taxonomyCode'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in national_taxonomy:
                    if expire_date > national_taxonomy[code]:
                        national_taxonomy[code] = expire_date
                else:
                    national_taxonomy[code] = expire_date
        
        
    return {'national_taxonomy': national_taxonomy}


def process_cosmos_contract_section(dsection, settings):
    """
    process cosmos contract section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        cosmos_contract - (dict) cosmosDiv-cosmosPanelNumber -> max expire date for this contract type
    """
    
    
    cosmos_contract = dict()
    
    
    for record in dsection:
        m_id = record['cosmosProviderNumber'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['cosmosDiv'].lower()+"-"+record['cosmosPanelNumber'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in cosmos_contract:
                    if expire_date > cosmos_contract[code]:
                        cosmos_contract[code] = expire_date
                else:
                    cosmos_contract[code] = expire_date
        
        
    return {'cosmos_contract': cosmos_contract}


def process_unet_contract_section(dsection, settings):
    """
    process unet contract section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        unet_contract - (dict) marketNumber-productOfferId-acceptingPatientCode -> max expire date for this contract type
    """
    
    
    unet_contract = dict()
    
    
    for record in dsection:
        m_id = record['contractId'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['marketNumber'].lower()+"-"+record['productOfferId'].lower()+"-"+record['acceptingPatientCode'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in unet_contract:
                    if expire_date > unet_contract[code]:
                        unet_contract[code] = expire_date
                else:
                    unet_contract[code] = expire_date
        
        
    return {'unet_contract': unet_contract}


def process_specialty_type_section(dsection, settings):
    """
    process specialty type section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        specialty_org - (dict) specialtyTypeCode-contractingOrgCode-primaryCode 
        -> max expire date for this specialty type
    """
    
    
    specialty_org = dict()
    
    
    for record in dsection:
        m_id = record['specialtyTypeCode'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['specialtyTypeCode'].lower()+"-"+record['contractingOrgCode'].lower()+"-"+record['primaryCode'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in specialty_org:
                    if expire_date > specialty_org[code]:
                        specialty_org[code] = expire_date
                else:
                    specialty_org[code] = expire_date
        
        
    return {'specialty_org': specialty_org}


def process_contract_org_section(dsection, settings):
    """
    process contracting organization section (effective records only):
    
    Input:
        dsection - section data (dict)
    Output (dict): 
        contract_org - (dict) contractingOrgCode-primaryCode-correspondenceIndicator
        -> max expire date for this contract type
    """
    
    
    contract_org = dict()
    
    
    for record in dsection:
        m_id = record['contractingOrgCode'].lower()
        void_ind = record['voidedIndicator']
        
        if m_id and void_ind == 'N':
            code = record['contractingOrgCode'].lower()+"-"+record['primaryCode'].lower()+"-"+record['correspondenceIndicator'].lower()
            expire_date = int(datetime.strftime(datetime.strptime(record['cancelDate'], '%Y-%m-%d'),'%Y%m%d'))
            
            if expire_date >= settings.current_date:
                #get the max expire date for a code for non-expired contracts
                if code in contract_org:
                    if expire_date > contract_org[code]:
                        contract_org[code] = expire_date
                else:
                    contract_org[code] = expire_date
        
        
    return {'contract_org': contract_org}



def clean_data_with_all_processes(filename, outfilename, settings):
    """
    function to clean each data file with all processes 
    Input:
        filename - input file name
        outfilename - output file name
    Output:
        save data frame to outfilename as a pkl file
    """
    
    print("Process file: {}\n".format(filename))
    
    with open(filename,'r', encoding='utf8') as f:
        df = pd.DataFrame()
        all_data = json.load(f) #each element is a record
        
        for rindex in range(1, len(all_data)+1):
            data = all_data[rindex-1]
            data_dict = None
            
            if data['enterpriseProviderId']:
                data_dict = dict()
                data_dict['enterprise_provider_id'] = data['enterpriseProviderId']
                data_dict['generated_key'] = data['generatedKey']
            
                #process each section
                for process_name in process_names:
                    process = processes.get(process_name)
                    record = process(data[process_name], settings)
                    
                    if record:
                        data_dict.update(record)
                    else:
                        if record == None: #None reserved for void provider record
                            print("Void record during process: {} (ignore this document {}).".format(process_name, data_dict['generated_key'] ))
                            data_dict = None
                            break #ignore void record in the update
                        else:
                            print("Empty record during process: {}.".format(process_name))

                #add record to data frame
                if data_dict:
                    df = df.append(data_dict, ignore_index=True)
            
            if rindex % settings.num_dispay == 0:
                print("Have processed {} records.".format(rindex))
          
    print("Processed a total of {} records.".format(rindex))        
    print("Data frame shape: {}\n".format(df.shape))
    print("Save data frame.\n")
    
    #save df to pickle file
    df.to_pickle(outfilename)
    print("-----------------")


#process mapping dictionary 
processes = {
             "providerData": process_provider_data_section,
             "providerTinAddressData": process_provider_address_section,
             "cspContractData": process_csp_contract_section,
             "nationalProviderIdData": process_national_provider_section,
             "cosmosContractData": process_cosmos_contract_section,
             "unetContractData": process_unet_contract_section,
             "specialtyContractingOrgData": process_specialty_type_section,
             "addressContractingOrgData": process_contract_org_section,
             }


#processes required for each record
process_names = ['providerData', 'providerTinAddressData', 'cspContractData',
                 'nationalProviderIdData', "cosmosContractData", "unetContractData",
                 "specialtyContractingOrgData", "addressContractingOrgData"]



################main process #########################
def main_process(settings):
    #path configuration
    indir = settings.local_indir
    outdir = settings.local_outdir
  
    filenames = settings.all_infiles
    outfilenames = settings.all_outfiles
    num_file = len(filenames)
    file_indices = [x for x in range(num_file)]
    
    
    #data extraction/cleaning from the raw data file
    start_time = time.time()

    for findex in file_indices:
        filename = indir + filenames[findex]
        outfilename = outdir + outfilenames[findex]
        settings.data_type = settings.data_types[findex] #add a temp data type indicator
        
        #single thread processing
        clean_data_with_all_processes(filename, outfilename, settings)
        
    end_time = time.time()
    print("Execution time: {} secs.".format(end_time-start_time))
        
