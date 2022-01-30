#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json
from urllib.request import urlopen
from minio import Minio
import os


# In[2]:


def upload(filename):
    client = Minio(
        os.environ['MINIO_URL'],
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY']
    )
    bucket=os.environ['MINIO_BUCKET']
    client.fput_object(bucket, filename, filename)
    print(f" - {filename} uploaded")


# In[3]:


# Création du dataset france_covid
def france_covid(df_france):
    df_france = df_france.set_index('date')
    df_france = df_france.apply(pd.to_numeric)
    df_france['variation'] = df_france['value'].pct_change()
    df_france.to_csv('france_covid.csv')
    print(" - france_covid.csv créé")
    upload("france_covid.csv")


# In[4]:


# Création du dataset france_covid
def france_covid_mm7(df_france_mm7):
    df_france_mm7['value'] = df_france_mm7['value'].rolling(window=7).mean()
    df_france_mm7['variation'] = df_france_mm7['value'].pct_change(periods = 7)
    df_france_mm7.to_csv('france_covid_mm7.csv')
    print(" - france_covid_mm7.csv créé")
    upload("france_covid_mm7.csv")


# In[5]:


def get_df_mutation(url, mutation):
    print(f"  - Travail sur la mutation {mutation}")
    response = urlopen(url)
    json_prop_variant = json.loads(response.read())

    df_prop_variant = pd.json_normalize(
        json_prop_variant['regions'], 
        record_path =['values'], 
        meta=['code_level']
    )
    df_prop_variant.rename(columns={'value': mutation}, inplace=True)
    return df_prop_variant


# In[6]:


def get_df_france():
    url_cas_positifs = "https://data.widgets.dashboard.covid19.data.gouv.fr/cas_positifs.json"
    response = urlopen(url_cas_positifs)
    json_cas_positifs = json.loads(response.read())
    df_france = pd.json_normalize(json_cas_positifs['france'][0]['values'])
    return df_france


# In[7]:


def get_df_regions():
    url_cas_positifs = "https://data.widgets.dashboard.covid19.data.gouv.fr/cas_positifs.json"
    response = urlopen(url_cas_positifs)
    json_cas_regions = json.loads(response.read())
    df_cas_regions = pd.json_normalize(
        json_cas_regions['regions'], 
        record_path =['values'], 
        meta=['code_level']
    )
    df_cas_regions.rename(columns={'value': 'nb_cas'}, inplace=True)
    return df_cas_regions


# In[8]:


def variant_regions():
    df_cas_regions = get_df_regions()
    df_prop_variant_A = get_df_mutation("https://data.widgets.dashboard.covid19.data.gouv.fr/prop_variant_A.json","prop_a")
    df_prop_variant_B = get_df_mutation("https://data.widgets.dashboard.covid19.data.gouv.fr/prop_variant_B.json","prop_b")
    df_prop_variant_C = get_df_mutation("https://data.widgets.dashboard.covid19.data.gouv.fr/prop_variant_C.json","prop_c")
    df_prop_variant_D = get_df_mutation("https://data.widgets.dashboard.covid19.data.gouv.fr/prop_variant_D.json","prop_d")
    
    df_prop_variant_AB = pd.merge(df_prop_variant_A, df_prop_variant_B,  how='outer', left_on=['date','code_level'], right_on = ['date','code_level'])
    df_prop_variant_ABC = pd.merge(df_prop_variant_AB, df_prop_variant_C,  how='outer', left_on=['date','code_level'], right_on = ['date','code_level'])
    df_prop_variant_region = pd.merge(df_prop_variant_ABC, df_prop_variant_D,  how='outer', left_on=['date','code_level'], right_on = ['date','code_level'])
    df_prop_variant_region = pd.merge(df_prop_variant_region, df_cas_regions,  how='left', left_on=['date','code_level'], right_on = ['date','code_level'])
    df_prop_variant_region = df_prop_variant_region[['date', 'code_level','nb_cas', 'prop_a', 'prop_b', 'prop_c', 'prop_d']]
    
    # To numeric
    df_prop_variant_region[['prop_a', 'prop_b', 'prop_c', 'prop_d']] = df_prop_variant_region[['prop_a', 'prop_b', 'prop_c', 'prop_d']].apply(pd.to_numeric)
    # Replace NA 
    df_prop_variant_region[['prop_a', 'prop_b', 'prop_c', 'prop_d']] = df_prop_variant_region[['prop_a', 'prop_b', 'prop_c', 'prop_d']].fillna(0)
    df_prop_variant_region.to_csv('prop_variant_region.csv')
    print(" - prop_variant_region.csv créé")
    upload("prop_variant_region.csv")


# In[9]:


def main():
    print("Création des dataset France")
    df_france = get_df_france()
    france_covid(df_france)
    france_covid_mm7(df_france)
    
    print("Création du dataset variant par régions")
    variant_regions()
    


# In[10]:


if __name__ == '__main__':
    main()

