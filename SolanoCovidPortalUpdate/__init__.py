import logging
import os
import azure.functions as func
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from arcgis import GIS
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import load_workbook
import numpy as np
from io import BytesIO
import urllib



def updateExcelDate(date_field):
    excel_date = int(round(date_field,0))
    dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
    return dt

def getRaceDem(fp):
    racial = pd.read_excel(fp, sheet_name='Summary Tables', header = 35, usecols='B:M', nrows=9, engine="openpyxl" )
    racial.columns = ['Race_Ethnicity', 'Cases_Number', 'Cases_Percentage', 'Cases_Rate', 'Hospital_Number','Hospital_Percentage','Hospital_Rate','Death_Number','Death_Percentage','Death_Rate','County_Number','County_Percentage']
    return racial

def getAgeDem(fp):
    age = pd.read_excel(fp, sheet_name='Summary Tables', header = 2, usecols='B:M', nrows=5, engine="openpyxl" )
    age.columns = ['Age_Group', 'Cases_Number', 'Cases_Percentage', 'Cases_Rate', 'Hospital_Number','Hospital_Percentage','Hospital_Rate','Death_Number','Death_Percentage','Death_Rate','County_Number','County_Percentage']
    return age

def getGenderDem(fp):
    gender = pd.read_excel(fp, sheet_name='Summary Tables', header = 6, usecols='O:S', nrows=2, engine="openpyxl" )
    gender.columns=['Gender','Cases_Number','Cases_Percentage','SC_Cases_Number','SC_Cases_Percentage']
    return gender

def getCity(fp):
    city = pd.read_excel(fp, sheet_name='Summary Tables', header = 17, usecols='O:T', nrows=8, engine="openpyxl" )
    city.columns=['City','Cases_Number','Cases_Percentage','Cases_Rate','SC_Cases_Number','SC_Cases_Percentage']
    return city

def getVaccine(fp):
    Part3_Vaccine=pd.read_excel(fp, sheet_name='Vaccine', usecols='A:J', engine="openpyxl")
    Part3_Vaccine.columns=['date','vax_recv_SCPH','vax_dist_HP','ind_vax_SCPH_vax','ind_vax2_SCPH_dose2','inject_vax_SCPH','tot_ind_MCES','tot_ind2_MCES_does2','tot_vax_MCES','vax_proj_7_Days']
    return Part3_Vaccine

def getVaccineSummary(fp):
    file = urllib.request.urlopen(fp).read()
    wb = load_workbook(filename = BytesIO(file))
    records =[]
    schema=[]
    sheet_ranges = wb['Vaccine 2']
    schema.append(sheet_ranges['F3'].value)
    schema.append(sheet_ranges['F5'].value)
    schema.append(sheet_ranges['F6'].value)
    schema.append(sheet_ranges['J5'].value)
    schema.append(sheet_ranges['J6'].value)
    sheet_ranges=wb['Booster and BT']
    schema.append(sheet_ranges['A2'].value)
    schema.append(sheet_ranges['B2'].value)
    sheet_ranges=wb['Summary Tables']
    schema.append(sheet_ranges['C8'].value)
    schema.append(sheet_ranges['P2'].value)
    schema.append(sheet_ranges['F8'].value)
    schema.append(sheet_ranges['I8'].value)
    schema.append(sheet_ranges['P4'].value)
    schema.append(sheet_ranges['S2'].value)


    records.append(schema)
    Vaccine_Summary = pd.DataFrame(records,columns=['doses18','resvax18','pctvax18','fullvax','pctfullvax','Vaccine_BT_Percentage','Number_Boosted','Cumulative_Cases','Active_cases','total_hospitalizations','total_deaths','residents_tested','total_tests_performed'])
    return Vaccine_Summary


def getPercentPositivity(fp):
    Percent_Positivity=pd.read_excel(fp, sheet_name=2, usecols='A:I', engine="openpyxl")
    return Percent_Positivity


def getHostpitalStats(fp):
    Hospital_Stats=pd.read_excel(fp, sheet_name='Summary Tables',header = 1, usecols='W:Z', engine="openpyxl" )
    Hospital_Stats['CalcDate']=Hospital_Stats.apply(lambda x: updateExcelDate(x['Date']),axis=1)
    return Hospital_Stats

def getEpiCurve(fp):
    Epi_Curve=pd.read_excel(fp, sheet_name='Epi Curve', usecols='B:J', engine="openpyxl" )
    Epi_Curve.columns =['Date_collected', 'Daily_number', 'Running_Daily_Average_Number_7D',
        'Running_Daily_Average_Rate_7Day',
        'Running_Average_14D', 'Running_Total_Number_7D',
        'Cumulative_Running_Rate_7Day',
        'Cumulative_Running_Total_Number_14D',
        'Cumulative_Running_Rate_14Day']
    return Epi_Curve

def getRaceRate(fp):
    RaceRate=pd.read_excel(fp, sheet_name='Vaccine 2', header = 11, usecols='I:N',nrows=10, engine="openpyxl"  )
    RaceRate.columns = ['Race','Total Cases','Percent','Rate','SC_Total','SC_Percent']
    return RaceRate

def getAgeRate(fp):
    AgeRate=pd.read_excel(fp, sheet_name='Vaccine 2', header = 21, usecols='B:G',nrows=8 , engine="openpyxl" )
    AgeRate.columns = ['Age_Group','Total Cases','Percent','Rate','SC_Total','SC_Percent']
    return AgeRate

def getGenderRate(fp):
    GenderRate=pd.read_excel(fp, sheet_name='Vaccine 2', header = 32, usecols='B:G',nrows=4, engine="openpyxl" )
    GenderRate.columns = ['Gender','Total Cases','Percent','Rate','SC_Total','SC_Percent']
    return GenderRate

def getCityRate(fp):
    CityRate=pd.read_excel(fp, sheet_name='Vaccine 2', header = 24, usecols='I:N',nrows=10 , engine="openpyxl")
    CityRate.columns = ['City','Total Cases','Percent','Rate','SC_Total','SC_Percent']
    return CityRate


def getCityFlatten(city):
    city.columns= ['City','Total Cases','Percent','Rate','SC_Total','SC_Percent']
    CityFlat = city.unstack().to_frame().sort_index(level=1).T
    col_list=[]
    for col in CityFlat.columns:
        col_name=col[0] + str(col[1])
        col_list.append(col_name)
    CityFlat.columns=col_list
    return CityFlat


def getItemInfo(Id,gis):
    ## retieve item from GIS and create a template feature for update
    item = gis.content.get(Id)
    tbl = item.tables[0]
    return [tbl]


def cleanDF(df):
    ##Cleans data frame to return data in a cleaned fashion
    df.columns= df.columns.str.lower()
    df = df.replace({np.nan:None})
    insert_dict=df.to_dict('records')
    return insert_dict

def cleanInsertDictionary(additions):
    ##Removes null values from dataframe
    cleanAdds=[]
    for x in additions:
        delete = []
        for key, val in x.items():
            if val is None:
                delete.append(key)           
        for i in delete:
            del x[i]
        
        cleanAdds.append(x)
    return cleanAdds

def DeleteAppend(table_service,new_features):
    del_records = table_service.delete_features(where="objectid >= 0")
    del_records
    add_records = table_service.edit_features(adds=new_features)
    add_records

def UpdateVaccineSummary(df,gis):
    items = getItemInfo('fa3cab1fa7eb4544a9e9c2fffc870e49',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "doses18": x['doses18'],
                        "cumulative_cases": x['cumulative_cases'],
                        "residents_tested": x['residents_tested'],
                        "vaccine_bt_percentage": x['vaccine_bt_percentage'],
                        "resvax18": x['resvax18'],
                        "number_boosted": x['number_boosted'],
                        "total_tests_performed": x['total_tests_performed'],
                        "total_deaths": x['total_deaths'],
                        "pctfullvax": x['pctfullvax'],
                        "pctvax18": x['pctvax18'],
                        "fullvax": x['fullvax'],
                        "active_cases": x['active_cases'],
                        "total_hospitalizations": x['total_hospitalizations']
                        }
                    }
    
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)


def UpdateAgeDem(df,gis):
    items = getItemInfo('b3307e55fedb422ca9ba67b35e75fb0c',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "county_number": x['county_number'],
                        "cases_rate": x['cases_rate'],
                        "hospital_number": x['hospital_number'],
                        "hospital_percentage": x['hospital_percentage'],
                        "age_group": x['age_group'],
                        "county_percentage": x['county_percentage'],
                        "death_number": x['death_number'],
                        "cases_number": x['cases_number'],
                        "cases_percentage": x['cases_percentage'],
                        "death_percentage": x['death_percentage'],
                        "hospital_rate": x['hospital_rate'],
                        "death_rate": x['death_rate']
                        }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateRaceDem(df,gis):
    items = getItemInfo('4c8d4f2cfc234651a685bc22807e4e5f',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "county_number": x['county_number'],
                        "race_ethnicity": x['race_ethnicity'],
                        "cases_rate": x['cases_rate'],
                        "hospital_number": x['hospital_number'],
                        "hospital_percentage": x['hospital_percentage'],
                        "county_percentage": x['county_percentage'],
                        "death_number": x['death_number'],
                        "cases_number": x['cases_number'],
                        "cases_percentage": x['cases_percentage'],
                        "death_percentage": x['death_percentage'],
                        "hospital_rate": x['hospital_rate'],
                        "death_rate": x['death_rate']
                         }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateGenderDem(df,gis):
    items = getItemInfo('ed16496ab60a477bb67a292624769cf9',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "sc_cases_percentage": x['sc_cases_percentage'],
                        "gender": x['gender'],
                        "sc_cases_number": x['sc_cases_number'],
                        "cases_number": x['cases_number'],
                        "cases_percentage": x['cases_percentage']
                        }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)


def UpdateCityDem(df,gis):
    items = getItemInfo('d7c68424afb44a5aa117fbe41eef34cd',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "cases_rate": x['rate'],
                        "sc_cases_percentage": x['sc_percent'],
                        "city": x['city'],
                        "sc_cases_number": x['sc_total'],
                        "cases_number": x['total cases'],
                        "cases_percentage": x['percent']
                        }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateVaccine(df,gis):
    items = getItemInfo('32788636976b4ad1816538c5ab65aa94',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "vax_recv_scph": x['vax_recv_scph'],
                        "tot_vax_mces": x['tot_vax_mces'],
                        "inject_vax_scph": x['inject_vax_scph'],
                        "vax_dist_hp": x['vax_dist_hp'],
                        "ind_vax_scph_vax": x['ind_vax_scph_vax'],
                        "tot_ind_mces": x['tot_ind_mces'],
                        "vax_proj_7_days": x['vax_proj_7_days'],
                        "date_": x['date'],
                        "tot_ind2_mces_does2": x['tot_ind2_mces_does2'],
                        "ind_vax2_scph_dose2": x['ind_vax2_scph_dose2']
                        }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)


def UpdatePerPositive(df,gis):
    items = getItemInfo('32cf3ff6fa5f4a509db404780b4e7d42',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "calredie_copia_specimen_date": x['calredie/copia specimen date'],
                        "number_cases___calredie_copia": x['number cases - calredie/copia '],
                        "number_not_positive": x['number not positive'],
                        "daily_percent_positive": x['daily percent positive'],
                        "overall_percent_positive": x['overall percent positive'],
                        "number_tests_performed": x['number tests performed'],
                        "f14_day_running_percent_positiv": x['14-day running percent positive'],
                        "number_positive___ncov": x['number positive - ncov'],
                        "f7_day_running_percent_positive": x['7-day running percent positive']
                        }
                    }
        additions.append(new_record)

    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)


def UpdateHospital(df,gis):
    items = getItemInfo('26fd3f479a214bd3a230e12cfe58ab09',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "number_inpatient_by_day": x['number inpatient by day'],
                        "vent_availability____": x['vent availability (%)'],
                        "icu_availability____": x['icu availability (%)'],
                        "date_": x['calcdate']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateEpiCurve(df,gis):
    items = getItemInfo('34d58dab76fb4da2b78444624d1083d0',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "running_average___14d": x['running_average_14d'],
                        "running_daily_average_number___": x['running_daily_average_number_7d'],
                        "cumulative_running_total_number": x['cumulative_running_total_number_14d'],
                        "running_daily_average_rate__per": x['running_daily_average_rate_7day'],
                        "cumulative_running_rate__per_11": x['cumulative_running_rate_14day'],
                        "date_collected": x['date_collected'],
                        "daily_number": x['daily_number'],
                        "running_total_number___7d": x['running_total_number_7d'],
                        "cumulative_running_rate__per_10": x['cumulative_running_rate_7day']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateRaceRates(df,gis):
    items = getItemInfo('158e538fc56f4d1ab8ed89295ce084e6',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "total_cases": x['total cases'],
                        "race": x['race'],
                        "rate": x['rate'],
                        "sc_total": x['sc_total'],
                        "sc_percent": x['sc_percent'],
                        "percent_": x['percent']
                         }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateAgeRates(df,gis):
    items = getItemInfo('75b19fe35847433aa024e25db9c84c4a',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "total_cases": x['total cases'],
                        "rate": x['rate'],
                        "sc_total": x['sc_total'],
                        "age_group": x['age_group'],
                        "sc_percent": x['sc_percent'],
                        "percent_": x['percent']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateGenderRates(df,gis):
    items = getItemInfo('7c173309a7fb47fa9b52d181da3104bf',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "total_cases": x['total cases'],
                        "gender": x['gender'],
                        "rate": x['rate'],
                        "sc_total": x['sc_total'],
                        "sc_percent": x['sc_percent'],
                        "percent_": x['percent']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateCityRates(df,gis):
    items = getItemInfo('3114665b4e06499aa59f2e1d82178a04',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "total_cases": x['total cases'],
                        "city": x['city'],
                        "rate": x['rate'],
                        "sc_total": x['sc_total'],
                        "sc_percent": x['sc_percent'],
                        "percent_": x['percent']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)

def UpdateCityFlatten(df,gis):
    items = getItemInfo('25d8cc653674462a80a8be30365d3b23',gis)
    insert_dict = cleanDF(df)
    additions=[]
    for x in insert_dict:
        new_record={
                    "attributes": {
                        "city0": x['city0'],
                        "city1": x['city1'],
                        "city2": x['city2'],
                        "percent1": x['percent1'],
                        "city7": x['city7'],
                        "percent2": x['percent2'],
                        "percent3": x['percent3'],
                        "percent4": x['percent4'],
                        "city3": x['city3'],
                        "city4": x['city4'],
                        "city5": x['city5'],
                        "percent0": x['percent0'],
                        "city6": x['city6'],
                        "sc_percent2": x['sc_percent2'],
                        "sc_percent3": x['sc_percent3'],
                        "sc_percent4": x['sc_percent4'],
                        "sc_percent5": x['sc_percent5'],
                        "percent5": x['percent5'],
                        "percent6": x['percent6'],
                        "sc_percent0": x['sc_percent0'],
                        "percent7": x['percent7'],
                        "sc_percent1": x['sc_percent1'],
                        "rate4": x['rate4'],
                        "rate5": x['rate5'],
                        "rate6": x['rate6'],
                        "rate7": x['rate7'],
                        "rate0": x['rate0'],
                        "rate1": x['rate1'],
                        "rate2": x['rate2'],
                        "rate3": x['rate3'],
                        "sc_percent6": x['sc_percent6'],
                        "sc_percent7": x['sc_percent7'],
                        "sc_total3": x['sc_total3'],
                        "sc_total4": x['sc_total4'],
                        "sc_total1": x['sc_total1'],
                        "sc_total2": x['sc_total2'],
                        "sc_total0": x['sc_total0'],
                        "sc_total7": x['sc_total7'],
                        "sc_total5": x['sc_total5'],
                        "sc_total6": x['sc_total6'],
                        "total_cases1": x['total cases1'],
                        "total_cases0": x['total cases0'],
                        "total_cases3": x['total cases3'],
                        "total_cases2": x['total cases2'],
                        "total_cases5": x['total cases5'],
                        "total_cases4": x['total cases4'],
                        "total_cases7": x['total cases7'],
                        "total_cases6": x['total cases6']
                        }
                    }
        additions.append(new_record)
    cleanAdds = cleanInsertDictionary(additions)
    DeleteAppend(items[0],cleanAdds)



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    file_name = req.params.get('file_name')
    if not file_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            file_name = req_body.get('file_name')
    
    org=os.environ["agol_org"]
    user=os.environ["agol_username"]
    password= os.environ["agol_password"]
    
    gis = GIS(org, user, password)
    name = str(gis.properties.user.username)

    account_name=os.environ["a_name"]
    account_key = os.environ["a_key"]
    container_name =os.environ["c_name"]
    blob_name = file_name

    blob =  generate_blob_sas(account_name=account_name, container_name=container_name,blob_name=blob_name,account_key=account_key, permission=BlobSasPermissions(read=True), expiry=datetime.utcnow() + timedelta(hours=1))
    HSS= "https://solanocountycovidupdate.blob.core.windows.net/"+container_name+"/"+blob_name+"?"+blob


    AgeDem=getAgeDem(HSS)
    RaceDem=getRaceDem(HSS)
    GenDem =getGenderDem(HSS)
    CityDem = getCity(HSS)
    Vaccine = getVaccine(HSS)
    VaccineSum = getVaccineSummary(HSS)
    PerPositve = getPercentPositivity(HSS)
    Hospital = getHostpitalStats(HSS)
    EPICurve = getEpiCurve(HSS)
    RaceRates = getRaceRate(HSS)
    AgeRates = getAgeRate(HSS)
    GenderRates = getGenderRate(HSS)
    CityRates = getCityRate(HSS)
    CityFlatten = getCityFlatten(CityDem)

    UpdateAgeDem(AgeDem,gis)
    print("Updated Age Dem")
    UpdateRaceDem(RaceDem,gis)
    print("Updated Race Dem")
    UpdateGenderDem(GenDem,gis)
    print("Updated Gender Dem")
    UpdateCityDem(CityDem,gis)
    print("Updated City Dem")
    UpdateVaccine(Vaccine,gis)
    print("Updated Vaccine")
    UpdateVaccineSummary(VaccineSum,gis)
    print("Updated Vaccine Summary")
    UpdatePerPositive(PerPositve,gis)
    print("Updated Percent Positivity")
    UpdateHospital(Hospital,gis)
    print("Updated Hospital Stats")
    UpdateEpiCurve(EPICurve,gis)
    print("Updated Epi Curve")
    UpdateRaceRates(RaceRates,gis)
    print("Updated Race Rates")
    UpdateAgeRates(AgeRates,gis)
    print("Updated Age Rates")
    UpdateGenderRates(GenderRates,gis)
    print("Updated Gender Rates")
    UpdateCityRates(CityRates,gis)
    print("Updated City Rates")
    UpdateCityFlatten(CityFlatten,gis)
    print("Updated City Flatten")

    print (account_key)

    if file_name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function has recieved {file_name}.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
