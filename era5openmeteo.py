#%%
import requests
import pandas as pd
import numpy as np
import datetime
import os
import time
import pcse


def df_to_met(weatherfile, df, device, latitude, longitude, annual_Tmean, annual_Tamplitude):
    """
    input:
    - weatherfile: name of weatherfile to overwrite
    - df: pandas dataframe
                columns:
                - 'year' year
                - 'day' julian day
                - 'radn' radiation in MJ/m**2
                - 'maxt' in ºC
                - 'mint' in ºC
                - 'rain' in mm
                - "rh" in %
                - "windspeed" in m/s
    - device: station number
    - latitude: latitude of station in degrees
    - longitude: longitude of station in degrees
    - annual_Tmean: climatological annual daily Tmean ºC
    - annual_Tamplitude: annual amplitude in mean monthly temperature ºC

    Return: overwrite weatherfile with a .met file in the format of APSIM
    """

    os.makedirs(os.path.split(weatherfile)[0], exist_ok=True)

    annual_average = annual_Tmean
    annual_amplitude = annual_Tamplitude
    created = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")

    header = f"""
[weather.met.weather]
!station number = {device}
!station name = JOKIOINEN
latitude = {latitude} (DECIMAL DEGREES)
longitude = {longitude} (DECIMAL DEGREES)
tav = {annual_average} (oC) ! annual average ambient temperature
amp = {annual_amplitude} (oC) ! annual amplitude in mean monthly temperature
!File created on {created}
!
year day radn maxt mint rain rh wind_speed
 ()   () (MJ/m^2) (oC) (oC) (mm) (%) (m/s)
"""
    f = open(weatherfile, "w")
    f.write(header.lstrip())
    f.write(df.to_string(header=False, index=False) + "\n")
    f.close()

def df_to_csv_wofost(weatherfile, df, device, latitude, longitude, elevation, angstroma, angstromb, hassunshine):
    """
    input:
    - weatherfile: name of weatherfile to overwrite
    - df: pandas dataframe
                columns:
                - 'date' python datetime
                - 'irrad' radiation in kJ/m**2
                - 'tmax' in ºC
                - 'tmin' in ºC
                - 'rain' in mm
                - "vap" in kPa
                - "wind" mean wind speed at 2m height in m/s
    - Latitude: latitude of station in degrees
    - Longitude: longitude of station in degrees
    - Elevation
    - Angstrom A
    - Angstrom B
    - HasSunShine

    Return: overwrite weatherfile with a .csv file in the format of WOFOST CSVWeatherDataProvider https://pcse.readthedocs.io/en/stable/code.html#csvweatherdataprovider
    """

    os.makedirs(os.path.split(weatherfile)[0], exist_ok=True)

    header = f"""
## Site Characteristics
Country = 'Finland'
Station = 'Jokioinen'
Description = 'Jokioinen weather data from local DataSense station'
Source = 'ERA5'
Contact = 'Nadia Testani'
Country = 'Finland'
Longitude = {longitude}; Latitude = {latitude}; Elevation = {elevation}; AngstromA = {angstroma}; AngstromB = {angstromb}; HasSunshine = {hassunshine}
## Daily weather observations (missing values are NaN)
DAY,IRRAD,TMIN,TMAX,VAP,WIND,RAIN,SNOWDEPTH
"""
    
    with open(weatherfile, 'w') as f:
        f.write(header.lstrip())
        f.write(df.to_csv(sep=',', header=False, index=False, na_rep='NaN').replace('\r\n', '\n'))
        f.close()

def df_to_csv_dssat(location, weatherfile, df, latitude, longitude, elevation):
    """
    input:
    - weatherfile: name of weatherfile to overwrite
    - df: pandas dataframe
                columns:
                - 'DATE' YYYYDDD
                - 'SRAD' radiation in MJ/m**2
                - 'TMAX' in ºC
                - 'TMIN' in ºC
                - 'RAIN' in mm
                - "RHUM" in %
    - Latitude: latitude of station in degrees
    - Longitude: longitude of station in degrees
    - Elevation (m)

    Return: overwrite weatherfile with a .csv file in the format of DSSAT 
    """

    os.makedirs(os.path.split(weatherfile)[0], exist_ok=True)
    TAV = ((df['TMAX'] + df['TMIN'])/2).mean() #MEAN TEMPERATURE
    AMP = (df['TMAX'] - df['TMIN']).mean() #TEMPERATURE AMPLITUDE
    REFHT = -99.0 #REFERENCE HEIGHT OF TEMPERATURE MEASUREMENT (m)
    WNDHT = -99.0 #REFERENCE HEIGHT OF WIND SPEED MEASUREMENT (m)
    header = f"""
$WEATHER DATA : {location}

@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT
  XXX   {latitude}   {longitude}    {elevation}   {TAV}  {AMP} {REFHT} {WNDHT}
@ DATE  SRAD  TMAX  TMIN  RAIN  RHUM
"""  
    with open(weatherfile, 'w') as f:
        f.write(header.lstrip())
        f.write(df.to_csv(sep=',', header=False, index=False, na_rep='NaN').replace('\r\n', '\n'))
        f.close()

#%%
def hourly_to_daily(latitude, longitude, startdate, enddate):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={startdate}&end_date={enddate}&hourly=relativehumidity_2m,windspeed_10m,et0_fao_evapotranspiration"
    r = requests.get(url)
    if r.status_code != 200:
        print(r.text)

    odf = pd.DataFrame(r.json()["hourly"])
    odf["time"] = pd.DatetimeIndex(odf["time"])
    odf["date"] = odf.time.dt.date
    del odf["time"]

    #Based on FAO equation 47 in Allen et al (1998)
    #Openmeteo windspeed is km/h
    odf["windspeed_2m"] = (odf["windspeed_10m"]/3.6 ) * (4.87 / np.log((67.8 * 10) - 5.42))
    #print(odf.head())

    daily = odf.groupby("date", as_index=False).agg({"relativehumidity_2m" : "mean",
                                                     "windspeed_2m" : "mean",
                                                     "et0_fao_evapotranspiration" : "sum"
                                                    })
    daily.columns = ["date", "rh", "windspeed", "et0"]
    return daily


def era5_daily(latitude, longitude, startdate, enddate):
    ## Open meteo historical API
    # for ECMWF ERA5 data
    # daily
    # https://open-meteo.com/en/docs/historical-weather-api

    r = requests.get(f"https://archive-api.open-meteo.com/v1/era5?latitude={latitude}&longitude={longitude}&start_date={startdate}&end_date={enddate}&daily=temperature_2m_max,temperature_2m_min,shortwave_radiation_sum,precipitation_sum,temperature_2m_mean&timezone=Europe%2FHelsinki")
    if r.status_code != 200:
        print(r.text)
    odf = pd.DataFrame(r.json()["daily"])
    odf["time"] = pd.DatetimeIndex(odf["time"])
    odf = odf.rename({"temperature_2m_max" : "maxt", "temperature_2m_min" : "mint",
                "shortwave_radiation_sum" : "radn", "precipitation_sum" : "rain",
                "temperature_2m_mean" : "avet"
                }, axis = 1)
    ddf = hourly_to_daily(latitude, longitude, startdate, enddate)


    odf["date"] = odf.time.dt.date
    odf = odf.merge(ddf)
    odf["day"] = odf.time.dt.dayofyear
    odf["year"] = odf.time.dt.year
    odf = odf.fillna(0)
    return odf

def era5_to_apsim(latitude, longitude, startdate, enddate):
    odf = era5_daily(latitude, longitude, startdate, enddate)
    df = odf[['year', 'day', 'radn', 'maxt', 'mint', 'rain', "rh", "windspeed"]]
    df = df.fillna(0)
    df = df.round(1)
    return df

def era5_to_wofost(latitude, longitude, startdate, enddate):
    odf = era5_daily(latitude, longitude, startdate, enddate)
    odf['irrad_kj'] = odf['radn']*10**3
    odf['vap_kpa'] = odf.apply(lambda row: pcse.util.vap_from_relhum(row['rh'], row['avet']), axis=1) # https://github.com/ajwdewit/pcse/blob/0a4386bb4e35d73132b17774b24bba0456019273/pcse/util.py#L373
    odf['date'] = odf.apply(lambda row: str(row['date']).replace('-',''), axis=1)
    odf['snowdepth'] = np.nan
    odf = odf.round(1)
    df = odf[['date', 'irrad_kj', 'mint', 'maxt', "vap_kpa", "windspeed", 'rain', "snowdepth"]]
    #df = df.fillna(0)
    return df

def era5_to_dssat(latitude, longitude, startdate, enddate):
    odf = era5_daily(latitude, longitude, startdate, enddate)
    odf['DATE'] = pd.to_datetime(odf['date']).dt.strftime('%Y%j')
    odf['SRAD'] = odf['radn'] #MJ m-2 día-1
    odf['TMAX'] = odf['maxt'] #°C
    odf['TMIN'] = odf['mint'] #°C
    odf['RAIN'] = odf['rain'] #mm
    odf['RHUM'] = odf['rh'] #%
    odf = odf.round(1)
    df = odf[['DATE', 'SRAD', 'TMAX', 'TMIN', 'RAIN', "RHUM"]]
    df = df.fillna(-99.0)
    return df

#%%
def era5_to_basgra(latitude, longitude, startdate, enddate):
    r = requests.get(f"https://archive-api.open-meteo.com/v1/era5?latitude={latitude}&longitude={longitude}&start_date={startdate}&end_date={enddate}&daily=temperature_2m_max,temperature_2m_min,shortwave_radiation_sum,precipitation_sum,temperature_2m_mean&timezone=Europe%2FHelsinki")
    odf = pd.DataFrame(r.json()["daily"])
    odf["time"] = pd.DatetimeIndex(odf["time"])
    ddf = daily_wind_and_rh(latitude, longitude, startdate, enddate)
    odf = odf.rename({"temperature_2m_max" : "maxt", "temperature_2m_min" : "mint",
                "shortwave_radiation_sum" : "radn", "rain_sum" : "rain",
                "temperature_2m_mean" : "avet"
                }, axis = 1)
    ddf = daily_wind_and_rh(latitude, longitude, startdate, enddate)
    odf["date"] = odf.time.dt.date
    odf = odf.merge(ddf)

    odf["day"] = odf.time.dt.dayofyear
    odf["year"] = odf.time.dt.year
    df = odf[['year', 'day', 'radn', 'avet', 'maxt', 'mint', 'rain', "rh", "windspeed"]]
    df = df.fillna(0)
    df = df.round(1)
    return df

#%%
if __name__ == "__main__":
    # Maaninka data
    #latitude = 63.14
    #longitude = 27.31
    # df = era5_to_apsim(latitude, longitude, "2000-01-01", "2022-12-31")
    #df = era5_to_apsim(latitude, longitude, "1980-01-01", "2022-12-31")
    #df_to_met("../grassmodels/models/maaninka_era5.met", df,  "openmeteo_era5", latitude, longitude, 3, 27)
    # Ruukki
    #latitude, longitude = (64.673390, 25.088409)
    #df = era5_to_apsim(latitude, longitude, "2000-01-01", "2022-12-31")
    #df_to_met("../grassmodels/models/maaninka_era5.met", df,  "openmeteo_era5", latitude, longitude, 3, 27)
    #bdf = era5_to_basgra(latitude, longitude, "2000-01-01", "2022-12-31")
    #bdf.to_csv("../grassmodels/data/maaninka_era5_basgra.csv", index=False)
    #bdf.to_csv("../grassmodels/data/ruukki_era5_basgra.csv", index=False)

    # Jokioinen
    latitude, longitude = (60.388485, 23.106666)
    df = era5_to_apsim(latitude, longitude, "1980-01-01", "2023-12-31")
    df_to_met("data/jokioinen_era5.met", df,  "openmeteo_era5", latitude, longitude, 3, 27)




# %%
