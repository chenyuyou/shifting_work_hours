import numpy as np
import cupy as cp
import xarray as xr

# 定义常量
PI = 3.1415926535897932
TWOPI = 6.2831853071795864
DEG_RAD = 0.017453292519943295
RAD_DEG = 57.295779513082323
SOLAR_CONST = 1367.
GRAVITY = 9.807
STEFANB = 5.6696E-8
Cp = 1003.5
M_AIR = 28.97
M_H2O = 18.015
RATIO = (Cp * M_AIR / M_H2O)
R_GAS = 8314.34
R_AIR = (R_GAS / M_AIR)
Pr = (Cp / (Cp + 1.25 * R_AIR))
EMIS_WICK = 0.95
ALB_WICK = 0.4
D_WICK = 0.007
L_WICK = 0.0254
EMIS_GLOBE = 0.95
ALB_GLOBE = 0.05
D_GLOBE = 0.0508
EMIS_SFC = 0.999
ALB_SFC = 0.45
CZA_MIN = 0.00873
NORMSOLAR_MAX = 0.85
REF_HEIGHT = 2.0 
MIN_SPEED = 0.13
CONVERGENCE = 0.1
MAX_ITER = 10


def calculate_zenith_vectorized(day_of_year, lon, lat, hour=12):
    DECL1, DECL2, DECL3, DECL4, DECL5, DECL6, DECL7 = 0.006918, 0.399912, 0.070257, 0.006758, 0.000907, 0.002697, 0.00148
    
    rad_lat = cp.deg2rad(lat)
    gamma = 2 * cp.pi * ((day_of_year - 1) + (hour / 24)) / 365
    
    decl = (DECL1 - DECL2 * cp.cos(gamma) + DECL3 * cp.sin(gamma) - 
            DECL4 * cp.cos(2 * gamma) + DECL5 * cp.sin(2 * gamma) - 
            DECL6 * cp.cos(3 * gamma) + DECL7 * cp.sin(3 * gamma))
    
    cos_zen = cp.sin(rad_lat) * cp.sin(decl) + cp.cos(rad_lat) * cp.cos(decl)
    cos_zen = cp.clip(cos_zen, -1.0, 1.0)
    
    return cos_zen

def estimate_direct_radiation_vectorized(radiation, zenith):
    cos_zenith = cp.cos(zenith)
    dir_norm = cp.where(cos_zenith > 0, 
                        cp.minimum(radiation / cos_zenith, 1098 * cos_zenith**0.5),
                        0)
    return dir_norm * cos_zenith

def solarposition(year, month, day, latitude, longitude):
    # 确保所有输入都是 CuPy 数组
    day_of_year = cp.asarray(day)
    lat = cp.deg2rad(cp.asarray(latitude))
    lon = cp.deg2rad(cp.asarray(longitude))
    
    # 扩展 day_of_year 的维度以匹配 lat 和 lon
    if day_of_year.ndim == 1 and lat.ndim > 1:
        day_of_year = day_of_year[:, cp.newaxis, cp.newaxis]
    
    # 计算太阳赤纬
    gamma = 2 * cp.pi * (day_of_year - 1) / 365
    decl = 0.006918 - 0.399912 * cp.cos(gamma) + 0.070257 * cp.sin(gamma) - \
           0.006758 * cp.cos(2 * gamma) + 0.000907 * cp.sin(2 * gamma) - \
           0.002697 * cp.cos(3 * gamma) + 0.001480 * cp.sin(3 * gamma)
    
    # 计算太阳时角
    local_time = 12  # 假设在正午计算
    equation_of_time = 229.18 * (0.000075 + 0.001868 * cp.cos(gamma) - 0.032077 * cp.sin(gamma) -
                                 0.014615 * cp.cos(2 * gamma) - 0.040849 * cp.sin(2 * gamma))
    
    # 确保 equation_of_time 的形状正确
    if equation_of_time.ndim < lon.ndim:
        equation_of_time = equation_of_time.reshape(equation_of_time.shape + (1,) * (lon.ndim - equation_of_time.ndim))
    
    time_offset = equation_of_time + 4 * lon * RAD_DEG - 60 * 0  # 假设时区为0
    hour_angle = (local_time + time_offset / 60 - 12) * 15 * DEG_RAD
    
    # 计算天顶角余弦值
    cos_zenith = cp.sin(lat) * cp.sin(decl) + cp.cos(lat) * cp.cos(decl) * cp.cos(hour_angle)
    
    return cos_zenith

def calc_solar_parameters(year, month, day, lat, lon, solar):
    cos_zenith = solarposition(year, month, day, lat, lon)
    
    # 确保 solar 的形状与 cos_zenith 匹配
    if solar.ndim < cos_zenith.ndim:
        solar = solar.reshape(solar.shape + (1,) * (cos_zenith.ndim - solar.ndim))
    
    toasolar = SOLAR_CONST * cp.maximum(0., cos_zenith)
    toasolar = cp.where(cos_zenith < CZA_MIN, 0., toasolar)
    normsolar = cp.minimum(solar / toasolar, NORMSOLAR_MAX)
    solar = normsolar * toasolar
    fdir = cp.where(normsolar > 0,
                    cp.exp(3. - 1.34 * normsolar - 1.65 / normsolar),
                    0.)
    fdir = cp.clip(fdir, 0., 0.9)
    return solar, cos_zenith, fdir

def esat(tk, phase):
    if phase == 0:  # over liquid water
        y = (tk - 273.15) / (tk - 32.18)
        es = 6.1121 * cp.exp(17.502 * y)
    else:  # over ice
        y = (tk - 273.15) / (tk - 0.6)
        es = 6.1115 * cp.exp(22.452 * y)
    return 1.004 * es

def dew_point(e, phase):
    if phase == 0:  # dew point
        z = cp.log(e / (6.1121 * 1.004))
        tdk = 273.15 + 240.97 * z / (17.502 - z)
    else:  # frost point
        z = cp.log(e / (6.1115 * 1.004))
        tdk = 273.15 + 272.55 * z / (22.452 - z)
    return tdk

def viscosity(Tair):
    sigma = 3.617
    eps_kappa = 97.0
    Tr = Tair / eps_kappa
    omega = (Tr - 2.9) / 0.4 * (-0.034) + 1.048
    return 2.6693E-6 * cp.sqrt(M_AIR * Tair) / (sigma * sigma * omega)

def thermal_cond(Tair):
    return (Cp + 1.25 * R_AIR) * viscosity(Tair)

def diffusivity(Tair, Pair):
    Pcrit13 = (36.4 * 218) ** (1 / 3)
    Tcrit512 = (132 * 647.3) ** (5 / 12)
    Tcrit12 = cp.sqrt(132 * 647.3)
    Mmix = cp.sqrt(1 / M_AIR + 1 / M_H2O)
    Patm = Pair / 1013.25
    return 3.64E-4 * (Tair / Tcrit12) ** 2.334 * Pcrit13 * Tcrit512 * Mmix / Patm * 1E-4

def evap(Tair):
    return (313.15 - Tair) / 30. * (-71100.) + 2.4073E6

def emis_atm(Tair, rh):
    e = rh * esat(Tair, 0)
    return 0.575 * e ** 0.143

def h_cylinder_in_air(diameter, length, Tair, Pair, speed):
    a, b, c = 0.56, 0.281, 0.4
    density = Pair * 100. / (R_AIR * Tair)
    Re = cp.maximum(speed, MIN_SPEED) * density * diameter / viscosity(Tair)
    Nu = b * Re ** (1. - c) * Pr ** (1. - a)
    return Nu * thermal_cond(Tair) / diameter

def h_sphere_in_air(diameter, Tair, Pair, speed):
    density = Pair * 100. / (R_AIR * Tair)
    Re = cp.maximum(speed, MIN_SPEED) * density * diameter / viscosity(Tair)
    Nu = 2.0 + 0.6 * cp.sqrt(Re) * Pr ** 0.3333
    return Nu * thermal_cond(Tair) / diameter

def Twb(Tair, rh, Pair, speed, solar, fdir, cza):
    a = 0.56
    Tsfc = Tair
    sza = cp.arccos(cza)
    eair = rh * esat(Tair, 0)
    Tdew = dew_point(eair, 0)
    Twb_prev = Tdew
    
    for i in range(MAX_ITER):
        Tref = 0.5 * (Twb_prev + Tair)
        h = h_cylinder_in_air(D_WICK, L_WICK, Tref, Pair, speed)
        Fatm = (STEFANB * EMIS_WICK * 
                (0.5 * (emis_atm(Tair, rh) * Tair**4 + EMIS_SFC * Tsfc**4) - Twb_prev**4) +
                (1. - ALB_WICK) * solar * 
                ((1. - fdir) * (1. + 0.25 * D_WICK / L_WICK) + 
                 fdir * ((cp.tan(sza) / PI) + 0.25 * D_WICK / L_WICK) + ALB_SFC))
        ewick = esat(Twb_prev, 0)
        density = Pair * 100. / (R_AIR * Tref)
        Sc = viscosity(Tref) / (density * diffusivity(Tref, Pair))
        Twb_new = (Tair - evap(Tref) / RATIO * (ewick - eair) / (Pair - ewick) * 
                   (Pr / Sc) ** a + Fatm / h)
        
        if cp.abs(Twb_new - Twb_prev).max() < CONVERGENCE:
            return cp.maximum(Twb_new - 273.15, -100)  # 限制最小值为 -100°C
        
        Twb_prev = 0.9 * Twb_prev + 0.1 * Twb_new
 
    return cp.maximum(Twb_prev - 273.15, -100)  # 限制最小值为 -100°C

def Tglobe(Tair, rh, Pair, speed, solar, fdir, cza):
    Tsfc = Tair
    Tglobe_prev = Tair
    
    for i in range(MAX_ITER):
        Tref = 0.5 * (Tglobe_prev + Tair)
        h = h_sphere_in_air(D_GLOBE, Tref, Pair, speed)
        Tglobe_new = ((0.5 * (emis_atm(Tair, rh) * Tair**4 + EMIS_SFC * Tsfc**4) -
                       h / (STEFANB * EMIS_GLOBE) * (Tglobe_prev - Tair) +
                       solar / (2. * STEFANB * EMIS_GLOBE) * (1. - ALB_GLOBE) * 
                       (fdir * (1. / (2. * cza) - 1.) + 1. + ALB_SFC)) ** 0.25)
        
        if cp.abs(Tglobe_new - Tglobe_prev).max() < CONVERGENCE:
            return cp.maximum(Tglobe_new - 273.15, -100)  # 限制最小值为 -100°C
        
        Tglobe_prev = 0.9 * Tglobe_prev + 0.1 * Tglobe_new
    
 
    return cp.maximum(Tglobe_prev - 273.15, -100)  # 限制最小值为 -100°C

def calculate_wbgt_vectorized(t_k, rh, wind_speed, radiation, day_of_year, lat, lon, pressure=1010):
    # 确保输入数据在GPU上并且形状正确
    t_k = cp.asarray(t_k)
    rh = cp.asarray(rh)
    wind_speed = cp.asarray(wind_speed)
    radiation = cp.asarray(radiation)
    day_of_year = cp.asarray(day_of_year)
    lat = cp.asarray(lat)
    lon = cp.asarray(lon)
    pressure = cp.asarray(pressure)

    # 确保 day_of_year 的形状正确
    if day_of_year.ndim == 1:
        day_of_year = day_of_year[:, cp.newaxis, cp.newaxis]

    # 使用calc_solar_parameters替代之前的计算
    radiation, cza, fdir = calc_solar_parameters(2021, 1, day_of_year, lat, lon, radiation)
    


    t_nwb = Twb(t_k, rh/100, pressure, wind_speed, radiation, fdir, cza)
    t_g = Tglobe(t_k, rh/100, pressure, wind_speed, radiation, fdir, cza)



    wbgt = 0.7 * t_nwb + 0.2 * t_g + 0.1 * (t_k - 273.15)



    return cp.asnumpy(wbgt), cp.asnumpy(t_nwb), cp.asnumpy(t_g)

def wbgt_liljegren_vectorized(combined_ds):
    tas = cp.asarray(combined_ds['tas'].values)
    tasmax = cp.asarray(combined_ds['tasmax'].values)
    hurs = cp.asarray(combined_ds['hurs'].values)
    sfcWind = cp.asarray(combined_ds['sfcWind'].values)
    rsds = cp.asarray(combined_ds['rsds'].values)
    lat = cp.asarray(combined_ds.lat.values)
    lon = cp.asarray(combined_ds.lon.values)
    day_of_year = cp.asarray(combined_ds.time.dt.dayofyear.values)
    
    # 确保 lat 和 lon 是 2D 数组
    if lat.ndim == 1 and lon.ndim == 1:
        lon, lat = cp.meshgrid(lon, lat)

    # 计算 WBGT
    wbgt_min, t_nwb, t_g = calculate_wbgt_vectorized(tas, hurs, sfcWind, rsds, day_of_year, lat, lon)
    wbgt_max, _, _ = calculate_wbgt_vectorized(tasmax, hurs, sfcWind, rsds, day_of_year, lat, lon)
    
    # 将结果转换回NumPy数组
    return np.array(wbgt_min), np.array(wbgt_max), np.array(t_nwb), np.array(t_g)