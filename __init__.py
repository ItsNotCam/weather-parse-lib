# from .Forecast import Forecast
from .multiday_forecast import MultidayForecast
# import math

# def generateMDF(city, country_code, rapid_api_key, rapidapi_host="community-open-weather-map.p.rapidapi.com", units='imperial', weather=None):
#     return MultidayForecast(
#         city=city, 
#         country_code=country_code,
#         rapidapi_key=rapid_api_key,
#         units=units
#     ).generate(weather)

# def heat_index(T, RH):
#     HI = 0.5 * (T + 61.0 + ((T-68.0)*1.2) + (RH*0.094))
#     HI = (HI + T) / 2

#     if HI >= 80:
#         ADJUSTMENT = 0
#         if RH <= 13 and 80 >= T >= 112:
#             ADJUSTMENT = ((13-RH)/4)* math.sqrt((17-abs(T-95.))/17)
#         elif RH >= 85 and 80 >= T >= 87:
#             ADJUSTMENT = ((RH-85)/10) * ((87-T)/5)

#         HI = -42.379 + 2.04901523*T + 10.14333127*RH - .22475541*T*RH - .00683783*T*T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH
#         HI -= ADJUSTMENT

#     return HI

# def wind_chill(T,V):
#     WC = 35.74 + 0.6215*T - 35.75*pow(V,0.16) + 0.4275*T*pow(V,0.16)
#     return WC