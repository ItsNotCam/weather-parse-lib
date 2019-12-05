from datetime import datetime, timedelta
import sqlite3
import json


class Forecast():
    def __init__(self, date, forecast=None):
        self.date = date
        self.cnx = sqlite3.connect(":memory:")
        self.cnx.execute('''
            CREATE TABLE weather(
                time            TIMESTAMP PRIMARY KEY,
                temp_avg        FLOAT,
                temp_hi         FLOAT,
                temp_lo         FLOAT,
                humidity        INT,
                clouds          INT,
                wind            INT,
                rain            FLOAT,
                snow            FLOAT,
                wind_chill      FLOAT,
                heat_index      FLOAT,
                apparent_temp   FLOAT
            );'''
                         )
        self.cnx.commit()

        if forecast is not None:
            self.populate(forecast)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4)

    def to_dict(self):
        return {
            "date": str(self.date.date()),
            "average_temp": self.average_temperature(),
            "highest_temp": self.highest_temperature(),
            "lowest_temp":  self.lowest_temperature(),
            "total_rain": self.total_rain(),
            "total_snow": self.total_snow(),
            "wind": self.average_wind(),
            "clouds": self.average_clouds(),
            "weather": [
                {
                    "time": res[0],
                    "wind": res[6],
                    "rain": res[7],
                    "snow": res[8],
                    "clouds": res[5],
                    "temp_hi": res[2],
                    "temp_lo": res[3],
                    "temp_avg": res[1],
                    "humidity": res[4],
                    "wind_chill": res[9],
                    "heat_index": res[10],
                    "apparent_temp": res[11]
                } for res in self.cnx.execute("SELECT * FROM weather;")
            ]
        }

    def populate(self, forecast):
        for data in forecast:
            time = data[1]
            wind = data[7]
            rain = data[8]
            snow = data[9]
            clouds = data[6]
            temp_hi = data[3]
            temp_lo = data[4]
            temp_avg = data[2]
            humidity = data[5]
            wind_chill = data[10]
            heat_index = data[11]
            apparent_temp = data[12]

            query = ''' 
                INSERT INTO weather(
                    time, temp_avg, temp_hi, temp_lo, humidity,
                    clouds, wind, rain, snow, wind_chill, heat_index,
                    apparent_temp
                ) VALUES (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                );
            '''.format(time, temp_avg, temp_hi, temp_lo, humidity, clouds, wind,
                       rain, snow, wind_chill, heat_index, apparent_temp)

            self.cnx.execute(query)

        self.cnx.commit()

    def __get_extreme_from_column(self, extreme, column):
        query = f"SELECT time,{extreme}({column}) FROM weather;"
        result = self.cnx.execute(query).fetchone()
        return {"time": result[0], "val": result[1]}

    def __get_sum_of_column(self, column):
        query = f"SELECT SUM({column}) FROM weather;"
        result = self.cnx.execute(query).fetchone()
        return result[0]

    def __get_avg_of_column(self, column):
        query = f"SELECT AVG({column}) FROM weather;"
        result = self.cnx.execute(query).fetchone()
        return result[0]
    
    def __tod_to_dt(self, time_of_day):
        if time_of_day == 'morning':
            time_start = self.date.replace(hour=1).timestamp()
            time_end = self.date.replace(hour=11).timestamp()
        elif time_of_day == 'afternoon':
            time_start = self.date.replace(hour=12).timestamp()
            time_end = self.date.replace(hour=15).timestamp()
        elif time_of_day == 'evening':
            time_start = self.date.replace(hour=16).timestamp()
            time_end = self.date.replace(hour=18).timestamp()
        else:
            time_start = self.date.replace(hour=19).timestamp()
            time_end = self.date.replace(hour=23).timestamp()
        
        return (time_start, time_end)

    ########
    # RAIN #
    ########
    def will_rain(self, time_of_day=None):
        if time_of_day is None:
            return self.__get_sum_of_column("rain") > 0
        
        time_start, time_end = self.__tod_to_dt(time_of_day)        

        query = '''        
             SELECT SUM(rain)
             FROM weather
             WHERE DATETIME(time,\"unixepoch\") 
                BETWEEN 
                    DATETIME({},\"unixepoch\") 
                    AND 
                    DATETIME({},\"unixepoch\")
         '''.format(time_start,time_end)

        result = self.cnx.execute(query).fetchone()
        return False if result[0] is None else result[0] > 0

    def rain_times(self):
        query = "SELECT time,rain FROM weather WHERE rain > 0;"
        result = self.cnx.execute(query).fetchall()
        times_dict = [{
            "time": res[0],
            "val": res[1]
        } for res in result]

        return times_dict

    def most_rain(self):
        return self.__get_extreme_from_column("MAX", "rain")

    def least_rain(self):
        return self.__get_extreme_from_column("MIN", "rain")

    def total_rain(self):
        return self.__get_sum_of_column("rain")

    def average_rain(self):
        return self.__get_avg_of_column("rain")

    ########
    # SNOW #
    ########
    def will_snow(self):
        return self.__get_sum_of_column("snow") > 0

    def snow_times(self):
        query = "SELECT time,snow FROM weather WHERE snow > 0;"
        result = self.cnx.execute(query).fetchall()
        times_dict = [{
            "time": res[0],
            "snow": res[1]
        } for res in result]

        return times_dict

    def most_snow(self):
        return self.__get_extreme_from_column("MAX", "snow")

    def least_snow(self):
        return self.__get_extreme_from_column("MIN", "snow")

    def total_snow(self):
        return self.__get_sum_of_column("snow")

    def average_snow(self):
        return self.__get_avg_of_column("snow")

    ###############
    # TEMPERATURE #
    ###############
    def lowest_temperature(self):
        return self.__get_extreme_from_column("MIN", "temp_lo")

    def highest_temperature(self):
        return self.__get_extreme_from_column("MAX", "temp_hi")

    def average_temperature(self):
        return self.__get_avg_of_column("temp_avg")
        
    ########################
    # APPARENT TEMPERATURE #
    ########################
    def average_apparent_temp(self):
        return self.__get_avg_of_column("apparent_temp")
    
    def highest_apparent_temp(self):
        return self.__get_extreme_from_column("MAX","apparent_temp")
    
    def lowest_apparent_temp(self):
        return self.__get_extreme_from_column("MIN","apparent_temp")

    ############
    # HUMIDITY #
    ############
    def average_humidity(self):
        return self.__get_avg_of_column("humidity")
    
    def highest_humidity(self):
        return self.__get_extreme_from_column("MAX","humidity")
    
    def lowest_humidity(self):
        return self.__get_extreme_from_column("MIN","humidity")
    
    ########
    # WIND #
    ########
    def average_wind(self):
        return self.__get_avg_of_column("wind")
    
    def highest_wind(self):
        return self.__get_extreme_from_column("MAX","wind")
    
    def lowest_wind(self):
        return self.__get_extreme_from_column("MIN","wind")

    ##########
    # CLOUDS #
    ##########
    def average_clouds(self):
        return self.__get_avg_of_column("clouds")
    
    def most_cloudy(self):
        return self.__get_extreme_from_column("MAX","clouds")
    
    def least_cloudy(self):
        return self.__get_extreme_from_column("MIN","clouds")