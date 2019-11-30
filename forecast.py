from datetime import datetime, timedelta
import sqlite3
from math import e as E
from math import sqrt
import json

def calc_hi(T, RH):
    HI = 0.5 * (T + 61.0 + ((T-68.0)*1.2) + (RH*0.094))
    HI = (HI + T) / 2

    if HI >= 80:
        ADJUSTMENT = 0
        if RH <= 13 and 80 >= T >= 112:
            ADJUSTMENT = ((13-RH)/4)*sqrt((17-abs(T-95.))/17)
        elif RH >= 85 and 80 >= T >= 87:
            ADJUSTMENT = ((RH-85)/10) * ((87-T)/5)

        HI = -42.379 + 2.04901523*T + 10.14333127*RH - .22475541*T*RH - .00683783*T*T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH
        HI -= ADJUSTMENT

    return HI

def calc_wc(T,V):
    WC = 35.74 + 0.6215*T - 35.75*pow(V,0.16) + 0.4275*T*pow(V,0.16)
    return WC


class Forecast():
    def __init__(self, forecast=None):
        self.cnx = sqlite3.connect(":memory:")
        self.cnx.create_function(
            "APPARENT_TEMPERATURE", 3,
            self.__apparent_temp
        )
        
        self.cnx.execute('''CREATE TABLE weather 
            (_id INTEGER PRIMARY KEY AUTOINCREMENT, dt timestamp, temp_avg FLOAT, 
            temp_hi FLOAT, temp_lo FLOAT, humidity FLOAT, clouds INT, wind INT, rain FLOAT, 
            snow FLOAT, wind_chill FLOAT, heat_index FLOAT, apparent_temp FLOAT);''')

        self.cnx.commit()

        if forecast is not None:
            self.populate(forecast)

    def __del__(self):
        self.cnx.close()
        del self.cnx

    def __repr__(self):
        try:
            query = "SELECT * FROM weather;"
            results = self.cnx.execute(query).fetchall()
            return "\n".join([r for r in results])

        except Exception as e:
            print(e)

    def populate(self, forecast):
        days = forecast["list"]
        try:
            for data in days:
                date = datetime.strptime(
                    data['dt_txt'], "%Y-%m-%d %X").timestamp()
                temp_avg = data['main']['temp']
                temp_hi = data['main']['temp_max']
                temp_lo = data['main']['temp_min']
                humidity = data['main']['humidity']
                clouds = data['clouds']['all']
                wind = data['wind']['speed']
                rain = 0 if 'rain' not in data else data['rain']
                if isinstance(rain, dict):
                    rain = 0 if '3h' not in rain else rain['3h']
                snow = 0 if 'snow' not in data else data['snow']
                if isinstance(snow, dict):
                    snow = 0 if '3h' not in snow else snow['3h']
                wind_chill = calc_wc(temp_avg,wind)
                heat_index = calc_hi(temp_avg,humidity)
                apparent_temp = self.__apparent_temp(temp_avg,humidity,wind)

                query = ''' 
                    INSERT INTO weather(
                        dt, temp_avg, temp_hi, temp_lo, humidity,
                        clouds, wind, rain, snow, wind_chill, heat_index,
                        apparent_temp
                    ) VALUES (
                        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                    );
                '''.format(date, temp_avg, temp_hi, temp_lo, humidity, clouds, wind, 
                    rain, snow, wind_chill, heat_index, apparent_temp)

                self.cnx.execute(query)

        except Exception as e:
            print(e)

        finally:
            self.cnx.commit()

    def __apparent_temp(self, T, RH, W):
        HI = 0.5 * (T + 61.0 + ((T-68.0)*1.2) + (RH*0.094))
        HI = (HI + T) / 2

        if HI >= 80:
            ADJUSTMENT = 0
            if RH <= 13 and 80 >= T >= 112:
                ADJUSTMENT = ((13-RH)/4) * sqrt((17-abs(T-95.))/17)
            elif RH >= 85 and 80 >= T >= 87:
                ADJUSTMENT = ((RH-85)/10) * ((87-T)/5)

            HI = -42.379 + 2.04901523*T + 10.14333127*RH - .22475541*T*RH - .00683783*T * \
                T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH
            HI -= ADJUSTMENT

        return HI - (1.072 * W)

    def __time_range(self, start_dt, end_dt):
        start_dt = datetime.today().timestamp() if start_dt is None \
            else start_dt.timestamp()

        end_dt = (datetime.today() + timedelta(days=5)).timestamp() if end_dt is None \
            else end_dt.timestamp()

        return (start_dt, end_dt)

    def __find_avg(self, start_dt, end_dt, field_name):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                SELECT AVG({})
                FROM weather
                WHERE dt BETWEEN {} AND {}
            '''.format(field_name, start_dt, end_dt)

            result = self.cnx.execute(query).fetchone()
            return result[0]

        except Exception as e:
            print(e)

    def average_rain(self, start_dt=None, end_dt=None):
        return self.__find_avg(start_dt, end_dt, 'rain')

    def average_snow(self, start_dt=None, end_dt=None):
        return self.__find_avg(start_dt, end_dt, 'snow')

    def average_temp(self, start_dt=None, end_dt=None):
        return self.__find_avg(start_dt, end_dt, 'temp_avg')

    def highest_temp(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                SELECT 
                    DATETIME(dt,\'unixepoch\'),
                    MAX(temp_hi) 
                FROM weather 
                WHERE dt BETWEEN {} AND {};
            '''.format(start_dt,end_dt)

            results = self.cnx.execute(query).fetchone()
            return {"dt": results[0],"temp":results[1]}

        except Exception as e:
            print(e)

    def lowest_temp(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = f"SELECT DATETIME(dt,\'unixepoch\'),MIN(temp_lo) FROM weather WHERE dt BETWEEN {start_dt} AND {end_dt};"
            results = self.cnx.execute(query).fetchone()
            return {"dt": results[0],"temp":results[1]}

        except Exception as e:
            print(e)

    def average_temp_on(self, date):
        try:
            query = '''
                SELECT DATE(dt,\'unixepoch\'),AVG(temp_avg)
                FROM weather
                WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\')
            '''.format(date.timestamp())

            result = self.cnx.execute(query).fetchone()
            return {"dt": result[0], "temp": result[1]}

        except Exception as e:
            print(e)

    def forecast_on(self, date):
        try:
            query = '''
                SELECT * FROM weather
                WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\')
            '''.format(date.timestamp())

            results = self.cnx.execute(query).fetchall()
            return [
                {
                    "dt": res[1],
                    "temp_avg": res[2],
                    "temp_hi": res[3],
                    "temp_lo": res[4],
                    "humidity": res[5],
                    "clouds": res[6],
                    "wind": res[7],
                    "rain": res[8],
                    "snow": res[9],
                    "wind_chill": res[10],
                    "heat_index": res[11],
                    "apparent_temp": res[12]
                } for res in results
            ]

        except Exception as e:
            print(e)

    def rainy_days(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        query = '''
            SELECT 
                DATE(dt,\'unixepoch\'),
                SUM(rain)
            FROM weather
            WHERE 
                (dt BETWEEN {} AND {})
                AND rain > 0
            GROUP BY DATE(dt,\'unixepoch\')
        '''.format(start_dt, end_dt)

        results = self.cnx.execute(query).fetchall()
        return [
            {"dt": result[0],"rain": result[1]} 
            for result in results
        ]
    
    def snowy_days(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        query = '''
            SELECT 
                DATE(dt,\'unixepoch\'),
                SUM(snow)
            FROM weather
            WHERE 
                (dt BETWEEN {} AND {})
                AND snow > 0
            GROUP BY DATE(dt,\'unixepoch\')
        '''.format(start_dt, end_dt)

        results = self.cnx.execute(query).fetchall()
        return [
            {"dt": result[0],"snow": result[1]} 
            for result in results
        ]

    def rainiest_day(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                with sums as (
                    select 
                        DATE(dt,\'unixepoch\') as dt, 
                        SUM(rain) as rain 
                    FROM weather 
                    WHERE dt BETWEEN {} and {}
                    GROUP BY DATE(dt,\'unixepoch\')
                )
                SELECT dt,MAX(rain) rain FROM sums
            '''.format(start_dt, end_dt)

            result = self.cnx.execute(query).fetchone()
            return {"dt": result[0],"rain": result[1]}

        except Exception as e:
            print(e)

    def snowiest_day(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                WITH sums AS (
                    SELECT 
                        DATE(dt,\'unixepoch\') AS dt, 
                        SUM(snow) AS snow 
                    FROM weather 
                    WHERE dt BETWEEN {} AND {}
                    GROUP BY DATE(dt,\'unixepoch\')
                )
                SELECT dt,MAX(snow) snow FROM sums
            '''.format(start_dt, end_dt)

            result = self.cnx.execute(query).fetchone()
            return {"dt": result[0],"snow": result[1]}

        except Exception as e:
            print(e)

    def highest_apparent_temp(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                WITH avgs AS (
                    SELECT 
                        dt,
                        AVG(apparent_temp) as apt
                    FROM weather
                    WHERE dt BETWEEN {} AND {}
                    GROUP BY DATE(dt,\'unixepoch\')
                )
                SELECT DATE(dt,\'unixepoch\'),MAX(apt) from avgs
            '''.format(start_dt, end_dt)

            result = self.cnx.execute(query).fetchone()
            return {"dt": result[0], "temp": result[1]}

        except Exception as e:
            print(e)

    def lowest_apparent_temp(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        try:
            query = '''
                WITH temps AS (
                    SELECT
                        AVG(temp_avg) AS temp_avg, 
                        AVG(humidity) AS humidity, 
                        AVG(wind) AS wind,
                        DATE(dt,\'unixepoch\') AS dt
                    FROM weather
                    WHERE dt BETWEEN {} AND {}
                    GROUP BY DATE(dt, \'unixepoch\')
                ), apts AS (
                    SELECT 
                        APPARENT_TEMPERATURE(temp_avg, humidity, wind) AS apt, 
                        dt 
                    FROM temps
                )
                SELECT dt, MIN(apt) FROM apts
            '''.format(start_dt, end_dt)

            result = self.cnx.execute(query).fetchone()
            return {"dt": result[0],"temp": result[1]}

        except Exception as e:
            print(e)
    
    def average_apparent_temp(self, start_dt=None, end_dt=None):
        start_dt, end_dt = self.__time_range(start_dt, end_dt)
        query = '''
            SELECT AVG(apparent_temp)
            FROM weather
            WHERE dt BETWEEN {} AND {}
        '''.format(start_dt, end_dt)

        result = self.cnx.execute(query).fetchone()
        return {"temp": result[0]}

    def highest_temp_on(self, date):
        query = '''
            SELECT DATE(dt,\'unixepoch\'),MAX(temp_hi)
            FROM weather
            WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\');
        '''.format(date.timestamp())

        result = self.cnx.execute(query).fetchone()
        return {"dt":result[0],"temp":result[1]}
    
    def lowest_temp_on(self, date):
        query = '''
            SELECT DATE(dt,\'unixepoch\'),MIN(temp_hi)
            FROM weather
            WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\');
        '''.format(date.timestamp())

        result = self.cnx.execute(query).fetchone()
        return {"dt":result[0],"temp":result[1]}
    
    def wind_chill_on(self, date):
        query = '''
            SELECT
                DATE(dt,\'unixepoch\'),
                AVG(wind_chill)
            FROM weather
            WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\')
        '''.format(date.timestamp())
        
        result = self.cnx.execute(query).fetchone()
        return {"dt":result[0],"wind_chill":result[1]}
    
    def heat_index_on(self, date):
        query = '''
            SELECT
                DATE(dt,\'unixepoch\'),
                AVG(heat_index)
            FROM weather
            WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\')
        '''.format(date.timestamp())

        result = self.cnx.execute(query).fetchone()
        return {"dt":result[0],"heat_index":result[1]}
    
    def apparent_temp_on(self, date):
        query = '''
            WITH temps AS( 
                SELECT
                    DATE(dt,\'unixepoch\') AS dt,
                    AVG(temp_avg) AS temp_avg,
                    AVG(humidity) AS humidity,
                    AVG(wind) AS wind
                FROM weather
                WHERE DATE(dt,\'unixepoch\')=DATE({},\'unixepoch\')
            )
            SELECT dt,APPARENT_TEMPERATURE(temp_avg,humidity,wind) 
            FROM temps
        '''.format(date.timestamp())

        result = self.cnx.execute(query).fetchone()
        return {"dt":result[0],"temp":result[1]}