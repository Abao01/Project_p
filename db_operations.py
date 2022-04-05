import sqlite3
from dbcm import DBCM
from scrape_weather import WeatherScraper

class DBOperations:
  def initialize_db(self):
    with DBCM("weather.sqlite") as cur:
      try:
        cur.execute("""create table if not exists weatherdata
                        ( id integer primary key autoincrement not null,
                          sample_date text not null,
                          location text not null,
                          max_temp real not null,
                          min_temp real not null,
                          avg_temp real not null);""")
        print("Table created successfully.")
      except Exception as e:
            print("Error creating table:", e)

  def save_date(self,dictionary):
      try:
        sql = """insert into weatherdata (sample_date,location,max_temp,min_temp,avg_temp)
                values (?,?,?,?,?)"""
        try:
          for dates,datas in dictionary.items():
            data = []
            data.append(dates)
            data.append("Winnipeg, MB")
            for key, value in datas.items():
                data.append(value)
            with DBCM("weather.sqlite") as cur:
              cur.execute(sql, data)
          print("Added weatherdata successfully.")
        except Exception as e:
          print("Error: ",e)
      except Exception as e:
        print("Error inserting weatherdata.", e)

  def purge_data(self):
    with DBCM("weather.sqlite") as cur:
      sql= """delete from  weatherdata"""
      for row in cur.execute(sql):
        print(row)
      print(" Purge all the data from the DB. ")


weather = WeatherScraper()
weather.fetch_weather_data()

db = DBOperations()
db.initialize_db()
db.save_date(weather.weather)


