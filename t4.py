import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import numpy as np


engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:3306/{db}"
            .format(host="34.163.182.248", db='academy', user='root', pw='Ciocanul12%40'))

mydb = mysql.connector.connect(
  host="34.163.182.248",
  user="root",
  password="Ciocanul12@",
  database="academy"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM nba_salary")

myresult = mycursor.fetchall()
mycursor.execute("SELECT * FROM nba_savings")
myresult2 = mycursor.fetchall()

query_salary="SELECT * FROM nba_salary"
query_savings="SELECT * FROM nba_savings"
query_players="SELECT * FROM nba_players"

df_salary=pd.read_sql(query_salary,mydb)
# print(df_salary.to_string())
df_savings=pd.read_sql(query_savings,mydb)

df_players=pd.read_sql(query_players,mydb)
# print(df_savings.to_string())
sal_sav=pd.merge(df_salary,df_savings)
sal_sav.rename(columns={'id':'Id'},inplace=True)
# print(df_players.to_string())
df_players.rename(columns={'index':'Id'},inplace=True)

finaldf=pd.merge(sal_sav,df_players,on="Id")
print(finaldf.to_string())

#mycursor.execute("CREATE OR REPLACE TABLE city_average_niteanu (city VARCHAR(255), average_salary int, number_of_players int)")

list=finaldf['team.city'].unique()
list_salary=finaldf.groupby(['team.city'])['Salary'].mean()
list_salary2=finaldf.groupby(['team.city'])['Id'].count().reset_index()

ex1=pd.merge(list_salary,list_salary2,on='team.city')

ex1.to_sql('city_average_niteanu',con=engine,if_exists='replace',index = False)

#ex2
avg=finaldf['Salary'].mean()
finaldf['FULL']=finaldf["first_name"]+' '+finaldf["last_name"]

finaldf['Salary_avg']=np.where(finaldf['Salary']>avg,1,0)
ex2=finaldf[['Salary_avg','FULL']].copy()
ex2.to_sql('player_average_niteanu',con=engine,if_exists='replace',index = False)

#tried ex3
print(finaldf.where(finaldf['team.city']=='LA').sort_values(by=['Salary']))

