import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

import telegram
import hashlib

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'i-skalaban'

default_args = {
    'owner': 'i.skalaban',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 16)
}


CHAT_ID = -1001968507364
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Работает успешно ! Dag {dag_id} выполнен {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
    
@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def i_skalaban_games_analitycs():
    @task(retries=3)
    def get_year(login):
        year = 1994 + hash(f'{login}') % 23
        return year
    
    
    @task(retries=3)
    def get_data(year):
        data = pd.read_csv(url)
        filtered_data = data.query('Year == @year')
        return filtered_data
    
    @task(retries=4, retry_delay=timedelta(10))
    def most_popular_game(filtered_data):
        top_world_sales_2012  = filtered_data.groupby('Name',as_index=False)\
                                             .agg({'Rank':'count'})\
                                             .sort_values('Rank', ascending=False)\
                                             .head(1)\
                                             .reset_index().Name.to_list()
        return top_world_sales_2012
    
    @task(retries=4, retry_delay=timedelta(10))
    def top_genre_eu(filtered_data):
        top_eu_genre = filtered_data.groupby('Genre', as_index=False)\
                                    .agg({'EU_Sales': 'sum'})\
                                    .sort_values('EU_Sales', ascending=False)\
                                    .head(1)\
                                    .reset_index().Genre.to_list()
        return top_eu_genre
    
    
    @task(retries=4, retry_delay=timedelta(10))
    def top_platform_na(filtered_data):
        top_na_sales_platform = filtered_data.query('NA_Sales > 1').groupby('Platform')\
                                             .agg({'Name':'nunique'})\
                                             .sort_values('Name', ascending=False)\
                                             .head(1)\
                                             .reset_index().Platform.to_list()
        
        return top_na_sales_platform
    
    @task(retries=4, retry_delay=timedelta(10))
    def publisher_high_sales_jp(filtered_data):
        jp_top_publisher = filtered_data.groupby('Publisher')\
                                        .agg({'JP_Sales':'mean'})\
                                        .sort_values('JP_Sales', ascending=False)\
                                        .head(1)\
                                        .reset_index().Publisher.to_list()
        return jp_top_publisher
    
    @task(retries=4, retry_delay=timedelta(10))
    def eu_jp_games_сompare (filtered_data):
        compare_eu_jp_games = filtered_data.groupby('Name')\
                                           .agg({'EU_Sales':'sum','JP_Sales':'sum'})\
                                           .reset_index().query("EU_Sales > JP_Sales").shape[0]
        return compare_eu_jp_games
    
    
    @task(on_success_callback=send_message)
    def print_data (year, top_world_sales_2012, top_eu_genre, top_na_sales_platform, jp_top_publisher, compare_eu_jp_games):
    
        context = get_current_context()
        date = context['ds']
    
        print(f'Данные игровой индустрии за {year} год: на {date}')
        print(f'Самой продаваемой игрой в мире стала  {", ".join(top_world_sales_2012)} на {date}')
        print(f'В Европе лучше всего продавались игры следующих жанров:  {", ".join(top_eu_genre)} на {date}')
        print(f'В Северной Америке больше всего игр с более чем миллионным тиражом было на платформах:  {", ".join(top_na_sales_platform)} на {date}')
        print(f'Издателями с самыми высокими продажами в Японии стали: {", ".join(jp_top_publisher)} на {date}')
        print(f'{compare_eu_jp_games} игр продалось лучше в Европе, чем в Японии на {date}')
        
    year = get_year(login)
    filtered_data = get_data(year)
    top_world_sales_2012 = most_popular_game(filtered_data)
    top_eu_genre = top_genre_eu(filtered_data)
    top_na_sales_platform = top_platform_na(filtered_data)
    jp_top_publisher = publisher_high_sales_jp(filtered_data)
    compare_eu_jp_games = eu_jp_games_сompare (filtered_data)
    print_data(year, top_world_sales_2012, top_eu_genre, top_na_sales_platform, jp_top_publisher, compare_eu_jp_games)

i_skalaban_games_analitycs = i_skalaban_games_analitycs()







