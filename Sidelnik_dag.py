import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import date, datetime, timedelta

# Дата вчерашнего дня
yesterday = date.today() - timedelta(days=1)

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}


connection_to_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}

# Дефолтные параметры для тасков
default_args = {
    'owner': 'i-sidelnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 12)
}

# Интервал запуска DAG
schedule_interval = '0 10 * * *'  

# Основной DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)  
def sidelnik_6_dag():
    
    # Таск вытаскивает данные из feed_actions
    @task
    def extract_feed():
        query = '''
        SELECT user_id, gender, age, os,
            SUM(action='view') AS views,
            SUM(action='like') AS likes
        FROM simulator_20221020.feed_actions
        WHERE toDate(time) = toDate(today()) - 1
        GROUP BY user_id, gender, age, os
        '''
        
        feed_data = ph.read_clickhouse(query, connection=connection)
        return feed_data
    
    
    # Таск вытаскивает данные из message_actions
    @task
    def extract_message():
        query = '''       
        SELECT
            if(s.user_id = 0, r.reciever_id, s.user_id) AS user_id,
            if(s.user_id = 0, r.gender, s.gender) AS gender,
            if(s.user_id = 0, r.age, s.age) AS age,
            if(s.user_id = 0, r.os, s.os) AS os,
            r.messages_recieved, s.messages_sent, 
            r.users_recieved, s.users_sent
        FROM

        (SELECT user_id, gender, age, os,
            COUNT(*) AS messages_sent,
            COUNT(DISTINCT reciever_id) AS users_sent
        FROM simulator_20221020.message_actions
        WHERE toDate(time) = toDate(today()) - 1
        GROUP BY user_id, gender, age, os) AS s

        FULL OUTER JOIN

        (SELECT reciever_id, gender, age, os,
            COUNT(*) AS messages_recieved,
            COUNT(DISTINCT user_id) AS users_recieved
        FROM simulator_20221020.message_actions
        WHERE toDate(time) = toDate(today()) - 1
        GROUP BY reciever_id, gender, age, os) AS r

        ON s.user_id = r.reciever_id
        '''
        
        feed_data = ph.read_clickhouse(query, connection=connection)
        return feed_data
        
        
    # Таск объединяет загруженные таблицы в одну
    @task
    def transfrom_join(feed_data, message_data):
        df = pd.merge(
            left=feed_data, right=message_data, how='outer', on='user_id', copy=False)

        df['gender'] = df['gender_x'].fillna(df['gender_y'])
        df['gender'] = df['gender'].map({1: 'male', 0: 'female'})
        df['age'] = df['age_x'].fillna(df['age_y'])
        df['os'] = df['os_x'].fillna(df['os_y'])
        df.fillna(0, inplace=True)
        df.drop(['gender_x', 'gender_y', 'age_x',
                 'age_y', 'os_x', 'os_y'], axis=1, inplace=True)

        return df
        
    
    # Таск трансформации в разрезе ОС
    @task
    def transfrom_os(df):
        os_data = df.drop(['user_id', 'gender', 'age'], axis=1)\
            .groupby('os', as_index=False).sum()

        os_data.iloc[:, 1:] = os_data.iloc[:, 1:].astype(int)
        os_data.insert(loc=0, column='event_date', value=yesterday)
        os_data.insert(loc=1, column='dimension', value='os')
        os_data.rename(columns={'os': 'dimension_value'}, inplace=True)
        return os_data
        

    # Таск трансформации в разрезе пола
    @task
    def transfrom_gender(df):
        gender_data = df.drop(['user_id', 'os', 'age'], axis=1)\
            .groupby('gender', as_index=False).sum()

        gender_data.iloc[:, 1:] = gender_data.iloc[:, 1:].astype(int)
        gender_data.insert(loc=0, column='event_date', value=yesterday)
        gender_data.insert(loc=1, column='dimension', value='gender')
        gender_data.rename(columns={'gender': 'dimension_value'}, inplace=True)
        return gender_data
    
    
    # Таск трансформации в разрезе возраста
    @task
    def transfrom_age(df):
        age_data = df.drop(['user_id', 'os', 'gender'], axis=1)\
            .groupby('age', as_index=False).sum()

        age_data = age_data.astype(int)
        age_data.insert(loc=0, column='event_date', value=yesterday)
        age_data.insert(loc=1, column='dimension', value='age')
        age_data.rename(columns={'age': 'dimension_value'}, inplace=True)
        return age_data
    
    
    # Таск загрузки результатов в базу test
    @task
    def load(os_data, gender_data, age_data):
        query = '''
        CREATE TABLE IF NOT EXISTS test.sidelnik_6_dag_table
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_recieved Int64,
        messages_sent Int64,
        users_recieved Int64,
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''
        ph.execute(query, connection=connection_to_test)
        
        result_df = pd.concat(
            [os_data, gender_data, age_data], axis=0)
        
        ph.to_clickhouse(result_df, 'sidelnik_6_dag_table', 
                         connection=connection_to_test, index=False)
        
    
    feed_data = extract_feed()
    message_data = extract_message()
    joined_table = transfrom_join(feed_data, message_data)
    os_data = transfrom_os(joined_table)
    gender_data = transfrom_gender(joined_table)
    age_data = transfrom_age(joined_table)
    load(os_data, gender_data, age_data)

    
sidelnik_6_dag = sidelnik_6_dag()