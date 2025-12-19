######
# Замятие 1 раз в час, Низкий с понедельник - пятница с 8:00 по МСК до 19:00 по МСК
######


from connector import connect as cn
import psycopg2
import pandas as pd
import datetime as tm
import schedule
import time

# SQL-запрос (без f-строки, так как нет динамических подстановок)
SQL_SELECT = """
select * from prn_triggers pt where problem_name = 'Host Unavailable' order by dtcreate desc
"""


class DBConnector:
    def __init__(self, server, login, password, db):
        self.server = server
        self.login = login
        self.password = password
        self.db = db

    def connect(self):
        """
        Устанавливает соединение с БД, выполняет SQL-запрос и возвращает
        DataFrame.
        В случае ошибки — логирует её в таблицу log_error.
        """
        try:
            # Устанавливаем соединение
            conn = psycopg2.connect(
                database=self.db,
                user=self.login,
                password=self.password,
                host=self.server,
                port=5432
            )

            # Выполняем запрос и загружаем в DataFrame
            df = pd.read_sql(SQL_SELECT, con=conn)

            # Закрываем соединение
            conn.close()

            return df

        except psycopg2.Error as e:
            event = f"Ошибка БД:\n {e} \n время события: \n {tm.datetime.now()}"
            self.insert_error_log(event)
            return None
        except Exception as e:
            event = f"Неожиданная ошибка:\n {e}\n  и время события:\n {tm.datetime.now()}"
            self.insert_error_log(event)
            return None

    def insert_error_log(self, error_msg):
        """Добавляет запись об ошибке в таблицу log_error.
        Args:
        error_msg (str): Текст ошибки для логирования
        """
        try:
            connection = psycopg2.connect(
                database=self.db,
                user=self.login,
                password=self.password,
                host=self.server,
                port=5432
            )
            cursor = connection.cursor()
            # Курсор для выполнения операций с базой данных select * from ###
            cursor = connection.cursor()
            query = "INSERT INTO log_error (error) VALUES (%s)"
            cursor.execute(query, (error_msg,))
            connection.commit()
            connection.close()
            print(
                f"Зафиксирована ошибка:\n {error_msg}\nЗапись добавлена в БД")

        except psycopg2.Error as e:
            print(f"Не удалось записать лог ошибки в БД: {e}")
        except Exception as e:
            print(f"Критическая ошибка при записи лога ошибки в БД: {e}")


        # Создаём экземпляр коннектора
connct_zbx = DBConnector(
    server=cn.SERVER_ZBX,
    login=cn.LOGIN_ZBX,
    password=cn.PASS_ZBX,
    db=cn.DB_ZBX

)


# Выполняем запрос и печатаем результат
result_df = connct_zbx.connect()


def change_result(df):
    for _, i in df.iterrows():
        print(i)



# шедуллер в  проработку 
####
# schedule.every(1).minutes.do(lambda: change_result(result_df))
# # Запускаем шедулер по расписанию
# while True:
#     schedule.run_pending()
#     time.sleep(1) 