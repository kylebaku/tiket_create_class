######
# Замятие 1 раз в час, Низкий с понедельник - пятница с 8:00 по МСК до 19:00 по МСК
######


from connector import connect as cn
import psycopg2
import pandas as pd
import datetime as tm
import schedule
import time
import pymysql
from ke_list import ke_lists, branch
import re
from create_tt import add

# SQL-запрос (без f-строки, так как нет динамических подстановок)
SQL_SELECT = """
select * from prn_triggers pt where problem_name = 'Host Unavailable' and contact like ('%A%')order by dtcreate desc
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

def query_incert_tt(prob_name, host, model, sn, ip, contact, remedy_tt, dat):
    connection = psycopg2.connect(user=cn.LOGIN_ZBX, password=cn.PASS_ZBX, host=cn.SERVER_ZBX, database=cn.DB_ZBX, port="5432")
    cursor = connection.cursor()
    # Курсор для выполнения операций с базой данныхselect * from checklist_ticket
    query = f"""INSERT INTO print_tt (hostname,contact,problem_name,serial_n,model,ip_addr,remedy_tt,dtcreate)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    # WHERE   request_id = % s and problem = % s
    cursor.execute(query, (host, contact, prob_name, sn, model, ip, remedy_tt, dat))
    connection.commit()
    connection.close()


# Выполняем запрос и печатаем результат
result_df = connct_zbx.connect()

class DBReport:
    """
    Класс обработки датафремйма данными с данными по тригерами принтеров
    и формирования ТТ
    """

    def __init__(self, result_df):
        self.df = result_df

    def read_df(self):
        """Метод построчной обработки датафрейма"""
        dtt = self.df
        rows = int(dtt.shape[0])
        # присваем кол-во записей в датафрейме
        for number in range(rows):
            dat = dtt.iloc[number][0]  # дата запроса выгрузки тригера
            prob_name = dtt.iloc[number][1]  # наименование тригера
            host = dtt.iloc[number][2]  # имя принтера
            model = dtt.iloc[number][3]  # модель принтера
            sn = dtt.iloc[number][4]  # sn принтера
            ip = dtt.iloc[number][5]  # ip адрес принтера
            contact = dtt.iloc[number][6]  # зашифрованный адрес устройства
            # сокращенный формат филиала "NN"
            fil = re.split("_|\.", contact)[1]
            adm = re.split("_|\.", contact)[2]  # код точки офиса
            try:
                result = ke_lists(adm)
                if len(result) != 4:
                    print(f"Ошибка: ke_lists вернула {len(result)} значений для adm={adm}")
                    continue
                cod, adr, fili, rol = result
                if 0 in (cod, adr, fili, rol):
                    continue
                br, rl = branch(fili, rol)

           
                print(f'data: {dat}\n'
                  f'prob_name: {prob_name}\n'
                  f'host: {host}\n'
                  f'model: {model}\n'
                  f'sn: {sn}\n'
                  f'ip: {ip}\n'
                  f'contact: {contact}\n'
                  f'fil: {fil}\n'
                  f'adm: {adm}\n'
                  f'br: {br}\n'
                  f'rl: {rl}\n'
                  f'cod: {cod}\n'
                  f'rol: {rol}\n'
                  f'adr: {adr}\n')
                remedy_tt = str(add(prob_name, host, model, sn, ip, contact, adr, br, rl))
                # Формируем ТТ
                query_incert_tt(prob_name,host, model,sn,ip,contact,remedy_tt,dat)
                # Записываем данные в таблицу по номеру ТТ
                print(remedy_tt)
            except Exception as e:
                print(f"Ошибка при обработке adm={adm}: {e}")
                continue
f = DBReport(result_df)
print(f.read_df())


# шедуллер в  проработку 
####
# schedule.every(1).minutes.do(lambda: change_result(result_df))
# # Запускаем шедулер по расписанию
# while True:
#     schedule.run_pending()
#     time.sleep(1)    
