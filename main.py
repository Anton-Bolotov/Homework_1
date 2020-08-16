import os
import sqlite3
import datetime
import time
import vk_api  # pip install vk_api
from settings import login_, password_


class Database:
    """ Class for working with a database """

    def __init__(self, db_name, table_to_create, insert_into, select_from):
        self.db_name = db_name
        self.set = set()
        self.table_to_create = table_to_create
        self.insert_into = insert_into
        self.select_from = select_from
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()

    def db_create_table(self):
        try:
            self.cursor.execute(self.table_to_create)
            self.connection.commit()
        except sqlite3.OperationalError:
            print('The table has already been created!')

    def insert_values_into_db(self, data_to_insert):
        try:
            self.cursor.executemany(self.insert_into, data_to_insert)
            self.connection.commit()
        except sqlite3.IntegrityError:
            print('These IDs have already been used!')

    def db_check_extract(self):
        self.cursor.execute(self.select_from)
        rows = self.cursor.fetchall()
        print(rows)
        for row in rows:
            *_, date = row
            self.set.add(date)


class Vk_group:
    """ class for working with VK API """

    def __init__(self, login, password):
        self.date_now = datetime.datetime.now().strftime('%Y-%m-%d')
        self.count = 0
        self.list = []
        __vk_session = vk_api.VkApi(login=login, password=password)
        __vk_session.auth()
        try:
            self.vk = __vk_session.get_api()
        except vk_api.exceptions.LoginRequired and vk_api.exceptions.BadPassword:
            raise Exception('Please go to the file settings.py and enter a valid username/password')

    def group_info(self, group_list):
        try:
            for group in group_list:
                self.count += 1
                group = str(group).replace('\n', '')
                group_to_check = group.split('/')[-1]
                group_info = self.vk.groups.getById(group_ids=group_to_check, fields='members_count')
                member_count = group_info[0]['members_count']
                data_to_insert = (self.count, group, member_count, self.date_now)
                self.list.append(data_to_insert)
        except vk_api.exceptions.ApiError:
            raise Exception('In the file input.txt there are no links to groups or IDs!')


def create_set_of_groups():
    groups_set = set()
    file_check = os.path.isfile('input.txt')
    if not file_check:
        with open(file='input.txt', mode='w', encoding='utf-8'):
            raise Exception('Please put it in the file input.txt required groups or IDs!')
    elif os.stat('input.txt').st_size == 0:
        raise Exception('Please put it in the file input.txt required groups or IDs!')
    with open(file='input.txt', mode='r', encoding='utf-8') as file:
        for groups in file:
            groups_set.add(groups)
    return groups_set


if __name__ == '__main__':
    vk = Vk_group(login=login_, password=password_)
    db = Database(
        db_name='vk_groups.bd',
        table_to_create='CREATE TABLE Vk_groups (id integer PRIMARY KEY, link text, subscribers integer, date date)',
        insert_into='INSERT INTO Vk_groups VALUES(?, ?, ?, ?)',
        select_from='SELECT * FROM Vk_groups',
    )
    db.db_create_table()

    while True:
        if vk.date_now in db.set:
            print("Today's data is already in the database. Waiting 6 hours for a second check!")
            time.sleep(21600)
        else:
            vk.group_info(group_list=create_set_of_groups())
            db.insert_values_into_db(data_to_insert=vk.list)
            vk.list.clear()
            db.db_check_extract()
