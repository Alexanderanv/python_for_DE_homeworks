from multiprocessing import Process, Pipe, Manager
from datetime import datetime as dt

def calculate_formula_1(conn, end:int, values:list = []):
    start_time = dt.now()

    for i in range(0, end):
        x = i + 1
        values.append(x^2-x^2+x*4-x*5+x+x)

    end_time = dt.now()
    duration = end_time - start_time
    conn.send(f'Этап 1 количество: {end}, длительность: {duration}')
    conn.close()

def calculate_formula_2(conn, end:int, values:list = []):
    start_time = dt.now()

    for i in range(0, end):
        x = i + 1
        values.append(x+x)

    end_time = dt.now()
    duration = end_time - start_time
    conn.send(f'Этап 2 количество: {end}, длительность: {duration}')
    conn.close()

def sum_values(end:int, values1:list, values2:list):
    start_time = dt.now()

    for i in range(0, end):
        x = values1[i] + values2[i]

    end_time = dt.now()
    duration = end_time - start_time
    print(f'Этап 3 количество: {end}, длительность: {duration}')

if __name__ == '__main__':

    def calculate(count:int):

        with Manager() as manager:
            values1 = manager.list()
            values2 = manager.list()
        
            parent_conn_1, child_conn_1 = Pipe()
            parent_conn_2, child_conn_2 = Pipe()

            p1 = Process(target=calculate_formula_1, kwargs={'conn':child_conn_1, 'end': count, 'values': values1})
            p1.start()

            p2 = Process(target=calculate_formula_2, kwargs={'conn':child_conn_2, 'end': count, 'values': values2})
            p2.start()

            p1.join()
            print(parent_conn_1.recv())
            
            p2.join()
            print(parent_conn_2.recv())    

            sum_values(count, values1, values2)


    count = 10000
    calculate(count)

    count = 100000
    calculate(count)




