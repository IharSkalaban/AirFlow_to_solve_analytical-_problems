There is a file [vgsales.csv](https://github.com/IharSkalaban/AirFlow_to_solve_analytical-_problems/blob/main/vgsales.csv) -  that contains information on game sales by year.

There is a file [i_skalaban_games_analitycs.py](https://github.com/IharSkalaban/AirFlow_to_solve_analytical-_problems/blob/main/i_skalaban_games_analitycs.py) - a script that performs the tasks

There is a file [README.dag_description.md](https://github.com/IharSkalaban/AirFlow_to_solve_analytical-_problems/blob/main/README.dag_description.md) - DAG's description
### Tasks:

1.  Determine the year for which we will look up the data using the function: 1994 + hash(f'{login}') % 23, where {login} is i-skalaban
2.  Make a DAG of several problems that will result in the answers to the following questions:
- Which game was the best-selling game that year worldwide?
- Which genre of game was the best-selling game in Europe? List all of them if there were more than one.
- Which platform had the most games that sold more than a million copies in North America?
List all of them if there are more than one
 - Which publisher had the highest average sales in Japan?
List all of them if there are more than one
 - How many games sold better in Europe than in Japan?

3. The final task was to **log the answers to each question**, and  **send a notification to Telegram**.


## The developed DAG can be found at [DAG](https://airflow-da.lab.karpov.courses/code?dag_id=i_skalaban_games_analitycs)
#### Some proof of its success 👍 
![1](https://i.ibb.co/7RchqWP/DAG-TREE.jpg)
![2](https://i.ibb.co/m9rp1zw/DAG-Graph.jpg)
![3](https://i.ibb.co/7g0cp5Q/DAG-LOGS.jpg)
![TG notification](https://i.ibb.co/BwH0WnP/Notification.jpg)
