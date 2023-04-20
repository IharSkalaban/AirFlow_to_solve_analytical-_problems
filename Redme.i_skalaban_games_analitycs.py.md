## The DAG's name is i_skalaban_games_analitycs
and it has the following parameters:

* default_args: a dictionary with default arguments for the DAG, including the owner's name, the number of retries, and the retry delay.
* schedule_interval: a string representing the frequency at which the DAG should run. In this case, it is set to 0 12 * * *, meaning the DAG runs at 12:00 UTC every day.
* catchup: a boolean indicating whether the DAG should "catch up" and run all the missed DAG runs when it is turned on. In this case, it is set to False.

#### The DAG has the following tasks:

* **get_year**: A Python function task that takes a login as input and returns a year based on a hash of the login.
* **get_data**: A Python function task that takes a year as input and reads the data from a CSV file stored in a local path. The task filters the data based on the input year.
* **most_popular_game**: A Python function task that takes filtered data as input and returns the name of the most popular game in the world 
* **top_genre_eu**: A Python function task that takes filtered data as input and returns the name of the genre that sold the most in Europe.
* **top_platform_na**: A Python function task that takes filtered data as input and returns the name of the platform that had the most games with over one million copies sold in North America.
* **publisher_high_sales_jp**: A Python function task that takes filtered data as input and returns the name of the publisher with the highest average sales in Japan.
* **eu_jp_games_compare**: A Python function task that takes filtered data as input and returns the number of games that sold better in Europe than in Japan.
* **print_data**: A Python function task that takes all the previous task outputs as input and prints them.

The DAG has a final task called send_message that sends a message via Telegram when the DAG run is successful.
