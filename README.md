## The project "imdb-spark-project" consists of three Python files:
### _main.py_ – to start the project
### _menu.py_ (Classes folder) – the implementation of the menu that selects the desired task of the project
### _tasks.py_ (Classes folder) – contains properties (attributes) and methods for implementing each project task
## The project also contains two folders:
### _Data_ - here are the source tsv - files that are used to implement project tasks
### _Results_ – result files created in the corresponding project tasks are stored here
### If the project download starts with the main.py file, a menu appears in the console
---------------------------------Task menu--------------------------------------------
1. Get all titles of series/movies etc. that are available in Ukrainian
2. Get the list of people’s names, who were born in the 19th century
3. Get titles of all movies that last more than 2 hours
4. Get names of people, corresponding movies/series and characters they played in those films
5. Get information about how many adult movies/series etc. there are per region. Get the top 100 of them from the region with the biggest count to the region with the smallest one
6. Get information about how many episodes in each TV Series. Get the top50 of them starting from the TV Series with the biggest quantity of episodes
7. Get 10 titles of the most popular movies/series etc. by each decade
8. Get 10 titles of the most popular movies/series etc. by each genre
9. Exit
---------------------------------------------------------------------------------------
Choice number of items:
### from which you need to select the number of the corresponding project task.
### But there is a possibility to launch the project from the tasks.py file, which is protected from "accidental" execution by the conditional statement
### if __name__ == __main__
### In which you need to uncomment the line of the corresponding task. Perhaps this launch path will be more optimal for testing.
