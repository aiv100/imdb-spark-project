from Classes.tasks import Tasks


class Menu:
    def __init__(self):
        self.items = (
            "1. Get all titles of series/movies etc. that are available in Ukrainian",
            "2. Get the list of people’s names, who were born in the 19th century",
            "3. Get titles of all movies that last more than 2 hours",
            "4. Get names of people, corresponding movies/series and characters they played in those films",
            ("5. Get information about how many adult movies/series etc. there are per"
             "region. Get the top 100 of them from the region with the biggest count to"
             "the region with the smallest one"),
            ("6. Get information about how many episodes in each TV Series.Get the top"
             "50 of them starting from the TV Series with the biggest quantity of episodes"),
            "7. Get 10 titles of the most popular movies/series etc. by each decade",
            "8. Get 10 titles of the most popular movies/series etc. by each genre",
            "0. Exit"
        )

    def out_menu(self):
        sel_item = -1
        while sel_item != 0 :
            print("---------------------------Task menu------------------------------------")
            for item in self.items:
                print(item, end="\n")
            print("------------------------------------------------------------------------")
            sel_item = input("Choice item:")
            if not sel_item.isnumeric():
                sel_item = -1
                print("Incorrect input!!!")
            else:
                sel_item=int(sel_item)
                self.switch_item(sel_item)

    def switch_item(self, item):
        tasks = Tasks()
        if item == 1:
            tasks.show_task1()
        elif item == 2:
            tasks.show_task2()
        elif item == 3:
            tasks.show_task3()
        elif item == 4:
            tasks.show_task4()
        elif item == 5:
            tasks.show_task5()
        elif item == 6:
            print(6)
        elif item == 7:
            print(7)
        elif item == 8:
            print(8)
        elif item == 0:
            print("By-by!!!")
        else:
            print("Incorrect number of item")