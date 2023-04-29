from Classes.tasks import Tasks

# --------------------------Menu configuration constants----------------------------------
ITEMS_OF_MENU = (
    "1. Get all titles of series/movies etc. that are available in Ukrainian",
    "2. Get the list of people’s names, who were born in the 19th century",
    "3. Get titles of all movies that last more than 2 hours",
    "4. Get names of people, corresponding movies/series and characters they played in those films",
    ("5. Get information about how many adult movies/series etc. there are per "
     "region. Get the top 100 of them from the region with the biggest count to "
     "the region with the smallest one"),
    ("6. Get information about how many episodes in each TV Series. Get the top"
     "50 of them starting from the TV Series with the biggest quantity of episodes"),
    "7. Get 10 titles of the most popular movies/series etc. by each decade",
    "8. Get 10 titles of the most popular movies/series etc. by each genre",
    "0. Exit"
)
INCORRECT_INPUT_TO_CHOICE = "Incorrect input !!!"
BY_BY = "By-by !!!"
INCORRECT_NUMBER_OF_ITEM = "Incorrect number of item !!!"
WAITING_MESSAGE = "Wait!!! I'm thinking ..."
# --------------------------------------------------------------------------------------------


class Menu:
    def __init__(self):
        self.items = ITEMS_OF_MENU
        self.incorrect_input = INCORRECT_INPUT_TO_CHOICE
        self.by_by = BY_BY
        self.incorrect_item = INCORRECT_NUMBER_OF_ITEM
        self.wait = WAITING_MESSAGE
        print(self.wait)
        self.tasks = Tasks()

    def out_menu(self):
        sel_item = -1
        while sel_item != 0:
            print("---------------------------------Task menu--------------------------------------------")
            for item in self.items:
                print(item, end="\n")
            print("---------------------------------------------------------------------------------------")
            sel_item = input("Choice number of items:")
            if not sel_item.isnumeric():
                sel_item = -1
                print(self.incorrect_input)
            else:
                sel_item = int(sel_item)
                self.switch_item(sel_item)

    def switch_item(self, item):
        print(self.wait)
        if item == 1:
            self.tasks.show_task1()
        elif item == 2:
            self.tasks.show_task2()
        elif item == 3:
            self.tasks.show_task3()
        elif item == 4:
            self.tasks.show_task4()
        elif item == 5:
            self.tasks.show_task5()
        elif item == 6:
            self.tasks.show_task6()
        elif item == 7:
            self.tasks.show_task7()
        elif item == 8:
            self.tasks.show_task8()
        elif item == 0:
            print(self.by_by)
        else:
            print(self.incorrect_item)
