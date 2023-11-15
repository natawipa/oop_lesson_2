import csv, os
import combination_gen

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None

import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table

    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def __is_float(self, element):
        if element is None: 
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)
    
    def pivot_table(self, keys_to_pivot_list, keys_to_aggreagte_list, aggregate_func_list):
        unique_values_list = []
        pivot_table = []
        for key in keys_to_pivot_list:
            my_table_selected = self.select([key])
            temp_list = []
            for value in my_table_selected:
                if value not in temp_list:
                    temp_list.append(value)
            unique_values_list.append(temp_list)
        combination_list = combination_gen.gen_comb_list(unique_values_list)
        for value in combination_list:
            aggregate_value = []
            temp = copy.copy(self)
            for i in range(len(value)):
                temp = temp.filter(lambda x: x[keys_to_pivot_list[i]] == value[i])
            for j in range(len(keys_to_aggreagte_list)):
                value = temp.aggregate(aggregate_func_list[j], keys_to_aggreagte_list[j])
                aggregate_value.append(value)
                pivot_table.append([value, aggregate_value])
        return pivot_table

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')
my_table2 = my_DB.search('countries')
my_table3 = my_DB.search('players')
my_table4 = my_DB.search('teams')
my_table5 = my_DB.search('titanic')


print("Test filter: only filtering out cities in Italy")
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()

print("What player on a team with “ia” in the team name played less than 200 minutes and made more than 100 passes?")
team_filtered = table3.filter(lambda x: 'ia' in x['team']).filter(lambda x: int(x['minutes']) < 200).filter(lambda x: int(x['passes']) > 100)
team_selected = team_filtered.select(['surname', 'team', 'position'])
print(team_selected)

print('The average number of games played for teams ranking below 10 versus teams ranking above or equal 10')
ranking_below_10 = my_table4.filter(lambda x: int(x['ranking']) < 10)
ranking_above_10 = my_table4.filter(lambda x: int(x['ranking']) >= 10)
average_games_played_below_10 = ranking_below_10.aggregate(lambda x: sum(x) / len(x), 'games')
average_games_played_above_10 = ranking_above_10.aggregate(lambda x: sum(x) / len(x), 'games')
print("below 10:", average_games_played_below_10)
print("above or equal to 10:", average_games_played_above_10)

print("The average number of passes made by forwards versus by midfielders")
forwards = table3.filter(lambda x: x['position'] == 'forward')
midfielders = table3.filter(lambda x: x['position'] == 'midfielder')
average_passes_forwards = forwards.aggregate(lambda x: sum(x) / len(x), 'passes')
average_passes_midfielders = midfielders.aggregate(lambda x: sum(x) / len(x), 'passes')
print("forwards:", average_passes_forwards)
print("midfielders:", average_passes_midfielders)

print("The average fare paid by passengers in the first class versus in the third class")
first_class = my_table5.filter(lambda x: int(x['class']) == 1)
third_class = my_table5.filter(lambda x: int(x['class']) == 3)
average_first_class = first_class.aggregate(lambda x: sum(x) / len(x), 'fare')
average_third_class = third_class.aggregate(lambda x: sum(x) / len(x), 'fare')
print("first class:", average_first_class)
print("third class:", average_third_class)

print("The survival rate of male versus female passengers")
male = my_table5.filter(lambda x: str(x['gender']) == 'M')
female = my_table5.filter(lambda x: str(x['gender']) == 'F')

all_female = []
survived_female = []
for item in female.table:
    all_female.append(item['gender'])
    if item['survived'] == 'yes':
        survived_female.append(item['survived'])

all_male = []
survived_male = []
for item in male.table:
    all_male.append(item['gender'])
    if item['survived'] == 'yes':
        survived_male.append(item['survived'])

print("Female survival rate", (len(survived_female) / len(all_female) * 100))
print("Male survival rate", (len(survived_male) / len(all_male) * 100))
print()
