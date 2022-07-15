import pandas as pd
import matplotlib.pyplot as plt
import os



CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
harmonized_path = CURR_DIR_PATH + "/harmonized/"

harmonized_file = (harmonized_path + 'harmonized_data.json')
data = pd.read_json(harmonized_file)
#plt.plot (data['date'], data['temperature'], color="orange") #['temp']
#data['precipitation'] = data['precipitation'] * 100
#plt.plot(data['date'], data['precipitation'], color="red")
plt.plot(data['temperature'], data['precipitation'], color="fuchsia")
#plt.plot(data['date'])

# Adding Title to the Plot
plt.title("😅😅😅😅", fontsize = 30)

# Setting the X and Y labels
plt.xlabel('?!?!?!')
plt.ylabel('?!?!?!', rotation=0)

plt.show()