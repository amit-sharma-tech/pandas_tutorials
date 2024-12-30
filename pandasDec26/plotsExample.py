import pandas as pd
import matplotlib.pyplot as plt
# import faulthandler

# faulthandler.enable()

air_quality = pd.read_csv("./data/air_quality_no2.csv",index_col=0,parse_dates=True)

print(air_quality.head())

# print(air_quality.plot())
# print(plt.show())

# air_quality["station_paris"].plot()
# print(plt.show())

##---I want to visually compare the  values measured in London versus Paris.

# air_quality.plot.scatter(x="station_london",y="station_paris", alpha=0.5)
# print(plt.show())

##another way to plot graph

# print([
#     method_name
#     for method_name in dir(air_quality.plot)
#     if not method_name.startswith("_")
# ])

##box plot example

# air_quality.plot.box()

# print(plt.show())

#---I want each of the columns in a separate subplot.

# axs = air_quality.plot.area(figsize=(12,4),subplots=True)
# print(plt.show())

###----I want to further customize, extend or save the resulting plot.

fig, axs = plt.subplots(figsize=(12, 4))

air_quality.plot.area(ax=axs)

axs.set_ylabel("NO$_2$ concentration")

fig.savefig("no2_concentrations.png")

print(plt.show())