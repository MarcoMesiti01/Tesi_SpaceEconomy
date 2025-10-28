import pandas as pd
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flag


inv=mylib.openDB("investors")

inv=flag.spacePercentage(inv, 2020, 0)
inv=inv[["investor_id","space_percentage"]]
print(len(inv))
print(inv["space_percentage"].quantile([0.5, 0.75, 0.8, 0.9, 0.95, 0.99]))