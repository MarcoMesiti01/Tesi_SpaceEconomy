import pandas as pd
import requests
import json
import openpyxl 
import pyarrow
import traceback
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import pycountry
from pathlib import Path
from typing import Optional, Literal

def isfloat(value):#True if is float
    try:
        float(value)
        return True
    except ValueError:
        return False

def investorInfo(df): #Returns a df of the columns: Investor, Investor type
    rows=[]
    listInvestor= list(df["Investors names"])
    listInvestorType=list(df["Each investor type"])
    
    for i in range(len(listInvestor)):
            try:
                if not pd.isna(listInvestor[i]) and ";" in listInvestor[i]:
                    listInvestorSplit=listInvestor[i].split(";")
                    listInvestorTypeSplit=listInvestorType[i].split(";")
                    for a in range(len(listInvestorSplit)):
                        if "," in listInvestorTypeSplit[a]:
                            listInvestorTypeSplitSplit=listInvestorTypeSplit[a].split(",")
                            for t in listInvestorTypeSplitSplit:
                                rows.append({"Investor" : listInvestorSplit[a], "investor_types" : t})
                        else:
                            add={"Investor" : listInvestorSplit[a], "investor_types" : listInvestorTypeSplit[a]}
                            rows.append(add)
                else:
                    if not pd.isna(listInvestor[i]) and "," in listInvestorType[i]:
                            listInvestorTypeSplit=listInvestorType[i].split(",")
                            for t in listInvestorTypeSplit:
                                rows.append({"Investor" : listInvestor[i], "investor_types" : t})
                    else:
                        add={"Investor" : listInvestor[i], "investor_types" : listInvestorType[i]}
                        rows.append(add)
            except(TypeError) as e: 
                traceback.print_exc()
                continue
    
    return_df=pd.DataFrame(rows)
    return_df.drop_duplicates(inplace=True)
    return return_df
        
def roundSplit(listAdd, df):#Divides each round made in each rows, the columns are: "Investor", "Amount", "Currency", "Amount in EUR", "Round type", "Round date"
    j=0
    listAdd=list()
    for i in range(len(df)): 
        try: 
            if "space" in df.loc[i, "Tags"]:
                investorListi=df.loc[i, "Each round investors"].split(";")
                amountListi=df.loc[i, "Each round amount"].split(";")
                currencyListi=df.loc[i, "Each round currency"].split(";")
                roundTypeListi=df.loc[i, "Each round type"].split(";")
                roundDateListi=df.loc[i, "Each round date"].split(";")
                firmTarget=df.loc[i, "Name"]
                firmTarget="" if pd.isna(firmTarget) else firmTarget
                firmId=df.loc[i, "ID"]
                firmId="" if pd.isna(firmId) else firmId
                firmTargetCountry=df.loc[i, "HQ country"]
                firmTargetCountry="" if pd.isna(firmTargetCountry) else firmTargetCountry
                for a in range(len(investorListi)):
                    roundType=roundTypeListi[a] 
                    roundDate=convertToDatetime(roundDateListi[a])
                    amountListi[a]=0 if pd.isna(amountListi[a]) or amountListi[a]=="n/a" else float(amountListi[a])
                    if "+" in investorListi[a]:
                        investorsA=investorListi[a].split("++")
                        if amountListi[a]!="n/a":
                            valuePlus=float(amountListi[a])/len(investorsA)
                        else:
                            valuePlus=0
                        for x in investorsA: 
                            listAdd.append([x, valuePlus, currencyListi[a], 0, roundType, roundDate, firmTarget, firmId, firmTargetCountry])
                    else:
                        listAdd.append([investorListi[a], amountListi[a], currencyListi[a], 0, roundType, roundDate, firmTarget, firmId, firmTargetCountry])
        except Exception as e: 
            inv=df.loc[i, "Each round investors"]
            amount=df.loc[i, "Each round amount"]
            amount=0 if pd.isna(amount) or amount=="n/a" else float(amount)
            firmTarget=df.loc[i, "Name"]
            firmTarget="" if pd.isna(firmTarget) else firmTarget
            firmId=df.loc[i, "ID"]
            firmId="" if pd.isna(firmId) else firmId
            firmTargetCountry=df.loc[i, "HQ country"]
            firmTargetCountry="" if pd.isna(firmTargetCountry) else firmTargetCountry
            if pd.isna(inv):
                continue
            elif not pd.isna(inv) and ";" not in inv and not pd.isna(amount):
                listAdd.append([inv, amount, "EUR", 0, df.loc[i, "Each round type"], convertToDatetime(df.loc[i, "Each round date"]), firmTarget, firmId, firmTargetCountry])
            elif pd.isna(inv) and pd.isna(amount):
                continue
            elif not pd.isna(inv) and pd.isna(amount):
                continue
            else:
                traceback.print_exc() 
                j=j+1
            continue
    final_df=pd.DataFrame(listAdd, columns=["Investor", "Amount", "Currency", "Amount in EUR", "Round type", "Round date", "Target firm", "company_id","company_country"])
    final_df.astype({"Investor":"string", "Amount":"float", "Currency":"string", "Amount in EUR":"float", "Round type":"string", "Round date":"string", "Target firm":"string", "company_id" : "int", "company_country":"string"})
    print(j)
    return final_df

def operConv(row, conversion_dict):
    try: 
        retVal=float(row["Amount"])/float(conversion_dict.get(row["Currency"], 0))
        return retVal
    except:
        return 0

def amountsConv(target_df, conversion_dict):# converts the amount from the currency to euros and cuts the columns linked to "Amount" and "Currency"
    i=0
    try:
        #target_df.loc[i, "Amount in EUR"]=float(target_df.loc[i, "Amount"])/float(conversion_dict.get(target_df.loc[i, "Currency"], 0))
        #target_df["Amount in EUR"]=target_df.apply(lambda row: float(row["Amount"])/float(conversion_dict.get(row["Currency"], 0)), axis=1)
        target_df["Amount in EUR"]=target_df.apply(operConv, conversion_dict=conversion_dict, axis=1)
    except Exception as e:
        traceback.print_exc()
    return target_df


def invSplit(df) -> pd.DataFrame:#return a dataframe with columns: Investor, Type, Firm. With type referring to Type of the investor. 
    listAdd=list()
    try:
        for i in range(len(df)):
            firm=df.loc[i, "Name"]
            invs=df.loc[i, "Investors names"]
            types=df.loc[i, "Each investor type"]
            if not isfloat(invs) and ";" in invs:
                listInv=invs.split(";")
                listType=types.split(";")
                for a in range(len(listInv)):
                    invA=listInv[a]
                    typeA=listType[a]
                    if "," in typeA:
                        listTypeInvInv=typeA.split(",")
                        for a in listTypeInvInv:
                            listAdd.append([invA, a, firm])
                    else:
                        listAdd.append([invA, typeA, firm])
            elif not isfloat(invs) and not ";" in invs:
                if "," in types: 
                    listTypeInvInv=types.split(",")
                    for a in listTypeInvInv:
                        listAdd.append([invs, a, firm])
                else:
                    listAdd.append([invs, types, firm])
            else:
                continue
    except Exception:
        traceback.print_exc()
    return_df=pd.DataFrame(listAdd, columns=["Investor", "Type" , "Firm"])
    return return_df

def splitTag(df) -> pd.DataFrame:#returns a dataframe with columns: Tag, Funding. With funding calculated as the sum of all the fundings in companies having the tag used as index. 
    dictT=dict()
    listAdd=list()
    for i in range(len(df)):
        try:
            funding=float(df.loc[i, "Total funding (EUR M)"])
            if pd.isna(funding):
                funding=0
            tags=df.loc[i, "Tags"]
            if not isfloat(tags) and "," in tags:
                listTag=tags.split(",")
                for t in listTag:
                    try:
                        dict[t]+=funding
                    except(KeyError, TypeError):
                        dictT[t]=funding
            elif not isfloat(tags):
                try:
                    dictT[tags]+=funding 
                except:
                    dictT[tags]=funding
            else:
                raise ValueError("tag is null")
        except:
            traceback.print_exc()
    tot=0
    for x in dictT.values():
        tot+=x
    for x, y in dictT.items():
        listAdd.append([x,y,(y/tot)])
    return_df=pd.DataFrame(listAdd, columns=["Tag", "Funding", "Percentage of TOT"])
    return return_df

def aggregateIndustries(df) -> pd.DataFrame: 
    dictR=dict()
    listAdd=list()
    for i in range(len(df)):
        try:
            industry=str(df.loc[i, "Industries"])
            amount=float(df.loc[i, "Total funding (EUR M)"])
            if pd.isna(amount):
                amount=0
            if not isfloat(industry) and ";" in industry:
                industryI=industry.split(";")
                for a in industryI:
                    try:
                        dictR[a]+=amount
                    except(KeyError, TypeError):
                        dictR[a]=amount
            elif not isfloat(industry):
                try:
                    dictR[industry]+=amount
                except(KeyError, TypeError):
                    dictR[industry]=amount
            else:
                raise ValueError("Industry is blank")
        except:
            traceback.print_exc()
    total=df["Total funding (EUR M)"].sum()
    for x,y in dictR.items():
        listAdd.append([x, y, y/total])
    return_df=pd.DataFrame(listAdd, columns=["Industry", "Total funding", "Percentage of TOT"])
    return return_df

def countryID(row) -> str:
    address=row["Firm address"]
    address="NA" if pd.isna(address) else address
    addressList=address.split(",")
    country=addressList[len(addressList)-1]
    return country

def to_iso3(name:str)->str:
    try:
        return pycountry.countries.lookup(name).alpha_3
    except LookupError:
        if name=="Russia":
            return "RUS"
        if name == "Kosovo":
            return "XKX"
        return None

def makeMap(df: pd.DataFrame, column: str) -> px.choropleth:
    df[countrColumn]=df["company_country"].apply(to_iso3)
    listColumns=[countrColumn, "company_country", column]
    df=df[listColumns]
    missing=df[df[countrColumn].isna()]
    if not missing.empty:
        print(missing)
    fig=px.choropleth(df, locations=countrColumn, color=column, hover_name="company_country", color_continuous_scale="Reds", projection="natural earth")
    fig.update_layout(title=column, coloraxis_colorbar_title="Value", margin=dict(l=0, r=0, t=40, b=0),)
    for i, row in df.iterrows():
        fig.add_trace(go.Scattergeo(locationmode="country names", locations=[row["company_country"]], text=[round(row[column])], mode="text", showlegend=False ))
    fig.show() 
    return fig

def findLocation(investor : str):
    if len(investor)==0:
        return {"Investor" : "missing"}
    API_KEY="API_Key"
    url="https://places.googleapis.com/v1/places:searchText"

    headers = {
        "Content-Type" : "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id" 
    }

    body= {
        "textQuery" : str(investor)+" headquarter",
        "languageCode":"en"
    }

    r = requests.post(url, headers=headers, json=body, timeout=30)
    dict_r=r.json()
    print(dict_r)
    ret=list()
    try:
        place_id_list=dict_r.get("places", "")
        for place in place_id_list:
            place_id=place.get("id", "")
            print(place_id)
            if place_id!="":
                url="https://places.googleapis.com/v1/places/"+place_id
                headers = {
                "X-Goog-Api-Key": API_KEY,
                "X-Goog-FieldMask": "id,addressComponents"
                }
                r=requests.get(url, headers=headers, timeout=30)
                retI=dict()
                retI["Investor"]= investor
                dict_r=r.json()
                print(dict_r)
                for component in dict_r.get("addressComponents"):
                    retI[component.get("types", "")[0]] = component.get("longText", "")
                ret.append(retI)
        if len(ret)==0:
            return {"Investor" : investor}
        else:
            return ret
            
    except:
        traceback.print_exc()
        return {"Investor" : investor}
    
def polish_loc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a dataframe with columns Investor and country 
    Returns a dataframe that keeps only the information of the country appearing the most linked to the investor
    Returns df -> columns=["Investor", countrColumn]
    """
    rows=list()
    for investor, group in df.groupby("Investor"):
        occurences=dict()
        for index, row in group.iterrows():
            try:
                occurences[row[countrColumn]]+=1
            except KeyError:
                occurences[row[countrColumn]]=1
        maxValue=0
        country=str()
        for key, values in occurences.items():
            if values>maxValue:
                maxValue=values
                country=key
        rows.append([investor, country])
    retDf=pd.DataFrame(rows, columns=["Investor", countrColumn])
    return retDf

def valuations(df: pd.DataFrame):
    listAdd=list()
    df[["Historical valuations - dates", "Historical valuations - values (EUR M)"]]=df[["Historical valuations - dates", "Historical valuations - values (EUR M)"]].apply(stringToList, axis=1)
    #df[]=df["Historical valuations - values (EUR M)"].apply(stringToList, by_row="compat")
    #print(df[["Historical valuations - dates", "Historical valuations - values (EUR M)"]][:100])
    try:
        df=df.explode(column=["Historical valuations - dates", "Historical valuations - values (EUR M)"], ignore_index=True)
        df["Historical valuations - dates"]=df["Historical valuations - dates"].apply(convertToDatetime, by_row="compat")
        df["Historical valuations - values (EUR M)"]=df["Historical valuations - values (EUR M)"].apply(avgValuation, by_row="compat")
    except Exception as e:
        """df["Historical valuations - dates"]=df["Historical valuations - dates"].apply(len, by_row="compat")
        df["Historical valuations - values (EUR M)"]=df["Historical valuations - values (EUR M)"].apply(len, by_row="compat")"""
        print(df[df["Historical valuations - dates"] != df["Historical valuations - values (EUR M)"]])
        traceback.print_exc()
        
    return df

def stringToList(entry) -> list:
    stringDates=entry.iloc[0]
    stringValues=entry.iloc[1]
    listDates=splitString(stringDates)
    listValues=splitString(stringValues)
    if len(listDates) > len(listValues):
        for i in range(len(listDates)-len(listValues)):
            listValues.append(0)
    elif len(listValues)>len(listDates):
        for i in range(len(listValues)-len(listDates)-1):
            listDates.append("1970")
    entry.iloc[0]=listDates
    entry.iloc[1]=listValues
    return entry


def splitString(entry):
    if not pd.isna(entry) and "," in entry:
        listItem=entry.split(",")
        return listItem
    elif not pd.isna(entry) and "," not in entry:
        return [entry]
    elif pd.isna(entry):
        return []
    else:
        print(entry)
        return []

    

def convertToDatetime(date : str) -> pd.Timestamp:
    if pd.isna(date):
        return pd.to_datetime("1970", format="%Y")
    if "/" in date:
        monthList=["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
        for i in range(len(monthList)):
            if monthList[i] in date:
                date=date.replace(monthList[i], str(i+1))
                return pd.to_datetime(date, format="%m/%Y")
    elif "-" in date:
        date=date.replace("-", "/")
        return pd.to_datetime(str(date), format="%m/%Y")
    else: 
        return pd.to_datetime(str(date), format="%Y")


def avgValuation(val) -> float:
    if pd.isna(val):
        return 0
    elif not pd.isna(val) and (isinstance(val, int) or isinstance(val, float)):
        return val
    elif  not pd.isna(val) and "-" in val:
        listVal=val.split("-")
        valFin=(float(listVal[0])+float(listVal[1]))/2
        return valFin

def getYear(date : pd.Timestamp) -> int:
    return date.year

def filterExits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts a dataframe that has the column "Round type" and filter the rows that are exits
    """
    if "Round type" not in df.columns:
        return df
    else:
        listExits=["BUYOUT", "ACQUISITION", "POST IPO EQUITY", "POST IPO CONVERTIBLE", "POST IPO DEBT", "POST IPO SECONDARY", "SPAC IPO", "SPAC PRIVATE PLACEMENT", "IPO"]
        df["Round type"]=df["Round type"].mask(df["Round type"].isin(listExits), other="NULL")
        df=df[df["Round type"]!="NULL"]
        return df

def toEU(df : pd.DataFrame, countrColumn: str) -> pd.DataFrame:
    """
    Accepts a dataframe with the column countrColumn and rename the EU countries as "EU"
    Returns a dataframe 
    """
    EU=["Italy", "Germany", "France", "Spain", "Portugal", "Greece", "Netherlands", "Belgium", "Luxemburg", "Finland", "Sweden", "Austria", "Denmark", "Bulgaria", "Romania", "Estonia", "Latvia", "Poland", "Lithuania", "Ireland", "Malta", "Croatia", "Czech republic", "Hungary", "Cyprus", "Liechtenstein", "Slovenia", "Slovakia"]
    df[countrColumn]=df[countrColumn].mask(df[countrColumn].isin(EU), other="EU")
    return df

def toEurope(df: pd.DataFrame, countrColumn: str):
    """
    Accepts a dataframe with the column `countrColumn` and renames European countries to "Europe".
    Matching is case-insensitive and trims whitespace. Handles common spelling variants.
    Returns the modified dataframe.
    """
    europe_set = {
        "italy", "germany", "france", "spain", "portugal", "greece",
        "netherlands", "belgium", "luxembourg", "luxemburg", "finland",
        "sweden", "austria", "denmark", "bulgaria", "romania", "estonia",
        "latvia", "poland", "lithuania", "ireland", "malta", "croatia",
        "czech republic", "czechia", "hungary", "cyprus", "liechtenstein",
        "slovenia", "slovakia", "united kingdom", "uk", "norway", "switzerland",
        "iceland",
    }

    series = df[countrColumn].astype(str)
    norm = series.str.strip().str.casefold()
    mask = norm.isin(europe_set)
    df.loc[mask, countrColumn] = "Europe"
    return df

def space(df: pd.DataFrame, column : str, filter : bool) -> pd.DataFrame:
    """Accepts a dataframe with the firm Id in the 'column', adds the flag space based on the Table, returns the dataframe with the flag if filter is 0, returns the dataframe filtered if filter is 1"""
    df_space=openDB("updown")
    df_space=df_space["Space"]
    df_fin=pd.merge(left=df, right=df_space, left_on=column, right_index=True, how="left")
    df_fin.fillna({"Space":0}, inplace=True)
    if filter:
        return df_fin[df_fin["Space"]==1]
    else:
        return df_fin

    

#add it here
def _find_db_out_dir(start: Optional[Path] = None) -> Path:
    """Locate the nearest 'DB_Out' directory walking up from this file.

    Searches the current file's directory, its parents, and siblings for a
    folder named 'DB_Out'. Returns the Path if found; raises FileNotFoundError
    otherwise.
    """
    start_dir = (start or Path(__file__).resolve()).parent

    # Walk up a few levels to be robust to different project layouts
    for base in [start_dir, *start_dir.parents]:
        candidate = base / "DB_Out"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "DB_Out directory not found. Expected a folder named 'DB_Out' "
        "in this project."
    )


DBTableName = Literal["investors", "rounds", "valuation", "export", "updown"]


def openDB(parameter: DBTableName) -> pd.DataFrame:
    """Open a parquet table from 'DB_Out' using a constrained set of names.

    Allowed values for `parameter` (offered by IDE autocompletion):
    - "investors"
    - "rounds"
    - "valuation"
    - "export"
    - "updown"

    Returns a pandas.DataFrame for the file `DB_<parameter>.parquet`.
    """
    allowed: tuple[str, ...] = ("investors", "rounds", "valuation", "export", "updown")

    if not isinstance(parameter, str):
        raise TypeError("parameter must be a string literal")

    key = parameter.strip().lower()
    if key not in allowed:
        raise ValueError(
            "Invalid parameter. Allowed: " + ", ".join(allowed)
        )

    db_dir = _find_db_out_dir()
    parquet_name = f"DB_{key}.parquet"
    parquet_path = db_dir / parquet_name

    if not parquet_path.is_file():
        available = ", ".join(p.name for p in sorted(db_dir.glob("*.parquet")))
        raise FileNotFoundError(
            f"Expected '{parquet_name}' in DB_Out. Available: {available if available else 'none'}"
        )

    return pd.read_parquet(parquet_path)
