import time

import streamlit as st

import numpy as np
import pandas as pd
import datetime
import os
import glob
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sb
import re
from PIL import Image as P_IMAGE
import io
import plost
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import pydeck as pdk
import json
import mysql.connector
from mysql.connector import Error

st.set_page_config(layout="wide")

###################################################################################
#    def:  C0_Prelim_Main 
###################################################################################
def C0_Prelim_Main():
    #
    ###################################################################################
    #    Prelim Last Date Tablee @ C Sidebar Container
    ###################################################################################

    C=st.sidebar.container()

    LOG_EARLIEST_DATE_READYPATH = ("R:/RP2_common/Bravo/Data/03_Prelim_Data_Analysis/Prelim_Report*").replace("/","@")
    LOG_EARLIEST_DATE_READYPATH2 = glob.glob(os.path.join(LOG_EARLIEST_DATE_READYPATH).replace("/","").replace("@","/"))

    list_raw_date=[]

    for i in range(len(LOG_EARLIEST_DATE_READYPATH2)):                               
        TEMP_RAW_DATE=LOG_EARLIEST_DATE_READYPATH2[i].replace("R:/RP2_common/Bravo/Data/03_Prelim_Data_Analysis\Prelim_Report_","")[:-14]
        #if TEMP_RAW_DATE[4]=="-" and TEMP_RAW_DATE[7]=="-" :
        list_raw_date.append(TEMP_RAW_DATE)
    RAW_EARLIEST_DATES=sorted(dict.fromkeys(list_raw_date), reverse=True)

    DATE_PICK=C.selectbox("Please choose a Prelim available date.", RAW_EARLIEST_DATES)

    ###################################################################################
    #    C2 Sidebar Container
    ###################################################################################
    ###################################################################################
    #    Main Frame:
    ###################################################################################
    left_col, right_col = st.columns([1,2])  
    CM1=right_col.container()
    CM1_A1 = ("R:/RP2_common/Bravo/Data/03_Prelim_Data_Analysis/Prelim_Report_" + str(DATE_PICK) + "_T*").replace("/","@")
    CM1_A2 = glob.glob(os.path.join(CM1_A1).replace("/","").replace("@","/"))
    CM1_f = open(CM1_A2[0], "r")
    CM1.write(CM1_f.read())
    
    CM2=left_col.container()
    CM2.write("Buses and Last Data File Date & Time")
    PRELIM_LAST_DATE_READYPATH = ("R:/RP2_common/Bravo/Data/03_Prelim_Data_Analysis/Last_file_Datetime_" + str(DATE_PICK) +"_T*").replace("/","@")
    PRELIM_LAST_DATE_READYPATH2 = glob.glob(os.path.join(PRELIM_LAST_DATE_READYPATH).replace("/","").replace("@","/"))
    CM2_D=pd.read_csv(PRELIM_LAST_DATE_READYPATH2[0])
    CM2.dataframe(CM2_D.style.set_properties(color="orange", align="left"))
    ###################################################################################
    #    End   def:  C0_Prelim_Main End
    ###################################################################################
###################################################################################
#    def:  C1_Data_Analysis
###################################################################################    
def C1_Data_Analysis():
    SBC1 = st.sidebar.container()
    bus_list=['7017', '7018', '7020', '7024', '7026', '7043', '7045', '7047', '7055', '7057']
    #bus_list = [C1_bus for C1_bus in Bus_number if C1_bus[0]=="7" and len(C1_bus)==4]
    
    ALL_SELECT=SBC1.checkbox("ALL Buses")
    if ALL_SELECT:
        BUS_CHOSEN = bus_list
        BUS_CHOSEN_STR = ",".join([bus for bus in bus_list])
    else:
        BUS_SELECT = SBC1.multiselect("Please Choose Bus(es):", bus_list)
        BUS_CHOSEN = BUS_SELECT
        BUS_CHOSEN_STR = ",".join([bus for bus in BUS_CHOSEN])
    
    MODE_SELECT = SBC1.multiselect("Please Choose J1939 Parameter:",("Engine Coolant Temperature", "Engine Coolant Level"))
    
    C1_DATE_PICK_FROM = SBC1.date_input("Please Choose a Start Date")
    C1_DATE_PICK_TO = SBC1.date_input("Please Choose an End Date", min_value=C1_DATE_PICK_FROM)
    SBC1.write("Analysis Date From " + str(C1_DATE_PICK_FROM) + " To " + str(C1_DATE_PICK_TO))

    DAMS1 = SBC1.selectbox("Please select a Visualization Mode", ("Stacked", "Overlay"))

    DAMV1 = SBC1.checkbox("Set Minimum Coolant Temperature Value")
    if DAMV1==True:
        DAMV2 = SBC1.number_input("Please enter a Min. Value")
        DAMV2_STR = "'" + str(DAMV2) + "'"

    DATE_FROM = "'" + datetime.datetime.strptime(str(C1_DATE_PICK_FROM), "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S") + "'"
    
    DATE_TO_READY = datetime.datetime.strptime(str(C1_DATE_PICK_TO), "%Y-%m-%d") + datetime.timedelta(days=1)
    DATE_TO = "'" + DATE_TO_READY.strftime("%Y-%m-%d %H:%M:%S") + "'"
    
    if True:#SBC1.button("Confirm Run"):
        #
        #
        try:
            #
            mydb = mysql.connector.connect(
              host="10.0.20.143",
              user="rp2bravo",
              passwd="CAiRS1212",
              database="bravo_db"
            )

            if mydb.is_connected():
                mycursor = mydb.cursor()
                if DAMV1:
                    statement = "SELECT date_time, fleet_no, engine_coolant_temperature, engine_coolant_level, trip_id FROM record_tb " + \
                                "WHERE fleet_no IN (" + BUS_CHOSEN_STR + ")" + " AND date_time >= " + DATE_FROM +" AND date_time < " + DATE_TO + " AND engine_coolant_temperature >=" + DAMV2_STR +\
                                "ORDER BY fleet_no, date_time"                    
                else:
                    statement = "SELECT date_time, fleet_no, engine_coolant_temperature, engine_coolant_level, trip_id FROM record_tb " + \
                                "WHERE fleet_no IN (" + BUS_CHOSEN_STR + ")" + " AND date_time >= " + DATE_FROM +" AND date_time < " + DATE_TO +\
                                "ORDER BY fleet_no, date_time"

                mycursor.execute(statement)
                myresult = mycursor.fetchall()
                A1=[]
                A2=[]
                A3=[]
                A4=[]
                A5=[]
                i=1
                for row in myresult:
                    #
                    if i==1:
                        #
                        A1.append(row[0])
                        A2.append(str(row[1]))
                        A3.append(float(row[2]))
                        A4.append(float(row[3]))
                        A5.append(str(row[4]))
                        bus_number_last = str(row[1])
                        
                    if i % 20==0 and bus_number_last==str(row[1]) :
                        #
                        A1.append(row[0])
                        A2.append(str(row[1]))
                        A3.append(float(row[2]))
                        A4.append(float(row[3]))
                        A5.append(str(row[4]))
                        bus_number_last = str(row[1])
                        
                    if bus_number_last!=str(row[1]):
                        i=1
                        A1.append(row[0])
                        A2.append(str(row[1]))
                        A3.append(float(row[2]))
                        A4.append(float(row[3]))
                        A5.append(str(row[4]))
                        bus_number_last = str(row[1])
                    
                    i=i+1
                    
            A = pd.DataFrame({
                "Timestamp": A1,
                "Bus Fleet No": A2,
                "Engine Coolant Temperature": A3,
                "Engine Coolant Level": A4,
                "Trip ID": A5
            })
            A["Bus Fleet No"] = A["Bus Fleet No"].astype("string")
            A["Timestamp"]=pd.to_datetime(A["Timestamp"], format='%Y-%m-%d %H:%M:%S')
            #st.write(A['Engine Coolant Temperature'].dtypes)
        except Error as e:
            print (e)
        finally:
            if mydb.is_connected():
                mycursor.close()
                mydb.close()
        if BUS_CHOSEN==[]:
            st.write("Nil bus was chosen, Choose the bus(es) to begin")
        ##################################################################################################################
        #    STACKED
        ##################################################################################################################
        elif DAMS1 == "Stacked":
            #
            for bus in BUS_CHOSEN:
                #
                A1 = A[A["Bus Fleet No"]==bus]

                if "Engine Coolant Temperature" in MODE_SELECT:
                    #
                    st.title(bus)
                    st.write("Engine Coolant Temperature")

                    Z = pd.DataFrame({
                      "date" : A1["Timestamp"],
                      bus : A1["Engine Coolant Temperature"]
                    })

                    Z=Z.set_index("date")

                    st.line_chart(Z)
                #
                if "Engine Coolant Level" in MODE_SELECT:
                    #
                    st.write("Engine Coolant Level")
                    BVM=[]
                    ALL2=[]

                    Z = pd.DataFrame({
                      "date" : A1["Timestamp"],
                      bus : A1["Engine Coolant Level"]
                    })

                    Z=Z.set_index("date")
                    st.line_chart(Z)                
        ##################################################################################################################
        #    Overlay
        ##################################################################################################################
        elif DAMS1 == "Overlay":
            #
            TRIP_LIST = sorted(dict.fromkeys(A5), reverse=True)
            TRIP_SELECT=st.checkbox("ALL Trips")
            if TRIP_SELECT:
                TRIP_CHOSEN = TRIP_LIST
                TRIP_CHOSEN_STR = ",".join([trip for trip in TRIP_LIST])
            else:
                TRIP_SELECT = st.multiselect("Please Choose Trip ID:", TRIP_LIST)
                TRIP_CHOSEN = TRIP_SELECT
                TRIP_CHOSEN_STR = ",".join([trip for trip in TRIP_CHOSEN])
            
            if SBC1.button("Confirm Run"):
                A=A[A["Trip ID"].isin(TRIP_CHOSEN)]
                #
                if "Engine Coolant Temperature" in MODE_SELECT:
                    #
                    st.vega_lite_chart(A, {
                        #
                        "title":{
                            "text":"Engine Coolant Temperature vs Date-Time",
                            "fontSize":24,
                            "offset":15,
                        },
                        "width": 1400,
                        "height": 800,
                        "autosize": {"type": "fit", "contains": "padding"},
                        "layer":[
                            {
                                #
                                "selection": {"foo": {"type": "interval", "bind": "scales"}},
                                "mark":{"type":"line", "tooltip": {"content": "data"}},
                                'encoding': {
                                    "x": {
                                        "field": "Timestamp",
                                        "timeUnit":"year-month-date hours:minutes:seconds",
                                        "axis":{
                                            "labelAngle":45,
                                            "labelColor":"rgb(255, 94, 19)",
                                            "labelOverlap": True,
                                            "labelPadding": 0,
                                            "tickColor": "green",
                                            "format":"%Y-%m-%d, %H:%M:%S",
                                            "title":["Date & Time","(YYYY-MM-DD, h:m:s)"],
                                            "titleFontSize": 24,         
                                        }},
                                    "y":{
                                        "field": "Engine Coolant Temperature",
                                        "type":"quantitative",
                                        "axis":{
                                            "titleAngle":0,
                                            "title":["Engine","Coolant", "Temperature","(°C)"],
                                            "titleFontSize": 24,
                                        },
                                    },
                                    'color': {
                                        'field':'Bus Fleet No',
                                        "type":"nominal"
                                    },
                                }
                            },
                            {
                                "mark":{"type":"rule", "strokeDash":[4,2], "color":"red"},
                                'encoding': {
                                    'y': {"datum": 100}
                                }                            
                            },
                            {
                                "mark":{
                                    "type":"text", 
                                    "align":"center",
                                    "baseline": "bottom",
                                    "x": "width",
                                    "dx":-150,
                                    "y": "height",
                                    "text":"Normal Maximum Engine Coolant Temperature",
                                    "fontWeight":100, "fontSize":14, "font":"arial", 
                                },
                                "encoding":{
                                    'y': {"datum": 100}
                                }
                            },
                            {
                                "mark":{"type":"rule", "strokeDash":[6,4], "color":"blue"},
                                'encoding': {
                                    'y': {"field": "Engine Coolant Temperature", "aggregate": "max"}
                                }                            
                            },
                            {
                                "mark":{
                                    "type":"text", 
                                    "align":"center",
                                    "baseline": "bottom",
                                    "x": "width",
                                    "dx":-50,
                                    "y": "height",
                                    #"text":"Highest Temperature Recorded",
                                    "fontWeight":100, "fontSize":14, "font":"arial", 
                                },
                                "encoding":{
                                    'y': {"field": "Engine Coolant Temperature", "aggregate": "max"},
                                    "text":{"field": "Bus Fleet No", "aggregate": {"argmax": "Engine Coolant Temperature"}}

                                }  
                            },                        
                            {
                                "mark":{
                                    "type":"text", 
                                    "align":"center",
                                    "baseline": "bottom",
                                    "x": "width",
                                    "dx":-120,
                                    "y": "height",
                                    #"text":"Highest Temperature Recorded",
                                    "fontWeight":100, "fontSize":14, "font":"arial", 
                                },
                                "encoding":{
                                    'y': {"field": "Engine Coolant Temperature", "aggregate": "max"},
                                    "text":{"field": "Engine Coolant Temperature", "aggregate": "max"},

                                }
                            },
                            {
                                "mark":{
                                    "type":"text", 
                                    "align":"center",
                                    "baseline": "bottom",
                                    "x": "width",
                                    "dx":-250,
                                    "y": "height",
                                    "text":"Highest Temperature Recorded",
                                    "fontWeight":100, "fontSize":14, "font":"arial", 
                                },                            
                                "encoding":{
                                    'y': {"field": "Engine Coolant Temperature", "aggregate": "max"}
                                }
                            },                        
                        ],
                    }
                                      )
                #
                if "Engine Coolant Level" in MODE_SELECT:

                    plost.line_chart(
                        #
                        A,
                        title={
                            "text":"Engine Coolant Level vs Date-Time",
                            "fontSize":24,
                            "offset":15,
                        },
                        x={
                            "field": "Timestamp",
                            "timeUnit":"year-month-date hours:minutes:seconds",
                            "axis":{
                                "labelAngle":45,
                                "labelColor":"rgb(255, 94, 19)",
                                "labelOverlap": True,
                                "labelPadding": 0,
                                "tickColor": "green",
                                "format":"%Y-%m-%d, %H:%M:%S",
                                "title":["Date & Time","(YYYY-MM-DD, h:m:s)"],
                                "titleFontSize": 24,         
                            },
                        },
                        y={
                            "field": "Engine Coolant Level",
                            "type":"quantitative",
                            "axis":{
                                "titleAngle":0,
                                "title":["Engine","Coolant", "Level","(%)"],
                                "titleFontSize": 24,
                            },
                        },  # The name of the column to use for the data itself.

                        color={
                            "field":"Bus Fleet No",
                        }, # The name of the column to use for the line colors.
                        height=800,
                        y_annot={
                            #
                            100 : "Normal Engine Coolant Level",
                        }, 
                    )
                #
                st.balloons()
                ###################################################################################
                #    END   def:  C1_Data_Analysis
                ###################################################################################
###################################################################################
#    def:  C2_GPS (Non-Overlay)
###################################################################################    
def C2_GPS():
    C2A1_LISTDIR = os.listdir("R:/RP2_common/Bravo/Data/04_GPS_Testing/")
    SBC2 = st.sidebar.container()
    SBC2.write("Map Utilities:")
    C2_MAP_HEIGHT = SBC2.slider("Map Height", min_value=500, max_value=2000, step=10)
    C2_MAP_DOT_SIZE = SBC2.slider("Map Plot Dot Size", min_value=2, max_value=20, value=10, step=1 )
    #print(C1A1_LISTDIR)\
    MAP_BUS_CHOOSE_LIST = SBC2.multiselect("Please select the bus(es)", C2A1_LISTDIR)
    
    for mapbus in MAP_BUS_CHOOSE_LIST:
        
        C2M = st.container()
        C2M_L, C2M_R = C2M.columns([1,3])
        C2A1_READYPATH = ("R:/RP2_common/Bravo/Data/04_GPS_Testing/" + mapbus +"/*.csv").replace(str("/"), "@")
        BVM=[]
        C2A1_READYPATH2 = glob.glob(os.path.join(C2A1_READYPATH).replace("/","").replace("@","/"))
        if len(C2A1_READYPATH2) == 0:
            1
        else:
            #
            for wildcard in glob.glob(os.path.join(C2A1_READYPATH).replace("/","").replace("@","/")):
                BV=[]
                BV=pd.read_csv(wildcard)
                BVM.append(BV)
            combine=pd.concat(BVM, axis=0, ignore_index=True)
            
            combine["Date/Time"]=pd.to_datetime(combine["Date/Time"], format='%Y-%m-%d %H:%M:%S')
            combine=combine.sort_values(by='Date/Time', ascending=True).reset_index(drop=True)
            C2A1_combine=combine.rename(columns={"Latitude Value": "lat", "Longitude Value": "lon"})
            
            C2M_L.write(mapbus)
            C2M_L.write("Select Available Date Time Range")
            C2A1_MIN_DATETIME = C2M_L.select_slider("Select a Start Date-time:", options=C2A1_combine["Date/Time"])
            if C2A1_MIN_DATETIME == max(C2A1_combine["Date/Time"]):
                C2M_L.write("Start Date-Time as same as End, ease the slide.")
            else:
                C2A1_MAX_DATETIME = C2M_L.select_slider("Select a End Date-time:", options=C2A1_combine[(C2A1_combine["Date/Time"]>=C2A1_MIN_DATETIME)]["Date/Time"])
            
            C2A1_combine2 = C2A1_combine[(C2A1_combine["Date/Time"]>=C2A1_MIN_DATETIME) & (C2A1_combine["Date/Time"]<=C2A1_MAX_DATETIME)]
                          
            
            C2M_R.pydeck_chart(pdk.Deck(
                map_style="mapbox://styles/mapbox/light-v9",
                initial_view_state=pdk.ViewState(
                    latitude=(max(C2A1_combine2['lat'])+min(C2A1_combine2['lat']))/2,
                    longitude=(max(C2A1_combine2['lon'])+min(C2A1_combine2['lon']))/2,
                    zoom=11,
                    pitch=50,
                    height=C2_MAP_HEIGHT
                ),
                layers=[pdk.Layer(
                    'ScatterplotLayer',
                    data=C2A1_combine2.dropna(),
                    get_position='[lon, lat]',
                    get_color='[200, 30, 0, 160]',
                    get_radius=C2_MAP_DOT_SIZE,
                ),],
                #api_keys=("pk.eyJ1Ijoia2luZ2h1aTQ1IiwiYSI6ImNrdmx2OG4zdjlhNXAybnM3NWx0M3ozYmIifQ.zyVcc5fyM_ZR5xIvALCIVw",),
                
            ))
            ###################################################################################
            #    END   def:  C1_Data_Analysis
            ###################################################################################
###################################################################################
#    def:  C2_GPS_OL (Overlay)
###################################################################################
def C2_GPS_OL():
    C2A1_LISTDIR_READY = os.listdir("R:/RP2_common/Bravo/Data/01_Raw_Data/")
    C2A1_LISTDIR = [valid_bus for valid_bus in C2A1_LISTDIR_READY if (valid_bus[0]=="7" or valid_bus[0]=="1") and len(valid_bus)==4 ]
    
    SBC2 = st.sidebar.container()
    SBC2.write("Map Utilities:")
    C2_MAP_HEIGHT = SBC2.slider("Map Height", min_value=500, max_value=2000, step=10, value=750)
    C2_MAP_DOT_SIZE = SBC2.slider("Map Plot Dot Size", min_value=1, max_value=45, value=20, step=1 )
    MAP_BUS_ALL_SELECT = SBC2.checkbox("Map ALL Buses")
    if MAP_BUS_ALL_SELECT:
        MAP_BUS_CHOOSE_LIST = C2A1_LISTDIR
    else:
        MAP_BUS_CHOOSE_LIST = SBC2.multiselect("Please select the bus(es)", C2A1_LISTDIR)
    
    C2M = st.container()
    C2M_L, C2M_R = C2M.columns([1,3])    
    MAP_LAYER = []
    C2A1_combine3 = []
    for mapbus in MAP_BUS_CHOOSE_LIST:
        
        C2A1_READYPATH = ("R:/RP2_common/Bravo/Data/01_Raw_Data/" + mapbus +"/*/*RawGPSdata.csv").replace(str("/"), "@")
        BVM=[]
        C2A1_READYPATH2 = glob.glob(os.path.join(C2A1_READYPATH).replace("/","").replace("@","/"))
        if len(C2A1_READYPATH2) == 0:
            SBC2_1L, SBC2_1R = SBC2.columns([1,5])
            SBC2_1L.image("Attention.png", output_format="png", use_column_width=True)
            SBC2_1R.write(mapbus + " has no GPS Data")

        else:
            #
            for wildcard in glob.glob(os.path.join(C2A1_READYPATH).replace("/","").replace("@","/")):
                #print(wildcard)
                if os.stat(wildcard).st_size != 0:
                    #
                    BV=[]
                    BV=pd.read_csv(wildcard)
                    #print(BV)
                    BVM.append(BV)
            combine=pd.concat(BVM, axis=0, ignore_index=True)
            combine["timestamp"]=pd.to_datetime(combine["timestamp"], format='%Y-%m-%d %H:%M:%S')
            combine=combine.sort_values(by='timestamp', ascending=True).reset_index(drop=True)
            C2A1_combine=combine.rename(columns={"latitude": "lat", "longitude": "lon"})

            C2M_L.title(mapbus)
            C2M_L.write("Select Available Date Time Range")
            C2A1_MIN_DATETIME = C2M_L.select_slider("Select a Start Date-time:", options=C2A1_combine["timestamp"])
            if C2A1_MIN_DATETIME == max(C2A1_combine["timestamp"]):
                C2M_L.write("Start Date-Time as same as End, ease the slide.")
            else:
                C2A1_MAX_DATETIME = C2M_L.select_slider("Select a End Date-time:", 
                                                        options=C2A1_combine[(C2A1_combine["timestamp"]>=C2A1_MIN_DATETIME)]["timestamp"],
                                                        value=max(C2A1_combine["timestamp"])
                                                       )
            
            C2A1_combine3 = C2A1_combine[(C2A1_combine["timestamp"]>=C2A1_MIN_DATETIME) & (C2A1_combine["timestamp"]<=C2A1_MAX_DATETIME)].dropna(axis=1)

            BUS_COLOR=[]
            BUS_COLOR.extend([100 + int(mapbus[2:])*5, 30+ int(mapbus[2:])*5, 0, 160])
            #BUS_COLOR = [255, 255, 0, 150]
            BUS_COLOR1 = str(BUS_COLOR)
            MAP_LAYER.append(pdk.Layer('ScatterplotLayer',data=C2A1_combine3, get_position='[lon, lat]', get_color=BUS_COLOR1, get_radius=C2_MAP_DOT_SIZE, pickable=True,))
 
    tooltip = {
        "html":
        "<b>Bus Fleet No.:</b> {Fleet No} <br/>"
        "<b>Route No.:</b> {Route No} <br/>"
        "<b>Lon - Lat:</b> {lon}, {lat} <br/>",
        "style": {
            "backgroundColor": "steelblue",
            "color": "black",
    }}

    C2M_R.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        #map_style=pdk.map_styles.ROAD,
        initial_view_state=pdk.ViewState(
            latitude=22.24,
            longitude=114.17,
            zoom=11,
            pitch=50,
            height=C2_MAP_HEIGHT),
        layers=MAP_LAYER,
        tooltip=tooltip,
    ))
###################################################################################
#    END   def:  C2_GPS_OL (Overlay)
###################################################################################

###################################################################################
#    Main Function Select
###################################################################################

C0=st.sidebar.container()
C0.image("cairs_logo.png", output_format='auto', use_column_width=True)
C0_Home=C0.selectbox("Please choose function:", ["Prelim Analysis", "Data Analysis", "GPS", "GPS (Overlay)"])
if C0_Home == "Prelim Analysis":
    C0_Prelim_Main()    
if C0_Home == "Data Analysis":
    C1_Data_Analysis()
if C0_Home == "GPS":
    C2_GPS()
if C0_Home == "GPS (Overlay)":
    C2_GPS_OL()

