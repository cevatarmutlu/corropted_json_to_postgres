import re
import json
import psycopg2

corrupted_json_file = open("src/files/corrupted-file.json")
corrupted_json_str = corrupted_json_file.read()
corrupted_json_file.close()

remove_newline = re.sub('\n','', corrupted_json_str)

temp = open("temp_file.json", "w")
temp.write("[" + remove_newline + "]")
temp.close()


tranform_json = json.load(open("temp_file.json", "r"))

for i in tranform_json:
    # print(i["content-spec"])

    # print(i["device"]["deviceID"])

    # print(i["device"]["metaData"]["cloudGateway"]["subscriptionTopic"])
    # print(i["device"]["metaData"]["cloudGateway"]["awsTarget"])

    # print(i["device"]["metaData"]["cloudGateway"]["hostName"])

    # print(i["device"]["metaData"]["cloudGateway"]["splitMeasurements"])

    # print("\n\nsensor_names----------------------")
    # for j in i["measurements"]:
    #     sensor_names = list(j["series"].keys())[1]
    #     print(sensor_names)
    
    print("\n\nlen values----------------------")
    for j in i["measurements"]:
        dollar_times = j["series"]["$_time"]
        sensor_values = j["series"][list(j["series"].keys())[1]]
        if len(dollar_times) != len(sensor_values):
            print("not equal", len(dollar_times), len(sensor_values))


for i in tranform_json:
    for j in i["measurements"]:
        # print(list(j["series"].items())[0][1])
        sensor_name = list(j["series"].keys())[1]

        if ".ab" in sensor_name:
            new_sensor_name = sensor_name.replace(".ab", "")
            sensor_values = j["series"][sensor_name]
            del j["series"][sensor_name]
            j["series"][new_sensor_name] = sensor_values

arr = []

for i in tranform_json:
    for j in i["measurements"]:
        ts = j["ts"]
        dollar_times = j["series"]["$_time"]
        sensor_name = list(j["series"].keys())[1]
        sensor_values = j["series"][sensor_name]
        zipped_arr = zip(
            [ts for i in range(len(dollar_times))],
            [sensor_name for i in range(len(dollar_times))],
            dollar_times,
            sensor_values
        )
        arr += zipped_arr

conn = psycopg2.connect(
    user="ntt",
    password="ntt",
    host="127.0.0.1",
    port="5432",
    database="ntt"
)

table_name = 'table2'

cur = conn.cursor()

cur.execute(f"""
CREATE TABLE public.{table_name} (
	"timestamp" varchar(255) NULL,
    sensor_name varchar(255) NULL,
	dollar_time varchar(255) NULL,
	sensor_value varchar(255) NULL
);
""")

for i in arr:
    cur.execute(f"INSERT INTO {table_name} VALUES ('{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}')")


conn.commit()
cur.close()
conn.close()


