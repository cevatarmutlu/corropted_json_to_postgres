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

    # print("\n\nseries_names----------------------")
    # for j in i["measurements"]:
    #     series_name = list(j["series"].keys())[1]
    #     print(series_name)
    
    print("\n\nlen values----------------------")
    for j in i["measurements"]:
        dollar_time = j["series"]["$_time"]
        series_values = j["series"][list(j["series"].keys())[1]]
        if len(dollar_time) != len(series_values):
            print("not equal", len(dollar_time), len(series_values))


for i in tranform_json:
    for j in i["measurements"]:
        # print(list(j["series"].items())[0][1])
        series_name = list(j["series"].keys())[1]

        if ".ab" in series_name:
            new_series_name = series_name.replace(".ab", "")
            series_values = j["series"][series_name]
            del j["series"][series_name]
            j["series"][new_series_name] = series_values

arr = []

for i in tranform_json:
    for j in i["measurements"]:
        ts = j["ts"]
        dollar_times = j["series"]["$_time"]
        series_name = list(j["series"].keys())[1]
        series_values = j["series"][series_name]
        zipped_arr = zip(
            [ts for i in range(len(dollar_times))],
            [series_name for i in range(len(dollar_times))],
            dollar_times,
            series_values
        )
        arr += zipped_arr

conn = psycopg2.connect(
    user="ntt",
    password="ntt",
    host="127.0.0.1",
    port="5432",
    database="ntt"
)

cur = conn.cursor()

for i in arr:
    cur.execute(f"INSERT INTO ntt_case2 VALUES ('{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}')")


conn.commit()
cur.close()
conn.close()


