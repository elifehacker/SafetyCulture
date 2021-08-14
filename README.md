# SafetyCulture

This task was done by reading the content of message_bus.json line by line to simulate the incoming stream of data. With every line, the following command prompt is returned.

Option 1 2 and 3 print the data as is.

Option 4 find the database data from database.csv stored in a panda dataframe based on the user id

OPtion 5 find 3rd party data by getting the email column in what is returned from option 4, then finding it in the list of json data from 3rd_party_data.json

Should there be an error, the error would be caught and saved to a txt file and it does not stop the processing of the rest of the data stream.

```
(base) F:\workspace>cd "Data Engineering Test"

(base) F:\workspace\Data Engineering Test>python monitor.py
update data with user_id ffbf731b-897f-465e-b4c7-2f5efa54989d
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

1
input was 1
ffbf731b-897f-465e-b4c7-2f5efa54989d
{'user_id': 'ffbf731b-897f-465e-b4c7-2f5efa54989d', 'checklist_id': 'c9d557ee-a580-46af-91e6-60fb8ec9bc9a', 'checklist_name': 'Checklist No 243', 'items': [{'item_id': 'e2953563-3c29-451a-8611-b775e4bb3190', 'item_title': 'item number 0', 'item_value': '0'}], 'media': {'1206dbe1-7f8f-4c77-8e54-189f454c796a': {'title': 'image_1', 'descritption': 'Foo Bar, Foo Bar'}, '9bf0fd17-6d9b-44ab-9eb0-bb6981428256': {'title': 'image_2', 'descritption': 'Foo Bar, Foo Bar'}}, 'timestamp': datetime.datetime(2021, 8, 14, 13, 29, 52, 276851)}
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

2
input was 2
                                 user_id                   email              name  ...        city postcode company_name
0   ddb10af5-9fed-4d43-a624-fd585c8e3673   jginspace@verizon.net        Danny Bird  ...      Sydney     2000    Company_1
1   584f6006-dc4b-4dd3-bf8b-eafbd98859aa    phizntrg@hotmail.com      Quincy Walls  ...   Melbourne     3000    Company_2
2   195e55b4-b843-4f7e-b9ef-c038f8465040        natepuri@aol.com      Hana Jenkins  ...  Townsville     4810    Company_3
3   d4078c4e-059a-4e68-bad7-e0ba9714d935       north@comcast.net     Sylvia Ibarra  ...      Sydney     2000    Company_4
4   9cc69da1-1043-452b-b032-fe04915fa22f        kjohnson@msn.com    Marvin Clayton  ...   Melbourne     3000    Company_5
5   d2e8787f-ff33-4d71-a9a1-91fcd8ca7096         balchen@aol.com     Jacob Carlson  ...  Townsville     4810    Company_6
6   5d36c61a-6302-454c-8f83-d7908a4fd19f         matty@yahoo.com  Armando Woodward  ...      Sydney     2000    Company_7
7   0ad2ee24-0394-492d-84de-ee036f797d70        alastair@mac.com   Malachi Rosario  ...   Melbourne     3000    Company_8
8   84f49eb7-28cf-41a8-a58d-364f32303fad       matthijs@live.com     Sariah Archer  ...  Townsville     4810    Company_9
9   6ac30b67-face-468c-b2d1-71632442405b          hakim@yahoo.ca    Bronson Lucero  ...      Sydney     2000   Company_10
10  a464e226-4cb9-485b-af27-3f74951870d2  specprog@optonline.net     Alani Pacheco  ...   Melbourne     3000   Company_11
11  22bb16aa-b898-4873-93ba-1a9392ab20ca          zyghom@att.net    Johnathon Vang  ...  Townsville     4810   Company_12
12  c71cbf53-8fe9-48bb-94bd-471b6cb29d66     nanop@optonline.net   Isabela Carroll  ...      Sydney     2000   Company_13
13  38992db5-5b0b-4ba4-9add-ac4c1fadae19         pspoole@att.net    Jamarion Mills  ...   Melbourne     3000   Company_14
14  43198eb0-6fe0-40ad-a852-c0f23e5af3f9        emmanuel@att.net    Zackary Hansen  ...  Townsville     4810   Company_15
15  ffbf731b-897f-465e-b4c7-2f5efa54989d        gumpish@yahoo.ca    Ayana Davidson  ...      Sydney     2000   Company_16
16  0e50c52d-9edf-4b68-ab3c-bd635875bf95       seasweb@yahoo.com    Aliza Villegas  ...   Melbourne     3000   Company_17
17  053571bf-23de-40b4-8be8-e9d4f89f5bed      jbuchana@gmail.com    Quentin Jarvis  ...  Townsville     4810   Company_18
18  3c3d679e-0257-45ed-bf47-2f3594c7d0fa    parkes@optonline.net        Marc Bryan  ...   Melbourne     3000   Company_19
19  bb3f5878-460f-4fb8-9b5f-a5df1e4ec950        pereinar@att.net   Valentina Lewis  ...  Townsville     4810   Company_20

[20 rows x 8 columns]
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

3
input was 3
{'user_email': 'jginspace@verizon.net', 'industry': 'industry_1', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'phizntrg@hotmail.com', 'industry': 'industry_2', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'natepuri@aol.com', 'industry': 'industry_3', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'north@comcast.net', 'industry': 'industry_4', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'kjohnson@msn.com', 'industry': 'industry_5', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'balchen@aol.com', 'industry': 'industry_6', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'matty@yahoo.com', 'industry': 'industry_7', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'alastair@mac.com', 'industry': 'industry_8', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'matthijs@live.com', 'industry': 'industry_9', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'hakim@yahoo.ca', 'industry': 'industry_10', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'specprog@optonline.net', 'industry': 'industry_11', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'zyghom@att.net', 'industry': 'industry_1', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'nanop@optonline.net', 'industry': 'industry_2', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'pspoole@att.net', 'industry': 'industry_3', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'emmanuel@att.net', 'industry': 'industry_4', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'gumpish@yahoo.ca', 'industry': 'industry_5', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'seasweb@yahoo.com', 'industry': 'industry_6', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'jbuchana@gmail.com', 'industry': 'industry_7', 'region': 'region_3', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'parkes@optonline.net', 'industry': 'industry_8', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
{'user_email': 'pereinar@att.net', 'industry': 'industry_9', 'region': 'region_2', 'compay_profile': 'profile', 'social_presence': 'text_text'}
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

4
input was 4
                                 user_id             email            name  ...    city postcode company_name
15  ffbf731b-897f-465e-b4c7-2f5efa54989d  gumpish@yahoo.ca  Ayana Davidson  ...  Sydney     2000   Company_16

[1 rows x 8 columns]
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

5
input was 5
{'user_email': 'gumpish@yahoo.ca', 'industry': 'industry_5', 'region': 'region_1', 'compay_profile': 'profile', 'social_presence': 'text_text'}
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message


input was
update data with user_id 584f6006-dc4b-4dd3-bf8b-eafbd98859aa
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

1
input was 1
ffbf731b-897f-465e-b4c7-2f5efa54989d
{'user_id': 'ffbf731b-897f-465e-b4c7-2f5efa54989d', 'checklist_id': 'c9d557ee-a580-46af-91e6-60fb8ec9bc9a', 'checklist_name': 'Checklist No 243', 'items': [{'item_id': 'e2953563-3c29-451a-8611-b775e4bb3190', 'item_title': 'item number 0', 'item_value': '0'}], 'media': {'1206dbe1-7f8f-4c77-8e54-189f454c796a': {'title': 'image_1', 'descritption': 'Foo Bar, Foo Bar'}, '9bf0fd17-6d9b-44ab-9eb0-bb6981428256': {'title': 'image_2', 'descritption': 'Foo Bar, Foo Bar'}}, 'timestamp': datetime.datetime(2021, 8, 14, 13, 29, 52, 276851)}
584f6006-dc4b-4dd3-bf8b-eafbd98859aa
{'user_id': '584f6006-dc4b-4dd3-bf8b-eafbd98859aa', 'checklist_id': 'c9944b8c-3342-4443-99ce-385795384f00', 'checklist_name': 'Checklist No 390', 'items': [{'item_id': '8b76d968-848d-4067-9d00-a6d13031d6b9', 'item_title': 'item number 1', 'item_value': '1'}], 'timestamp': datetime.datetime(2021, 8, 14, 13, 30, 4, 814303)}
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message


input was
update data with user_id 584f6006-dc4b-4dd3-bf8b-eafbd98859aa
save old message to a database
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message

1
input was 1
ffbf731b-897f-465e-b4c7-2f5efa54989d
{'user_id': 'ffbf731b-897f-465e-b4c7-2f5efa54989d', 'checklist_id': 'c9d557ee-a580-46af-91e6-60fb8ec9bc9a', 'checklist_name': 'Checklist No 243', 'items': [{'item_id': 'e2953563-3c29-451a-8611-b775e4bb3190', 'item_title': 'item number 0', 'item_value': '0'}], 'media': {'1206dbe1-7f8f-4c77-8e54-189f454c796a': {'title': 'image_1', 'descritption': 'Foo Bar, Foo Bar'}, '9bf0fd17-6d9b-44ab-9eb0-bb6981428256': {'title': 'image_2', 'descritption': 'Foo Bar, Foo Bar'}}, 'timestamp': datetime.datetime(2021, 8, 14, 13, 29, 52, 276851)}
584f6006-dc4b-4dd3-bf8b-eafbd98859aa
{'user_id': '584f6006-dc4b-4dd3-bf8b-eafbd98859aa', 'checklist_id': '58979a18-b8c0-42bf-8af0-a74d5e6d07f1', 'checklist_name': 'Checklist No 595', 'items': [{'item_id': 'f212c872-0f67-452d-b1c9-b37a4b87e8be', 'item_title': 'item number 2', 'item_value': '2'}], 'timestamp': datetime.datetime(2021, 8, 14, 13, 30, 13, 345347)}
enter: proceed
            1: check latest message of each user
            2: print database
            3: print 3rd party data
            4: print database record of the latest message
            5: print 3rd party data of the latest message
```
