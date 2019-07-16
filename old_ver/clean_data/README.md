# Welp, getting the data into s3 but with no formatting or cleaning was easy

Important thing I want to keep in mind is to create a py file that will run on one .txt file and insert/update into a...dynamodb. Once I get that working I can probably put combine that into my lambda function that puts data into s3 to automatically do that as well.

## Only gets the...ones created at the moment
Probably create a CSV of filenames that have been processed. Create another function to run on all old stuff and insert/update same as normal based on post_id and date_uploaded - created_utc after fixing some time zones.
