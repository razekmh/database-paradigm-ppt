DROP TABLE IF EXISTS emails

CREATE TABLE emails (
email_message_id VARCHAR (50) PRIMARY KEY,
email_date TIMESTAMP with time zone,
email_subject TEXT,
email_body TEXT
);

COPY emails FROM '/opt/data/emails.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS users

CREATE TABLE users (
user_id VARCHAR (50) PRIMARY KEY,
user_email VARCHAR (250),
first_name VARCHAR (250),
last_name VARCHAR (250)
);

COPY users FROM '/opt/data/unique_users.csv' DELIMITER ',' CSV HEADER;


