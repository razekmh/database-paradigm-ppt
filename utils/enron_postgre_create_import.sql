DROP TABLE IF EXISTS emails;

CREATE TABLE emails (
email_message_id VARCHAR (50) PRIMARY KEY,
email_date TIMESTAMP with time zone,
email_subject TEXT,
email_body TEXT
);

COPY emails FROM '/opt/data/emails.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS users;

CREATE TABLE users (
user_id VARCHAR (50) PRIMARY KEY,
user_email VARCHAR (250),
first_name VARCHAR (250),
last_name VARCHAR (250)
);

INSERT INTO users (user_id, user_email, first_name, last_name)
VALUES('0','Unknown',NULL, NULL);

COPY users FROM '/opt/data/unique_users.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS email_users;

CREATE TABLE email_users (
transaction_id VARCHAR (50) PRIMARY KEY,
email_message_id VARCHAR (50),
sender VARCHAR (250),
receiver VARCHAR (250),
transaction_type VARCHAR (250),
FOREIGN KEY (email_message_id) REFERENCES emails(email_message_id),
FOREIGN KEY (sender) REFERENCES users(user_id),
FOREIGN KEY (receiver) REFERENCES users(user_id)
);

COPY email_users FROM '/opt/data/unique_email_users.csv' DELIMITER ',' CSV HEADER;