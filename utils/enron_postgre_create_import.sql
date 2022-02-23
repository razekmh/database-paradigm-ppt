DROP TABLE IF EXISTS email CASCADE;

CREATE TABLE email (
email_message_id VARCHAR (50) PRIMARY KEY,
email_date TIMESTAMP with time zone,
email_subject TEXT,
email_body TEXT
);

COPY email FROM '/opt/data/emails.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS employee CASCADE;

CREATE TABLE employee (
user_id VARCHAR (50) PRIMARY KEY,
user_email VARCHAR (250),
first_name VARCHAR (250),
last_name VARCHAR (250),
rank VARCHAR (250),
role VARCHAR (250),
company VARCHAR (250)
);

INSERT INTO employee (user_id, user_email, first_name, last_name, rank, role, company)
VALUES('0','Unknown',NULL, NULL, NULL, NULL, NULL);

COPY employee FROM '/opt/data/unique_users_with_names.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS eamil_transaction CASCADE;

CREATE TABLE eamil_transaction (
transaction_id VARCHAR (50) PRIMARY KEY,
email_message_id VARCHAR (50),
sender VARCHAR (250),
receiver VARCHAR (250),
transaction_type VARCHAR (250),
external_or_internal VARCHAR (250),
FOREIGN KEY (email_message_id) REFERENCES email(email_message_id),
FOREIGN KEY (sender) REFERENCES employee(user_id),
FOREIGN KEY (receiver) REFERENCES employee(user_id)
);

COPY eamil_transaction FROM '/opt/data/unique_email_users.csv' DELIMITER ',' CSV HEADER;
