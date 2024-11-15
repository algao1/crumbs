CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Eva');
SELECT name, id FROM users;
SELECT id, name FROM users;
SELECT id, name FROM users WHERE id = 2;
SELECT id + 2, name FROM users WHERE name = 'Alice';
INSERT INTO users VALUES (1, 3, 'Alice');
INSERT INTO users VALUES (1);