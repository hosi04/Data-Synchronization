CREATE TABLE IF NOT EXISTS users_log_before(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_log_after(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER //
CREATE TRIGGER before_insert_users
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "UPDATE");
END //

CREATE TRIGGER before_delete_users
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END //

CREATE TRIGGER after_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "UPDATE");
END //

CREATE TRIGGER after_insert_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END //

CREATE TRIGGER after_delete_users
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END //
DELIMITER ;

INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (1, 'alice', 'g001', 'https://avatars.com/alice', 'https://github.com/alice');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (2, 'bob', 'g002', 'https://avatars.com/bob', 'https://github.com/bob');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (3, 'carol', 'g003', 'https://avatars.com/carol', 'https://github.com/carol');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (4, 'dave', 'g004', 'https://avatars.com/dave', 'https://github.com/dave');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (5, 'eve', 'g005', 'https://avatars.com/eve', 'https://github.com/eve');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (6, 'frank', 'g006', 'https://avatars.com/frank', 'https://github.com/frank');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (7, 'grace', 'g007', 'https://avatars.com/grace', 'https://github.com/grace');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (8, 'heidi', 'g008', 'https://avatars.com/heidi', 'https://github.com/heidi');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (9, 'ivan', 'g009', 'https://avatars.com/ivan', 'https://github.com/ivan');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (10, 'judy', 'g010', 'https://avatars.com/judy', 'https://github.com/judy');
UPDATE users SET login = 'alice_updated' WHERE user_id = 1;
UPDATE users SET gravatar_id = 'g002x' WHERE user_id = 2;
UPDATE users SET avatar_url = 'https://cdn.avatars.com/carol' WHERE user_id = 3;
UPDATE users SET url = 'https://github.com/dave-new' WHERE user_id = 4;
UPDATE users SET login = 'eve_2025' WHERE login = 'eve';
UPDATE users SET gravatar_id = 'g006-new' WHERE login = 'frank';
UPDATE users SET avatar_url = 'https://cdn.avatars.com/grace' WHERE login = 'grace';
UPDATE users SET url = CONCAT(url, '?ref=update') WHERE user_id = 8;
UPDATE users SET login = 'ivan_the_great' WHERE user_id = 9;
UPDATE users SET gravatar_id = 'g010-updated' WHERE login = 'judy';
DELETE FROM users WHERE user_id = 1;
DELETE FROM users WHERE login = 'bob';
DELETE FROM users WHERE gravatar_id = 'g003';