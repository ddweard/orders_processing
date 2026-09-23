CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer VARCHAR(255) NOT NULL,
    product  VARCHAR(255) NOT NULL,
    amount   DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- пользователь app уже создаётся образом mysql через MYSQL_USER/MYSQL_PASSWORD,
-- но на всякий случай дадим ему права на новую БД
GRANT ALL PRIVILEGES ON orders_db.* TO 'app'@'%';
FLUSH PRIVILEGES;